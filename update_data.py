import yfinance as yf
import pandas as pd
import sqlite3
import requests
from io import StringIO
from datetime import datetime
import time

def get_total_market_tickers():
    """모든 경로가 막혔을 때를 대비한 3중 우회 수집 로직"""
    all_tickers = set()
    # 봇 차단을 피하기 위한 실제 브라우저 위장 헤더
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8"
    }
    
    # 1순위: 가장 대중적이고 차단이 적은 GitHub 내 금융 데이터셋 (약 8,000개)
    print("Source 1: GitHub Raw 금융 데이터셋 접속 중...")
    try:
        url = "https://raw.githubusercontent.com/rreichel3/US-Stock-Symbols/main/all_tickers.txt"
        res = requests.get(url, headers=headers, timeout=15)
        if res.status_code == 200:
            tickers = [s.strip().upper() for s in res.text.split('\n') if s.strip()]
            all_tickers.update(tickers)
            print(f"Source 1 성공: {len(tickers)}개 확보")
    except: pass

    # 2순위: 1순위 실패 시 다른 금융 데이터 허브 공략
    if len(all_tickers) < 3000:
        print("Source 2: 대체 데이터 허브(Datasets) 접속 중...")
        try:
            url2 = "https://raw.githubusercontent.com/datasets/s-and-p-500-companies/master/data/constituents.csv"
            res2 = requests.get(url2, headers=headers, timeout=15)
            df2 = pd.read_csv(StringIO(res2.text))
            tickers2 = df2['Symbol'].tolist()
            all_tickers.update(tickers2)
            print(f"Source 2 성공: {len(tickers2)}개 확보")
        except: pass

    # 정제 로직: 티커가 아닌 이름들이 섞이지 않도록 엄격하게 필터링
    clean_list = []
    for s in all_tickers:
        s = str(s).strip().upper().replace('.', '-')
        # 1-5자 영문, 하이픈만 허용 (숫자나 긴 기업명 제거)
        if 1 <= len(s) <= 5 and s.replace('-', '').isalpha():
            clean_list.append(s)
    
    final_list = sorted(list(set(clean_list)))
    print(f"--- [최종 확인] 수집된 전체 티커: {len(final_list)}개 ---")
    return final_list

def update_database():
    start_time = datetime.now()
    tickers = get_total_market_tickers()
    
    if len(tickers) < 100:
        print("모든 소스 수집 실패. 비상용 리스트 가동.")
        tickers = ['AAPL', 'MSFT', 'NVDA', 'TSLA', 'GOOGL', 'AMZN', 'META', 'BRK-B', 'LLY', 'AVGO']

    all_results = []
    # 5,000개 이상 대량 처리를 위해 100개씩 끊어서 처리
    chunk_size = 100 
    
    for i in range(0, len(tickers), chunk_size):
        chunk = tickers[i:i + chunk_size]
        print(f"[{i}/{len(tickers)}] 전체 시장 종목 분석 중...")
        
        try:
            # 야후 차단을 방지하기 위해 threads=False 권장
            data = yf.download(chunk, period="1y", interval="1d", progress=False, threads=False)['Close']
            
            if isinstance(data, pd.Series): data = data.to_frame()

            for ticker in data.columns:
                series = data[ticker].dropna()
                if len(series) < 200: continue 
                
                # IBD Relative Strength 점수 가중치 계산
                now = series.iloc[-1]
                m3, m6, m9, m12 = series.iloc[-63], series.iloc[-126], series.iloc[-189], series.iloc[0]
                raw_score = (now/m3 * 2) + (now/m6) + (now/m9) + (now/m12)
                
                all_results.append({
                    'symbol': ticker, 'raw_score': raw_score, 'price': round(now, 2)
                })
        except: continue
        time.sleep(0.5) 

    if all_results:
        final_df = pd.DataFrame(all_results)
        # 전체 시장 모수 안에서 1-99점 부여
        final_df['rs_score'] = (final_df['raw_score'].rank(pct=True) * 99).astype(int)
        
        conn = sqlite3.connect('ibd_system.db')
        final_df[['symbol', 'rs_score', 'price']].to_sql('repo_results', conn, if_exists='replace', index=False)
        conn.close()
        print(f"[{datetime.now()}] 업데이트 완료! 총 {len(final_df)}개 종목 분석 완료.")

if __name__ == "__main__":
    update_database()
