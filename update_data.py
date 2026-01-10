import yfinance as yf
import pandas as pd
import sqlite3
import requests
from io import StringIO
from datetime import datetime
import time

def get_total_market_tickers():
    """다양한 백업 소스를 활용하여 미국 시장 전체 티커 확보"""
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"}
    all_tickers = set()

    # 소스 1: 가장 방대한 주식 데이터 리포지토리 (Cloudflare 등 우회)
    print("Source 1: 글로벌 금융 데이터 백업 소스 확인 중...")
    try:
        # 8,000개 이상의 종목이 포함된 비교적 안정적인 GitHub 데이터
        url = "https://raw.githubusercontent.com/yandm/stock_tickers/master/data/us_tickers.csv"
        res = requests.get(url, timeout=10)
        df = pd.read_csv(StringIO(res.text))
        # 심볼 컬럼 추출 (보통 'Symbol' 또는 'ticker')
        col = 'Symbol' if 'Symbol' in df.columns else df.columns[0]
        all_tickers.update(df[col].dropna().astype(str).str.upper().tolist())
    except:
        print("Source 1 접속 실패.")

    # 소스 2: 또 다른 안정적인 티커 리스트 (S&P 및 Russell 기반)
    if len(all_tickers) < 2000:
        print("Source 2: 대체 금융 데이터셋 시도 중...")
        try:
            url2 = "https://raw.githubusercontent.com/datasets/s-and-p-500-companies/master/data/constituents.csv"
            df2 = pd.read_csv(url2)
            all_tickers.update(df2['Symbol'].dropna().astype(str).str.upper().tolist())
        except:
            print("Source 2 접속 실패.")

    # 데이터 정제 및 필터링
    clean_list = []
    for s in all_tickers:
        s = s.strip().replace('.', '-')
        # 기업명 혼입 방지: 1~5자 사이, 영문 및 하이픈만 허용
        if 1 <= len(s) <= 5 and s.replace('-', '').isalpha():
            clean_list.append(s)
    
    # 중복 제거
    final_list = list(set(clean_list))
    print(f"--- 최종 확보된 전체 티커 수: {len(final_list)}개 ---")
    return final_list

def update_database():
    start_time = datetime.now()
    tickers = get_total_market_tickers()
    
    if not tickers:
        print("에러: 모든 소스에서 티커를 가져오지 못했습니다. 비상용 핵심 리스트를 가동합니다.")
        tickers = ['AAPL', 'MSFT', 'NVDA', 'TSLA', 'GOOGL', 'AMZN', 'META'] # 최소 동작 보장

    all_results = []
    # 5,000개급 처리를 위해 150개씩 끊어서 처리 (야후 차단 방지 최적화)
    chunk_size = 150 
    
    for i in range(0, len(tickers), chunk_size):
        chunk = tickers[i:i + chunk_size]
        print(f"[{i}/{len(tickers)}] 다운로드 및 분석 중...")
        
        try:
            # threads=False와 slow down으로 야후 서버 안정성 확보
            data = yf.download(chunk, period="1y", interval="1d", progress=False, threads=False)['Close']
            
            if isinstance(data, pd.Series): data = data.to_frame()

            for ticker in data.columns:
                series = data[ticker].dropna()
                if len(series) < 240: continue 
                
                now = series.iloc[-1]
                m3, m6, m9, m12 = series.iloc[-63], series.iloc[-126], series.iloc[-189], series.iloc[0]
                raw_score = (now/m3 * 2) + (now/m6) + (now/m9) + (now/m12)
                
                all_results.append({
                    'symbol': ticker, 'raw_score': raw_score, 'price': round(now, 2)
                })
        except: continue
        time.sleep(1) # 요청 간격 조절

    if all_results:
        final_df = pd.DataFrame(all_results)
        # 전체 시장에서의 백분위 순위(1-99) 계산
        final_df['rs_score'] = (final_df['raw_score'].rank(pct=True) * 99).astype(int)
        
        conn = sqlite3.connect('ibd_system.db')
        final_df[['symbol', 'rs_score', 'price']].to_sql('repo_results', conn, if_exists='replace', index=False)
        conn.close()
        print(f"[{datetime.now()}] 업데이트 성공! 총 {len(final_df)}개 종목 분석 완료.")

if __name__ == "__main__":
    update_database()
