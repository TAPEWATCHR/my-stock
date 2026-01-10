import yfinance as yf
import pandas as pd
import sqlite3
import os
from datetime import datetime
import time

def get_total_market_tickers():
    """외부 서버 의존을 버리고 내 저장소의 파일을 최우선으로 읽음"""
    # 1. 내 저장소에 tickers.txt가 있다면 그것을 사용 (가장 확실)
    if os.path.exists('tickers.txt'):
        print("내 저장소의 tickers.txt 발견! 전체 종목 수집을 시작합니다.")
        with open('tickers.txt', 'r') as f:
            tickers = [line.strip().upper() for line in f if line.strip()]
        return list(set(tickers))
    
    # 2. 파일이 없을 경우에만 기존 백업 로직 가동
    print("tickers.txt가 없습니다. S&P 500만 가져옵니다. (전체 종목을 원하시면 파일을 만드세요)")
    try:
        url = "https://raw.githubusercontent.com/datasets/s-and-p-500-companies/master/data/constituents.csv"
        df = pd.read_csv(url)
        return df['Symbol'].tolist()
    except:
        return ['AAPL', 'MSFT', 'NVDA', 'TSLA']

def update_database():
    start_time = datetime.now()
    tickers = get_total_market_tickers()
    print(f"--- 분석 대상: 총 {len(tickers)}개 종목 ---")
    
    all_results = []
    chunk_size = 50 # 5,000개 이상일 때는 차단을 피하기 위해 더 작게 나눔
    
    for i in range(0, len(tickers), chunk_size):
        chunk = tickers[i:i + chunk_size]
        print(f"[{i}/{len(tickers)}] 처리 중...")
        
        try:
            # 전체 종목 처리 시 에러 방지를 위해 threads=False
            data = yf.download(chunk, period="1y", interval="1d", progress=False, threads=False)['Close']
            
            if isinstance(data, pd.Series): data = data.to_frame()

            for ticker in data.columns:
                series = data[ticker].dropna()
                if len(series) < 200: continue 
                
                now = series.iloc[-1]
                m3, m6, m9, m12 = series.iloc[-63], series.iloc[-126], series.iloc[-189], series.iloc[0]
                raw_score = (now/m3 * 2) + (now/m6) + (now/m9) + (now/m12)
                
                all_results.append({
                    'symbol': ticker, 'raw_score': raw_score, 'price': round(now, 2)
                })
        except: continue
        time.sleep(1) # 대량 수집 시 매너 타임 필수

    if all_results:
        final_df = pd.DataFrame(all_results)
        final_df['rs_score'] = (final_df['raw_score'].rank(pct=True) * 99).astype(int)
        
        conn = sqlite3.connect('ibd_system.db')
        final_df[['symbol', 'rs_score', 'price']].to_sql('repo_results', conn, if_exists='replace', index=False)
        conn.close()
        print(f"업데이트 완료! 총 {len(final_df)}개 종목의 순위를 매겼습니다.")

if __name__ == "__main__":
    update_database()
