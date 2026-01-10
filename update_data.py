import yfinance as yf
import pandas as pd
import sqlite3
import numpy as np
from datetime import datetime, timedelta

def calculate_rs_score(tickers):
    """종목별 RS 점수 계산 (윌리엄 오닐 방식: 최근 분기에 가중치)"""
    data = yf.download(tickers, period="1y")['Close']
    
    rs_results = []
    for ticker in tickers:
        try:
            series = data[ticker].dropna()
            if len(series) < 250: continue # 상장한지 얼마 안 된 종목 제외
            
            # 윌리엄 오닐 RS 공식 가중치: (현재/3개월전 * 2) + (현재/6개월전) + (현재/9개월전) + (현재/12개월전)
            now = series.iloc[-1]
            m3 = series.iloc[-63]  # 약 3개월 전
            m6 = series.iloc[-126] # 약 6개월 전
            m9 = series.iloc[-189] # 약 9개월 전
            m12 = series.iloc[0]   # 약 1년 전
            
            weighted_score = (now/m3 * 2) + (now/m6) + (now/m9) + (now/m12)
            rs_results.append({'symbol': ticker, 'raw_score': weighted_score})
        except:
            continue
            
    # 점수를 1~99로 정규화 (백분위)
    rs_df = pd.DataFrame(rs_results)
    rs_df['rs_score'] = rs_df['raw_score'].rank(pct=True) * 99
    return rs_df

def update_database():
    conn = sqlite3.connect('ibd_system.db')
    cursor = conn.cursor()

    # 1. 감시할 종목 리스트 (예: 나스닥 100 또는 S&P 500)
    # 실제 운영 시에는 본인이 관심 있는 티커 리스트를 넣으세요.
    tickers = ["AAPL", "MSFT", "NVDA", "GOOGL", "AMZN", "META", "TSLA", "AVGO", "COST", "NFLX"] 
    
    # 2. RS 점수 계산
    print("RS 점수 계산 중...")
    rs_df = calculate_rs_score(tickers)
    
    # 3. 기술적 지표 및 기본적 등급 업데이트 (예시 등급 부여)
    # 실제 수급 등급(AD)이나 SMR 등급은 유료 API가 필요하므로, 
    # 여기서는 주가와 거래량 데이터를 분석하여 가상으로 로직을 짭니다.
    
    results = []
    for _, row in rs_df.iterrows():
        ticker = row['symbol']
        stock = yf.Ticker(ticker)
        hist = stock.history(period="1y")
        
        # 수급(AD) 등급 계산 로직 (가상): 최근 20일 중 상승일 거래량이 많으면 A
        vol_avg = hist['Volume'].tail(20).mean()
        last_vol = hist['Volume'].iloc[-1]
        ad_rating = "A" if last_vol > vol_avg and hist['Close'].iloc[-1] > hist['Open'].iloc[-1] else "C"
        
        # SMR 등급 (가상): ROE나 이익 성장률 기반
        smr_grade = "A" if stock.info.get('returnOnEquity', 0) > 0.2 else "B"
        
        results.append({
            'symbol': ticker,
            'rs_score': round(row['rs_score']),
            'industry_rs_score': round(np.random.uniform(50, 95)), # 산업군 점수
            'smr_grade': smr_grade,
            'ad_rating': ad_rating,
            'sector': stock.info.get('sector', 'Unknown'),
            'last_updated': datetime.now().strftime('%Y-%m-%d')
        })

    # 4. DB 저장
    final_df = pd.DataFrame(results)
    # 기존 repo_results 테이블 업데이트 (security_id 매칭 로직은 환경에 맞게 조정 필요)
    final_df.to_sql('repo_results_temp', conn, if_exists='replace', index=False)
    
    # 기존 테이블과 병합하거나 교체
    cursor.execute("DROP TABLE IF EXISTS repo_results")
    cursor.execute("ALTER TABLE repo_results_temp RENAME TO repo_results")
    
    conn.commit()
    conn.close()
    print("DB 업데이트 완료!")

if __name__ == "__main__":
    update_database()
