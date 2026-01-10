import yfinance as yf
import pandas as pd
import sqlite3
import numpy as np
from datetime import datetime

def get_all_tickers():
    """미국 주요 지수 종목 리스트 통합 (S&P 500 + Nasdaq 100)"""
    # 1. S&P 500
    sp500 = pd.read_html('https://en.wikipedia.org/wiki/List_of_S%26P_500_companies')[0]
    sp500_tickers = sp500['Symbol'].tolist()
    
    # 2. Nasdaq 100
    ndx = pd.read_html('https://en.wikipedia.org/wiki/Nasdaq-100')[4]
    ndx_tickers = ndx['Ticker'].tolist()
    
    # 중복 제거 및 티커 정리 (yfinance 호환용)
    all_tickers = list(set(sp500_tickers + ndx_tickers))
    all_tickers = [t.replace('.', '-') for t in all_tickers]
    return all_tickers

def calculate_rs_score(data):
    """주가 데이터를 바탕으로 가중 RS 점수 계산"""
    rs_results = []
    for ticker in data.columns:
        series = data[ticker].dropna()
        if len(series) < 252: continue # 1년치 데이터 미만 제외
        
        # 윌리엄 오닐 스타일 가중치 (최근 성과 강조)
        now = series.iloc[-1]
        m3 = series.iloc[-63]
        m6 = series.iloc[-126]
        m9 = series.iloc[-189]
        m12 = series.iloc[0]
        
        # 가중치 계산 (3개월 성과에 2배 가중치)
        raw_score = (now/m3 * 2) + (now/m6) + (now/m9) + (now/m12)
        rs_results.append({'symbol': ticker, 'raw_score': raw_score, 'price': now})
        
    df = pd.DataFrame(rs_results)
    # 1~99 점수로 변환
    df['rs_score'] = df['raw_score'].rank(pct=True) * 99
    return df

def update_database():
    print(f"[{datetime.now()}] 전체 티커 리스트 확보 중...")
    tickers = get_all_tickers()
    
    print(f"{len(tickers)}개 종목 주가 데이터 다운로드 중...")
    # 전체 종목의 1년치 종가 한 번에 다운로드 (속도 향상)
    data = yf.download(tickers, period="1y", interval="1d")['Close']
    
    print("RS 점수 산출 및 기술적 지표 분석 중...")
    rs_df = calculate_rs_score(data)
    
    # 추가 데이터 (SMR, AD Rating 등 가상 로직)
    results = []
    for _, row in rs_df.iterrows():
        ticker = row['symbol']
        # 간단한 수급 분석: 현재가 > 50일 이평선 & 200일 이평선이면 우량으로 간주
        series = data[ticker].dropna()
        ma50 = series.rolling(50).mean().iloc[-1]
        ma200 = series.rolling(200).mean().iloc[-1]
        
        ad_rating = "A" if row['price'] > ma50 > ma200 else "C"
        
        results.append({
            'symbol': ticker,
            'rs_score': round(row['rs_score']),
            'industry_rs_score': round(np.random.uniform(40, 95)), # 실제 구현시 섹터별 평균 필요
            'smr_grade': "A" if row['rs_score'] > 80 else "B",
            'ad_rating': ad_rating,
            'sector': "Technology", # yf.Ticker 호출은 느리므로 필요시 별도 매핑 테이블 사용 권장
            'last_updated': datetime.now().strftime('%Y-%m-%d')
        })

    # DB 저장
    conn = sqlite3.connect('ibd_system.db')
    final_df = pd.DataFrame(results)
    final_df.to_sql('repo_results', conn, if_exists='replace', index=False)
    conn.close()
    print(f"[{datetime.now()}] {len(final_df)}개 종목 업데이트 완료!")

if __name__ == "__main__":
    update_database()
