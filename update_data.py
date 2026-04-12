import yfinance as yf
import pandas as pd
import sqlite3
import time
from datetime import datetime
import os
import requests
import numpy as np

# --- [설정] FMP API 키 ---
FMP_API_KEY = "1kJBflGjsp5fCgbancejhI5bN5iavEJF"

def get_all_stock_basics_fmp():
    """FMP 스크리너로 전 종목(1만개+)의 산업군, 시총, 이름을 한 번에 가져옵니다."""
    try:
        print("FMP에서 전 종목 기초 데이터를 로드 중...")
        url = f"https://financialmodelingprep.com/api/v3/stock-screener?apikey={FMP_API_KEY}"
        res = requests.get(url).json()
        if isinstance(res, list):
            df = pd.DataFrame(res)
            # 필요한 컬럼만 추출
            return df[['symbol', 'industry', 'marketCap', 'companyName']]
    except Exception as e:
        print(f"FMP 데이터 로드 실패: {e}")
    return pd.DataFrame()

def calculate_ad_raw(hist):
    """수급(AD) 계산 로직"""
    if len(hist) < 65: return 0
    df = hist.copy()
    df['daily_return'] = df['Close'].pct_change()
    df['vol_50ma'] = df['Volume'].rolling(50).mean()
    df = df.dropna(subset=['daily_return', 'vol_50ma']).tail(65)
    if len(df) < 65: return 0
    df['ad_daily'] = (df['Volume'] / df['vol_50ma']) * df['daily_return'] * 100
    return (df['ad_daily'].tail(20).sum() * 0.7) + (df['ad_daily'].head(45).sum() * 0.3)

def update_database():
    # 1. FMP에서 전 종목 리스트 및 산업군 가져오기
    df_basics = get_all_stock_basics_fmp()
    if df_basics.empty:
        print("기본 데이터를 가져오지 못해 중단합니다.")
        return

    tickers = df_basics['symbol'].unique().tolist()
    all_results = []
    
    print(f"--- 1단계: {len(tickers)}개 종목 주가/거래량 분석 시작 ---")
    
    # 야후 차단 방지를 위해 50개씩 묶어서 처리 (Chunking)
    chunk_size = 50
    for i in range(0, len(tickers), chunk_size):
        chunk = tickers[i:i + chunk_size]
        try:
            # GitHub 서버 IP를 이용해 대량 다운로드
            data = yf.download(chunk, period="1y", interval="1d", progress=False, group_by='ticker', threads=True)
            
            for ticker in chunk:
                try:
                    # 데이터가 한 종목일 때와 여러 종목일 때의 구조 처리
                    hist = data[ticker].dropna() if len(chunk) > 1 else data.dropna()
                    if len(hist) < 150: continue # 데이터 부족 종목 제외

                    price = hist['Close'].iloc[-1]
                    idx_252 = -min(252, len(hist))
                    
                    # RS Raw 계산 (오닐 가중치 방식)
                    rs_raw = (price/hist['Close'].iloc[-21]*2) + (price/hist['Close'].iloc[-63]*2) + \
                             (price/hist['Close'].iloc[-126]) + (price/hist['Close'].iloc[idx_252])
                    
                    all_results.append({
                        'symbol': ticker, 
                        'price': float(price), 
                        'rs_raw': rs_raw,
                        'ad_raw': calculate_ad_raw(hist), 
                        'adv_50': (hist['Close']*hist['Volume']).tail(50).mean()
                    })
                except: continue
        except: continue
        
        if i % 500 == 0:
            print(f" > {i} / {len(tickers)} 진행 중...")

    if not all_results: return

    # 3. 데이터 결합 및 상대 평가 (Ranking)
    df_prices = pd.DataFrame(all_results)
    final_df = pd.merge(df_prices, df_basics, on='symbol', how='left')
    
    # 점수화 (전체 종목 중 백분위 순위)
    final_df['rs_score'] = (final_df['rs_raw'].rank(pct=True) * 98 + 1).fillna(0).astype(int)
    final_df['ad_grade'] = pd.qcut(final_df['ad_raw'].rank(method='first'), 5, labels=['E', 'D', 'C', 'B', 'A'])
    
    # 산업군 RS 점수 계산
    ind_rs = final_df.groupby('industry')['rs_raw'].mean().reset_index(name='ind_rs_raw')
    ind_rs['industry_rs_score'] = (ind_rs['ind_rs_raw'].rank(pct=True) * 98 + 1).fillna(0).astype(int)
    final_df = pd.merge(final_df, ind_rs[['industry', 'industry_rs_score']], on='industry', how='left')

    # 상세 재무(SMR)는 대시보드에서 클릭 시 실시간으로 가져올 것이므로 'C'로 초기화
    final_df['smr_grade'] = 'C'

    # 4. DB 저장
    conn = sqlite3.connect('ibd_system.db')
    save_cols = ['symbol', 'price', 'rs_score', 'smr_grade', 'ad_grade', 'industry_rs_score', 'industry', 'adv_50']
    final_df[save_cols].to_sql('repo_results', conn, if_exists='replace', index=False)
    
    # 히스토리 기록
    history_df = final_df[['symbol', 'rs_score', 'industry_rs_score']].copy()
    history_df['date'] = datetime.now().strftime('%Y-%m-%d')
    history_df.to_sql('rs_history', conn, if_exists='append', index=False)
    
    conn.close()
    print(f"--- 분석 완료: 총 {len(final_df)}개 종목 저장됨 ---")

if __name__ == "__main__":
    update_database()
