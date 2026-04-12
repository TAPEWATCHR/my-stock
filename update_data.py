import yfinance as yf
import pandas as pd
import sqlite3
import time
from datetime import datetime
import os
import requests
import numpy as np

# --- [설정] FMP API 키 (Secrets에서 가져오거나 직접 입력) ---
FMP_API_KEY = os.environ.get('FMP_API_KEY', "1kJBflGjsp5fCgbancejhI5bN5iavEJF")

def get_ticker_list_fmp():
    """FMP 공식 API를 통해 미국 상장 종목 리스트를 안전하게 가져옵니다."""
    try:
        print("FMP API를 통해 전 종목 리스트 로드 중...")
        url = f"https://financialmodelingprep.com/api/v3/stock/list?apikey={FMP_API_KEY}"
        response = requests.get(url, timeout=15)
        res = response.json()
        
        if isinstance(res, list):
            df = pd.DataFrame(res)
            # 미국 주요 거래소(NASDAQ, NYSE, AMEX) 종목만 필터링
            df = df[df['exchangeShortName'].isin(['NASDAQ', 'NYSE', 'AMEX', 'NYSE American'])]
            # 티커 기호 정리
            df['symbol'] = df['symbol'].astype(str).str.strip().str.upper().str.replace('.', '-', regex=False)
            print(f"✅ 총 {len(df)}개의 미국 종목 리스트를 확보했습니다.")
            return df[['symbol', 'name']]
    except Exception as e:
        print(f"❌ FMP API 리스트 로드 실패: {e}")
    return pd.DataFrame()

def get_industry_fmp(ticker):
    """상위권 종목의 산업군 정보를 가져옵니다 (API 한도 사용)"""
    try:
        url = f"https://financialmodelingprep.com/api/v3/profile/{ticker}?apikey={FMP_API_KEY}"
        res = requests.get(url).json()
        if isinstance(res, list) and len(res) > 0:
            return res[0].get('industry', 'General Market')
    except: pass
    return 'General Market'

def calculate_ad_raw(hist):
    if len(hist) < 65: return 0
    df = hist.copy()
    df['daily_return'] = df['Close'].pct_change()
    df['vol_50ma'] = df['Volume'].rolling(50).mean()
    df = df.dropna(subset=['daily_return', 'vol_50ma']).tail(65)
    if len(df) < 65: return 0
    df['ad_daily'] = (df['Volume'] / df['vol_50ma']) * df['daily_return'] * 100
    return (df['ad_daily'].tail(20).sum() * 0.7) + (df['ad_daily'].head(45).sum() * 0.3)

def update_database():
    # 1. 종목 리스트 확보
    df_basics = get_ticker_list_fmp()
    if df_basics.empty:
        print("❌ 리스트를 가져오지 못해 중단합니다.")
        return

    tickers = df_basics['symbol'].unique().tolist()
    all_results = []
    
    print(f"--- 1단계: {len(tickers)}개 종목 주가 분석 시작 ---")
    chunk_size = 50
    for i in range(0, len(tickers), chunk_size):
        chunk = tickers[i:i + chunk_size]
        try:
            data = yf.download(chunk, period="1y", interval="1d", progress=False, group_by='ticker', threads=True)
            for ticker in chunk:
                try:
                    hist = data[ticker].dropna() if len(chunk) > 1 else data.dropna()
                    if len(hist) < 150: continue
                    price = hist['Close'].iloc[-1]
                    rs_raw = (price/hist['Close'].iloc[-21]*2) + (price/hist['Close'].iloc[-63]*2) + \
                             (price/hist['Close'].iloc[-126]) + (price/hist['Close'].iloc[-min(252, len(hist))])
                    all_results.append({
                        'symbol': ticker, 'price': float(price), 'rs_raw': rs_raw,
                        'ad_raw': calculate_ad_raw(hist), 'adv_50': (hist['Close']*hist['Volume']).tail(50).mean()
                    })
                except: continue
        except: continue
        if i % 500 == 0: print(f" > {i} / {len(tickers)} 완료...")

    if not all_results: return
    df_prices = pd.DataFrame(all_results)
    df_prices['rs_score'] = (df_prices['rs_raw'].rank(pct=True) * 98 + 1).fillna(0).astype(int)

    # 2. RS 상위 200개 종목에 대해서만 산업군 정보 채우기 (API 한도 최적화)
    print("--- 2단계: 상위 주도주 산업군 분석 (FMP) ---")
    top_indices = df_prices.sort_values('rs_score', ascending=False).head(200).index
    df_prices['industry'] = 'Unknown'
    
    for idx in top_indices:
        df_prices.at[idx, 'industry'] = get_industry_fmp(df_prices.at[idx, 'symbol'])
        time.sleep(0.05) # API 부하 방지

    # 3. 등급 계산
    df_prices['ad_grade'] = pd.qcut(df_prices['ad_raw'].rank(method='first'), 5, labels=['E', 'D', 'C', 'B', 'A'])
    df_prices['smr_grade'] = 'C'
    
    # 산업군 RS (정보가 있는 경우에만 계산)
    ind_rs = df_prices[df_prices['industry'] != 'Unknown'].groupby('industry')['rs_raw'].mean().reset_index(name='ind_rs_raw')
    ind_rs['industry_rs_score'] = (ind_rs['ind_rs_raw'].rank(pct=True) * 98 + 1).fillna(0).astype(int)
    final_df = pd.merge(df_prices, ind_rs[['industry', 'industry_rs_score']], on='industry', how='left').fillna(0)

    # 4. DB 저장
    conn = sqlite3.connect('ibd_system.db')
    save_cols = ['symbol', 'price', 'rs_score', 'smr_grade', 'ad_grade', 'industry_rs_score', 'industry', 'adv_50']
    final_df[save_cols].to_sql('repo_results', conn, if_exists='replace', index=False)
    
    history_df = final_df[['symbol', 'rs_score', 'industry_rs_score']].copy()
    history_df['date'] = datetime.now().strftime('%Y-%m-%d')
    history_df.to_sql('rs_history', conn, if_exists='append', index=False)
    conn.close()
    print(f"--- ✅ 완료: {len(final_df)}개 종목 저장됨 ---")

if __name__ == "__main__":
    update_database()
