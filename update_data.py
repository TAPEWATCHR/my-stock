import yfinance as yf
import pandas as pd
import sqlite3
import time
from datetime import datetime
import os
import requests
import io
import numpy as np

# --- [설정] FMP API 키 ---
FMP_API_KEY = os.environ.get('FMP_API_KEY', "1kJBflGjsp5fCgbancejhI5bN5iavEJF")

def get_full_market_data():
    try:
        # 산업군 소스 (rreichel3 데이터셋이 더 방대함)
        url = "https://raw.githubusercontent.com/rreichel3/US-Stock-Symbols/main/nasdaq/nasdaq_full_tickers.csv"
        id_df = pd.read_csv(url)
        id_df.columns = [c.lower() for c in id_df.columns]
        id_df = id_df[['symbol', 'industry']].rename(columns={'symbol': 'symbol'})
        id_df['symbol'] = id_df['symbol'].str.upper().str.replace('.', '-', regex=False)

        headers = {'User-Agent': 'My-Stock-App'}
        sec_res = requests.get("https://www.sec.gov/files/company_tickers.json", headers=headers).json()
        sec_tickers = [v['ticker'].upper().replace('.', '-') for v in sec_res.values()]
        
        base_df = pd.DataFrame({'symbol': sec_tickers})
        final_base = pd.merge(base_df, id_df, on='symbol', how='left')
        final_base['industry'] = final_base['industry'].fillna('Unknown')
        return final_base
    except: return pd.DataFrame()

def calculate_ad_raw(hist):
    if len(hist) < 65: return 0
    df = hist.copy()
    df['daily_return'] = df['Close'].pct_change()
    df['vol_50ma'] = df['Volume'].rolling(50).mean()
    df = df.dropna(subset=['daily_return', 'vol_50ma']).tail(65)
    return (df['Volume'] / df['vol_50ma'] * df['daily_return'] * 100).sum()

def get_smr_raw_yf(t_obj):
    try:
        qf = t_obj.quarterly_financials
        if qf.empty or 'Total Revenue' not in qf.index: return 0, False
        rev = qf.loc['Total Revenue'].dropna().values
        net = qf.loc['Net Income'].dropna().values if 'Net Income' in qf.index else [0]
        if len(rev) < 3: return 0, False
        g0 = (rev[0] - rev[1]) / abs(rev[1]) if rev[1] != 0 else 0
        g1 = (rev[1] - rev[2]) / abs(rev[2]) if rev[2] != 0 else 0
        return g0 - g1, net[0] > 0
    except: return 0, False

def update_database():
    base_df = get_full_market_data()
    if base_df.empty: return
    
    tickers = base_df['symbol'].tolist()
    all_results = []
    print(f"--- 분석 시작: {len(tickers)}개 종목 ---")
    
    chunk_size = 100
    for i in range(0, len(tickers), chunk_size):
        chunk = tickers[i:i+chunk_size]
        data = yf.download(chunk, period="1y", interval="1d", progress=False, group_by='ticker')
        
        for ticker in chunk:
            try:
                hist = data[ticker].dropna() if len(chunk) > 1 else data.dropna()
                if len(hist) < 200: continue
                
                # RS 산식 적용
                price = hist['Close'].iloc[-1]
                rs_raw = (price/hist['Close'].iloc[-21]*2) + (price/hist['Close'].iloc[-63]*2) + \
                         (price/hist['Close'].iloc[-126]) + (price/hist['Close'].iloc[-min(252, len(hist))])
                
                t_obj = yf.Ticker(ticker)
                s_acc, is_prof = get_smr_raw_yf(t_obj)
                
                all_results.append({
                    'symbol': ticker, 'price': float(price), 'rs_raw': rs_raw,
                    'ad_raw': calculate_ad_raw(hist), 'adv_50': (hist['Close']*hist['Volume']).tail(50).mean(),
                    'smr_acc': s_acc, 'is_prof': is_prof
                })
            except: continue
        print(f" > {i+len(chunk)} 완료...")

    if not all_results: return
    df = pd.merge(pd.DataFrame(all_results), base_df, on='symbol', how='left')

    # 등급 매기기
    df['rs_score'] = (df['rs_raw'].rank(pct=True) * 98 + 1).astype(int)
    df['ad_grade'] = pd.qcut(df['ad_raw'].rank(method='first'), 5, labels=['E', 'D', 'C', 'B', 'A']).astype(str)
    df['smr_rank_val'] = df['smr_acc'].rank(pct=True) + (df['is_prof'].astype(int) * 0.5)
    df['smr_grade'] = pd.qcut(df['smr_rank_val'].rank(method='first'), 5, labels=['E', 'D', 'C', 'B', 'A']).astype(str)
    
    # 산업군 RS 산식 적용
    ind_rs = df.groupby('industry')['rs_raw'].mean().reset_index(name='ind_rs_raw')
    ind_rs['industry_rs_score'] = (ind_rs['ind_rs_raw'].rank(pct=True) * 98 + 1).fillna(0).astype(int)
    final_df = pd.merge(df, ind_rs[['industry', 'industry_rs_score']], on='industry', how='left').fillna(0)

    conn = sqlite3.connect('ibd_system.db')
    # industry와 adv_50 컬럼 누락 방지
    final_df[['symbol', 'price', 'rs_score', 'smr_grade', 'ad_grade', 'industry_rs_score', 'industry', 'adv_50']].to_sql('repo_results', conn, if_exists='replace', index=False)
    
    history_df = final_df[['symbol', 'rs_score', 'industry_rs_score']].copy()
    history_df['date'] = datetime.now().strftime('%Y-%m-%d')
    history_df.to_sql('rs_history', conn, if_exists='append', index=False)
    conn.close()
    print("--- ✅ 업데이트 완료 ---")

if __name__ == "__main__":
    update_database()
