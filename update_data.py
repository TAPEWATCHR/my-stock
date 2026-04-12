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

def get_industry_master():
    """안정적인 외부 소스에서 산업군 정보를 로드합니다."""
    url = "https://raw.githubusercontent.com/yumoxu/stock-market-analysis/master/data/nasdaq_screener.csv"
    try:
        print("📡 산업군 마스터 데이터 로드 중...")
        res = requests.get(url, timeout=10).content
        df = pd.read_csv(io.StringIO(res.decode('utf-8')))
        df.columns = [c.lower() for c in df.columns]
        if 'symbol' in df.columns and 'industry' in df.columns:
            df['symbol'] = df['symbol'].astype(str).str.upper().str.replace('.', '-', regex=False)
            return df[['symbol', 'industry']].drop_duplicates('symbol')
    except Exception as e:
        print(f"⚠️ 산업군 로드 실패: {e}")
    return pd.DataFrame()

def calculate_ad_raw(hist):
    if len(hist) < 65: return 0
    df = hist.copy()
    df['daily_return'] = df['Close'].pct_change()
    df['vol_50ma'] = df['Volume'].rolling(50).mean()
    df = df.dropna(subset=['daily_return', 'vol_50ma']).tail(65)
    # 수급 점수: (거래량/이평) * 등락률의 합
    return (df['Volume'] / df['vol_50ma'] * df['daily_return'] * 100).sum()

def update_database():
    # 1. 기초 데이터 확보 (산업군 + 티커)
    industry_df = get_industry_master()
    headers = {'User-Agent': 'My-Stock-App contact@my-stock-app.com'}
    sec_res = requests.get("https://www.sec.gov/files/company_tickers.json", headers=headers).json()
    tickers = list(set([v['ticker'].upper().replace('.', '-') for v in sec_res.values()]))
    
    all_results = []
    print(f"--- 1단계: {len(tickers)}개 종목 분석 시작 ---")
    chunk_size = 50
    for i in range(0, len(tickers), chunk_size):
        chunk = tickers[i:i + chunk_size]
        try:
            data = yf.download(chunk, period="1y", interval="1d", progress=False, group_by='ticker', threads=True)
            for ticker in chunk:
                try:
                    hist = data[ticker].dropna() if len(chunk) > 1 else data.dropna()
                    if len(hist) < 200: continue
                    price = hist['Close'].iloc[-1]
                    idx_252 = -min(252, len(hist))
                    rs_raw = (price/hist['Close'].iloc[-21]*2) + (price/hist['Close'].iloc[-63]*2) + \
                             (price/hist['Close'].iloc[-126]) + (price/hist['Close'].iloc[idx_252])
                    
                    all_results.append({
                        'symbol': ticker, 'price': float(price), 'rs_raw': rs_raw,
                        'ad_raw': calculate_ad_raw(hist), 'adv_50': (hist['Close']*hist['Volume']).tail(50).mean()
                    })
                except: continue
        except: continue
        if i % 500 == 0: print(f" > {i} / {len(tickers)} 완료...")

    if not all_results: return
    df = pd.DataFrame(all_results)
    
    # 2. 산업군 결합 및 점수 산정
    df = pd.merge(df, industry_df, on='symbol', how='left')
    df['industry'] = df['industry'].fillna('Unknown')
    
    df['rs_score'] = (df['rs_raw'].rank(pct=True) * 98 + 1).astype(int)
    df['ad_grade'] = pd.qcut(df['ad_raw'].rank(method='first'), 5, labels=['E', 'D', 'C', 'B', 'A']).astype(str)
    df['smr_grade'] = 'C' # 정밀 재무 분석 전 기본값

    # 산업군 RS 점수 계산
    ind_rs = df[df['industry'] != 'Unknown'].groupby('industry')['rs_raw'].mean().reset_index(name='ind_rs_raw')
    ind_rs['industry_rs_score'] = (ind_rs['ind_rs_raw'].rank(pct=True) * 98 + 1).fillna(0).astype(int)
    final_df = pd.merge(df, ind_rs[['industry', 'industry_rs_score']], on='industry', how='left').fillna(0)

    # 3. DB 저장
    conn = sqlite3.connect('ibd_system.db')
    final_df.to_sql('repo_results', conn, if_exists='replace', index=False)
    
    # 히스토리 저장 (추세 차트용)
    history_df = final_df[['symbol', 'rs_score', 'industry_rs_score']].copy()
    history_df['date'] = datetime.now().strftime('%Y-%m-%d')
    history_df.to_sql('rs_history', conn, if_exists='append', index=False)
    conn.close()
    print(f"--- ✅ {len(final_df)}개 종목 업데이트 성공 ---")

if __name__ == "__main__":
    update_database()
