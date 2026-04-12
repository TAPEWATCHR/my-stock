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
    """전 종목 티커와 산업군 정보를 결합하여 로드"""
    try:
        # 1. 산업군 데이터 (가장 안정적인 GitHub 소스)
        url = "https://raw.githubusercontent.com/rreichel3/US-Stock-Symbols/main/nasdaq/nasdaq_full_tickers.csv"
        id_df = pd.read_csv(url)
        id_df.columns = [c.lower() for c in id_df.columns]
        id_df = id_df[['symbol', 'industry']].copy()
        id_df['symbol'] = id_df['symbol'].str.upper().str.replace('.', '-', regex=False)

        # 2. SEC 공식 전 종목 리스트
        headers = {'User-Agent': 'My-Stock-App contact@my-stock-app.com'}
        sec_res = requests.get("https://www.sec.gov/files/company_tickers.json", headers=headers, timeout=15).json()
        sec_tickers = [v['ticker'].upper().replace('.', '-') for v in sec_res.values()]
        
        base_df = pd.DataFrame({'symbol': sec_tickers})
        # 데이터 병합
        final_base = pd.merge(base_df, id_df, on='symbol', how='left')
        final_base['industry'] = final_base['industry'].fillna('Unknown')
        
        print(f"✅ 기초 데이터 로드 완료: {len(final_base)}개 종목")
        return final_base
    except Exception as e:
        print(f"🚨 기초 데이터 로드 실패: {e}")
        return pd.DataFrame()

def get_smr_raw_yf(ticker):
    """yfinance를 통한 재무 가속도 데이터 추출 (안전 장치 포함)"""
    try:
        t = yf.Ticker(ticker)
        qf = t.quarterly_financials
        if qf.empty or 'Total Revenue' not in qf.index: return 0, False
        rev = qf.loc['Total Revenue'].dropna().values
        net = qf.loc['Net Income'].dropna().values if 'Net Income' in qf.index else [0]
        if len(rev) < 3: return 0, False
        # 매출 성장 가속도 계산
        g0 = (rev[0] - rev[1]) / abs(rev[1]) if rev[1] != 0 else 0
        g1 = (rev[1] - rev[2]) / abs(rev[2]) if rev[2] != 0 else 0
        return (g0 - g1), (net[0] > 0)
    except: return 0, False

def update_database():
    base_df = get_full_market_data()
    if base_df.empty: return

    tickers = base_df['symbol'].tolist()
    all_results = []
    
    print(f"--- 1단계: {len(tickers)}개 전 종목 RS 분석 시작 ---")
    chunk_size = 100 # 대량 다운로드
    for i in range(0, len(tickers), chunk_size):
        chunk = tickers[i:i + chunk_size]
        try:
            data = yf.download(chunk, period="1y", interval="1d", progress=False, group_by='ticker', threads=True)
            for ticker in chunk:
                try:
                    hist = data[ticker].dropna() if len(chunk) > 1 else data.dropna()
                    if len(hist) < 200: continue
                    # RS 산식 (오닐 스타일 가중치)
                    p = hist['Close']
                    rs_raw = (p.iloc[-1]/p.iloc[-21]*2) + (p.iloc[-1]/p.iloc[-63]*2) + (p.iloc[-1]/p.iloc[-126]) + (p.iloc[-1]/p.iloc[-min(252, len(p))])
                    
                    # 수급(AD) 산식
                    ret = hist['Close'].pct_change()
                    vol_ma = hist['Volume'].rolling(50).mean()
                    ad_raw = (hist['Volume'] / vol_ma * ret * 100).tail(65).sum()

                    all_results.append({
                        'symbol': ticker, 'price': float(p.iloc[-1]), 'rs_raw': rs_raw,
                        'ad_raw': ad_raw, 'adv_50': (p * hist['Volume']).tail(50).mean()
                    })
                except: continue
        except: continue
        if i % 1000 == 0: print(f" > {i} 종목 RS 계산 중...")

    if not all_results: return
    df = pd.DataFrame(all_results)
    df = pd.merge(df, base_df, on='symbol', how='left')
    
    # RS 점수화 (전체 시장 기준)
    df['rs_score'] = (df['rs_raw'].rank(pct=True) * 98 + 1).astype(int)
    df['ad_grade'] = pd.qcut(df['ad_raw'].rank(method='first'), 5, labels=['E', 'D', 'C', 'B', 'A']).astype(str)

    # 2단계: SMR 분석 (시스템 안정을 위해 상위 1,500개 우선 분석)
    print("--- 2단계: 주도주 1,500개 정밀 SMR 분석 ---")
    df['smr_acc'] = 0.0
    df['is_prof'] = False
    top_indices = df.sort_values('rs_score', ascending=False).head(1500).index
    
    for idx in top_indices:
        acc, prof = get_smr_raw_yf(df.at[idx, 'symbol'])
        df.at[idx, 'smr_acc'] = acc
        df.at[idx, 'is_prof'] = prof
        time.sleep(0.1) # 서버 차단 방지

    df['smr_val'] = df['smr_acc'].rank(pct=True) + (df['is_prof'].astype(int) * 0.5)
    df['smr_grade'] = pd.qcut(df['smr_val'].rank(method='first'), 5, labels=['E', 'D', 'C', 'B', 'A']).astype(str)

    # 산업군 RS 계산
    ind_rs = df.groupby('industry')['rs_raw'].mean().reset_index(name='ind_rs_raw')
    ind_rs['industry_rs_score'] = (ind_rs['ind_rs_raw'].rank(pct=True) * 98 + 1).fillna(0).astype(int)
    final_df = pd.merge(df, ind_rs[['industry', 'industry_rs_score']], on='industry', how='left').fillna(0)

    # DB 저장 (컬럼 누락 방지)
    conn = sqlite3.connect('ibd_system.db')
    save_cols = ['symbol', 'price', 'rs_score', 'smr_grade', 'ad_grade', 'industry_rs_score', 'industry', 'adv_50']
    final_df[save_cols].to_sql('repo_results', conn, if_exists='replace', index=False)
    
    # 히스토리 저장
    history_df = final_df[['symbol', 'rs_score', 'industry_rs_score']].copy()
    history_df['date'] = datetime.now().strftime('%Y-%m-%d')
    history_df.to_sql('rs_history', conn, if_exists='append', index=False)
    conn.close()
    print(f"✅ 총 {len(final_df)}개 종목 업데이트 성공!")

if __name__ == "__main__":
    update_database()
