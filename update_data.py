import yfinance as yf
import pandas as pd
import sqlite3
import time
from datetime import datetime
import os
import requests
import io
import numpy as np

FMP_API_KEY = os.environ.get('FMP_API_KEY', "1kJBflGjsp5fCgbancejhI5bN5iavEJF")

def get_pure_common_stocks():
    """ETF, 우선주, 펀드, 스팩을 제외한 '순수 보통주'만 추출합니다."""
    # 1. SEC 공식 전 종목 리스트 (티커와 회사명 확보)
    headers = {'User-Agent': 'My-Stock-App contact@my-stock-app.com'}
    try:
        sec_res = requests.get("https://www.sec.gov/files/company_tickers.json", headers=headers, timeout=15).json()
        sec_data = [{'symbol': v['ticker'].upper().replace('.', '-'), 'name': v['title']} for v in sec_res.values()]
        base_df = pd.DataFrame(sec_data)
    except Exception as e:
        print(f"🚨 SEC 데이터 로드 실패: {e}")
        return pd.DataFrame()

    # 2. 산업군 데이터 로드
    urls = [
        "https://raw.githubusercontent.com/Ate329/top-us-stock-tickers/main/tickers/all.csv",
        "https://raw.githubusercontent.com/rreichel3/US-Stock-Symbols/main/nasdaq/nasdaq_full_tickers.csv"
    ]
    id_df = pd.DataFrame(columns=['symbol', 'industry'])
    for url in urls:
        try:
            res = requests.get(url, timeout=10)
            if res.status_code == 200:
                temp_df = pd.read_csv(io.StringIO(res.text))
                temp_df.columns = [c.lower() for c in temp_df.columns]
                if 'symbol' in temp_df.columns and 'industry' in temp_df.columns:
                    id_df = temp_df[['symbol', 'industry']].copy()
                    id_df['symbol'] = id_df['symbol'].astype(str).str.upper().str.replace('.', '-', regex=False)
                    break
        except: continue
        
    final_base = pd.merge(base_df, id_df, on='symbol', how='left')
    final_base['industry'] = final_base['industry'].fillna('Unknown')
    
    # --- 🛡️ [핵심] 불순물 필터링 로직 ---
    print(f"🧹 필터링 전 총 종목 수: {len(final_base)}개")
    
    # 제외할 키워드 (대소문자 무관)
    exclude_terms = [
        'ETF', 'FUND', 'TRUST', 'ACQUISITION', 'SPAC', 'WARRANT', 
        'BLANK CHECK', 'PORTFOLIO', 'ETN', 'HOLDINGS LIMITED - WARRANTS'
    ]
    
    for term in exclude_terms:
        # 산업군이나 회사 이름에 위 키워드가 들어가면 삭제
        final_base = final_base[~final_base['industry'].str.upper().str.contains(term, na=False)]
        final_base = final_base[~final_base['name'].str.upper().str.contains(term, na=False)]
        
    # 야후 파이낸스 기준 우선주(-P), 워런트(-W), 권리(-R) 기호 필터링
    # 주의: BRK-B(버크셔) 같은 클래스 주식은 살려둬야 하므로 특정 패턴만 제거
    final_base = final_base[~final_base['symbol'].str.contains(r'-[PWR]$', regex=True)]
    
    # 5글자 이상의 티커 중 끝이 W, R, Q(파산)로 끝나는 특수 종목 제거 (나스닥 룰)
    final_base = final_base[~( (final_base['symbol'].str.len() >= 5) & (final_base['symbol'].str.endswith(('W', 'R', 'Q'))) )]

    print(f"✨ 보통주 필터링 완료: {len(final_base)}개 종목 분석 대상 확정")
    return final_base

def get_smr_raw_yf(ticker):
    try:
        qf = yf.Ticker(ticker).quarterly_financials
        if qf.empty or 'Total Revenue' not in qf.index: return 0, False
        rev = qf.loc['Total Revenue'].dropna().values
        net = qf.loc['Net Income'].dropna().values if 'Net Income' in qf.index else [0]
        if len(rev) < 3: return 0, False
        g0 = (rev[0] - rev[1]) / abs(rev[1]) if rev[1] != 0 else 0
        g1 = (rev[1] - rev[2]) / abs(rev[2]) if rev[2] != 0 else 0
        return (g0 - g1), (net[0] > 0)
    except: return 0, False

def update_database():
    base_df = get_pure_common_stocks()
    if base_df.empty: return

    tickers = base_df['symbol'].tolist()
    all_results = []
    
    print(f"--- 1단계: {len(tickers)}개 순수 보통주 RS/AD 분석 시작 ---")
    chunk_size = 100
    for i in range(0, len(tickers), chunk_size):
        chunk = tickers[i:i + chunk_size]
        try:
            data = yf.download(chunk, period="1y", interval="1d", progress=False, group_by='ticker', threads=True)
            for ticker in chunk:
                try:
                    hist = data[ticker].dropna() if len(chunk) > 1 else data.dropna()
                    if len(hist) < 200: continue
                    p = hist['Close']
                    rs_raw = (p.iloc[-1]/p.iloc[-21]*2) + (p.iloc[-1]/p.iloc[-63]*2) + (p.iloc[-1]/p.iloc[-126]) + (p.iloc[-1]/p.iloc[-min(252, len(p))])
                    
                    ret = p.pct_change()
                    vol_ma = hist['Volume'].rolling(50).mean()
                    ad_raw = (hist['Volume'] / vol_ma * ret * 100).tail(65).sum()

                    all_results.append({
                        'symbol': ticker, 'price': float(p.iloc[-1]), 'rs_raw': rs_raw,
                        'ad_raw': ad_raw, 'adv_50': (p * hist['Volume']).tail(50).mean()
                    })
                except: continue
        except: continue
        if i % 1000 == 0: print(f" > {i} 종목 분석 완료...")

    if not all_results: return
    df = pd.merge(pd.DataFrame(all_results), base_df, on='symbol', how='left')
    
    df['rs_score'] = (df['rs_raw'].rank(pct=True) * 98 + 1).astype(int)
    df['ad_grade'] = pd.qcut(df['ad_raw'].rank(method='first'), 5, labels=['E', 'D', 'C', 'B', 'A']).astype(str)

    # 종목 수가 5~6천 개로 줄었으므로, SMR 분석 범위도 훨씬 안정적으로 돌아갑니다.
    print("--- 2단계: RS 상위 1,500개 주도주 SMR 분석 ---")
    df['smr_acc'] = 0.0
    df['is_prof'] = False
    top_indices = df.sort_values('rs_score', ascending=False).head(1500).index
    
    for idx in top_indices:
        acc, prof = get_smr_raw_yf(df.at[idx, 'symbol'])
        df.at[idx, 'smr_acc'] = acc
        df.at[idx, 'is_prof'] = prof

    df['smr_val'] = df['smr_acc'].rank(pct=True) + (df['is_prof'].astype(int) * 0.5)
    df['smr_grade'] = pd.qcut(df['smr_val'].rank(method='first'), 5, labels=['E', 'D', 'C', 'B', 'A']).astype(str)

    ind_rs = df.groupby('industry')['rs_raw'].mean().reset_index(name='ind_rs_raw')
    ind_rs['industry_rs_score'] = (ind_rs['ind_rs_raw'].rank(pct=True) * 98 + 1).fillna(0).astype(int)
    final_df = pd.merge(df, ind_rs[['industry', 'industry_rs_score']], on='industry', how='left').fillna(0)

    conn = sqlite3.connect('ibd_system.db')
    save_cols = ['symbol', 'price', 'rs_score', 'smr_grade', 'ad_grade', 'industry_rs_score', 'industry', 'adv_50']
    final_df[save_cols].to_sql('repo_results', conn, if_exists='replace', index=False)
    
    history_df = final_df[['symbol', 'rs_score', 'industry_rs_score']].copy()
    history_df['date'] = datetime.now().strftime('%Y-%m-%d')
    history_df.to_sql('rs_history', conn, if_exists='append', index=False)
    conn.close()
    print(f"✅ 완료: 총 {len(final_df)}개 순수 보통주 저장 성공")

if __name__ == "__main__":
    update_database()
