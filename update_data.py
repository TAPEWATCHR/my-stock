# -*- coding: utf-8 -*-
import yfinance as yf
import pandas as pd
import sqlite3
import time
from datetime import datetime, timedelta
import requests

def init_db():
    conn = sqlite3.connect('ibd_system.db')
    # 1. 산업군 마스터 테이블
    conn.execute("""
        CREATE TABLE IF NOT EXISTS company_profiles (
            symbol TEXT PRIMARY KEY, industry TEXT, description TEXT
        )
    """)
    # 2. [신규] SMR 재무 데이터 캐싱 테이블 (분기에 한 번만 업데이트하기 위함)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS smr_cache (
            symbol TEXT PRIMARY KEY, 
            smr_acc REAL, 
            is_prof INTEGER, 
            last_updated DATE
        )
    """)
    conn.close()

def get_pure_exchange_stocks():
    headers = {'User-Agent': 'MarketLeadersTerminal contact_my_email@gmail.com'} 
    try:
        url = "https://www.sec.gov/files/company_tickers_exchange.json"
        res = requests.get(url, headers=headers, timeout=10)
        res.raise_for_status() 
        res_json = res.json()
        df_sec = pd.DataFrame(res_json['data'], columns=res_json['fields'])
        df_sec = df_sec[df_sec['exchange'].isin(['Nasdaq', 'NYSE'])]
        df_sec['symbol'] = df_sec['ticker'].str.upper().replace('.', '-')
        return df_sec[['symbol', 'name']].copy()
    except Exception as e: 
        print(f"🚨 SEC 데이터 로드 실패: {e}") 
        return pd.DataFrame()

def update_database():
    init_db()
    base_df = get_pure_exchange_stocks()
    if base_df.empty: return

    conn = sqlite3.connect('ibd_system.db')
    master_profiles = pd.read_sql("SELECT symbol, industry FROM company_profiles", conn)
    base_df = pd.merge(base_df, master_profiles, on='symbol', how='left')
    base_df['industry'] = base_df['industry'].fillna('Unknown')

    tickers = base_df['symbol'].tolist()
    all_results = []
    
    print(f"🚀 1단계: {len(tickers)}개 종목 가격 및 AD 수급 분석 시작...")

    chunk_size = 50
    for i in range(0, len(tickers), chunk_size):
        chunk = tickers[i:i + chunk_size]
        try:
            price_data = yf.download(chunk, period="1y", interval="1d", progress=False, group_by='ticker')
        except:
            time.sleep(5); continue

        for ticker in chunk:
            try:
                hist = price_data[ticker].dropna() if len(chunk) > 1 else price_data.dropna()
                if len(hist) < 150: continue

                p = hist['Close']
                v = hist['Volume']
                
                # RS & 거래대금 계산
                rs_raw = (p.iloc[-1]/p.iloc[-21]*2) + (p.iloc[-1]/p.iloc[-63]*2)
                adv_50 = (p * v).tail(50).mean()

                # 👉 [복구] AD (Accumulation/Distribution) 수급 로직 즉시 계산
                ad_raw = (v / v.rolling(50).mean() * p.pct_change() * 100).tail(65).sum()

                # 산업군 업데이트 (Unknown인 경우만)
                industry = base_df.loc[base_df['symbol'] == ticker, 'industry'].values[0]
                if industry == 'Unknown':
                    try:
                        info = yf.Ticker(ticker).info
                        industry = info.get('industry', 'Unknown')
                        desc = info.get('longBusinessSummary', '')
                        if industry != 'Unknown':
                            conn.execute("INSERT OR REPLACE INTO company_profiles (symbol, industry, description) VALUES (?, ?, ?)", (ticker, industry, desc))
                            conn.commit()
                    except: pass

                all_results.append({
                    'symbol': ticker, 'price': float(p.iloc[-1]), 'rs_raw': rs_raw,
                    'industry': industry, 'adv_50': adv_50, 'ad_raw': ad_raw
                })
            except: continue
        
        if i % 500 == 0 and i > 0: print(f" > {i}개 완료...")
        time.sleep(0.5)

    if not all_results: return
    
    # 1차 데이터프레임 조립 및 기본 랭킹
    df = pd.DataFrame(all_results)
    df['rs_score'] = (df['rs_raw'].rank(pct=True) * 99).astype(int)
    
    # 👉 [복구] AD 등급 A~E 산정
    df['ad_raw'] = df['ad_raw'].fillna(0)
    df['ad_grade'] = pd.qcut(df['ad_raw'].rank(method='first'), 5, labels=['E', 'D', 'C', 'B', 'A']).astype(str)

    print("🚀 2단계: SMR(재무) 등급 스마트 업데이트 진행 중...")
    
    # DB에서 기존 SMR 캐시 불러오기
    smr_db = pd.read_sql("SELECT * FROM smr_cache", conn)
    smr_db['last_updated'] = pd.to_datetime(smr_db['last_updated'])
    df = pd.merge(df, smr_db, on='symbol', how='left')

    # 👉 [핵심 로직] RS 70점 이상인 주도주 중, 재무 데이터가 없거나 90일이 지난 종목만 선별
    ninety_days_ago = datetime.now() - timedelta(days=90)
    needs_smr_update = df[
        (df['rs_score'] >= 70) & 
        ((df['smr_acc'].isnull()) | (df['last_updated'] < ninety_days_ago))
    ]['symbol'].tolist()

    if needs_smr_update:
        print(f" > {len(needs_smr_update)}개 주도주 재무 데이터 신규 수집 중 (야후 서버 우회 전략)...")
        for idx, ticker in enumerate(needs_smr_update):
            try:
                qf = yf.Ticker(ticker).quarterly_financials
                if not qf.empty and 'Total Revenue' in qf.index:
                    rev = qf.loc['Total Revenue'].dropna().values
                    net = qf.loc['Net Income'].dropna().values if 'Net Income' in qf.index else [0]
                    if len(rev) >= 3:
                        g0 = (rev[0] - rev[1]) / abs(rev[1]) if rev[1] != 0 else 0
                        g1 = (rev[1] - rev[2]) / abs(rev[2]) if rev[2] != 0 else 0
                        smr_acc = g0 - g1
                        is_prof = 1 if net[0] > 0 else 0
                        
                        # 계산된 값을 DB에 저장
                        today_str = datetime.now().strftime('%Y-%m-%d')
                        conn.execute("INSERT OR REPLACE INTO smr_cache (symbol, smr_acc, is_prof, last_updated) VALUES (?, ?, ?, ?)", 
                                     (ticker, smr_acc, is_prof, today_str))
                        conn.commit()
                        
                        # 현재 데이터프레임에도 반영
                        df.loc[df['symbol'] == ticker, ['smr_acc', 'is_prof']] = [smr_acc, is_prof]
            except: pass
            if idx % 50 == 0 and idx > 0: time.sleep(2) # 50개마다 휴식
            
    # SMR 등급 산정 (전체 종목 대상 상대평가)
    df['smr_acc'] = df['smr_acc'].fillna(0)
    df['is_prof'] = df['is_prof'].fillna(0)
    df['smr_val'] = df['smr_acc'].rank(pct=True) + (df['is_prof'] * 0.5)
    df['smr_grade'] = pd.qcut(df['smr_val'].rank(method='first'), 5, labels=['E', 'D', 'C', 'B', 'A']).astype(str)

    # 산업군 RS 계산
    ind_rs = df.groupby('industry')['rs_raw'].mean().reset_index(name='ind_rs_raw')
    ind_rs['industry_rs_score'] = (ind_rs['ind_rs_raw'].rank(pct=True) * 99).astype(int)
    final_df = pd.merge(df, ind_rs[['industry', 'industry_rs_score']], on='industry')

    # 최종 DB 저장
    save_cols = ['symbol', 'price', 'rs_score', 'smr_grade', 'ad_grade', 'industry_rs_score', 'industry', 'adv_50']
    final_df[save_cols].to_sql('repo_results', conn, if_exists='replace', index=False)
    
    conn.close()
    print(f"✅ 업데이트 완벽 종료! 대시보드를 확인하세요.")

if __name__ == "__main__":
    update_database()
