# -*- coding: utf-8 -*-
import yfinance as yf
import pandas as pd
import sqlite3
import time
import numpy as np
from datetime import datetime, timedelta
import requests

def init_db():
    conn = sqlite3.connect('ibd_system.db')
    conn.execute("""
        CREATE TABLE IF NOT EXISTS company_profiles (
            symbol TEXT PRIMARY KEY, industry TEXT, description TEXT
        )
    """)
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
    headers = {'User-Agent': 'Mozilla/5.0'} 
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

    # yfinance 다운로드 방식 개선 (개별 데이터 보존)
    chunk_size = 100
    for i in range(0, len(tickers), chunk_size):
        chunk = tickers[i:i + chunk_size]
        try:
            # group_by='ticker'를 유지하되, 통째로 dropna() 하지 않음
            price_data = yf.download(chunk, period="1y", interval="1d", progress=False, group_by='ticker')
        except:
            time.sleep(2); continue

        for ticker in chunk:
            try:
                # 단일 종목일 때와 다중 종목일 때의 컬럼 구조 대응
                if len(chunk) == 1:
                    hist = price_data.copy()
                else:
                    if ticker not in price_data.columns.levels[0]: continue
                    hist = price_data[ticker].copy()
                
                hist = hist.dropna(subset=['Close', 'Volume'])
                
                # 최소 데이터 기준 완화 (신규 상장주 포함을 위해 65일로 축소)
                if len(hist) < 65: continue

                p = hist['Close']
                v = hist['Volume']
                
                # RS 계산 (최근 1분기 및 1달 가중치)
                current_p = float(p.iloc[-1])
                rs_raw = (current_p / p.iloc[-21] * 2) + (current_p / p.iloc[-63] * 2) if len(hist) >= 63 else 0
                
                # 거래대금(ADV) 계산 (최근 50일 평균 거래량 * 현재가, 달러 단위)
                adv_50 = float((v.tail(50).mean() * current_p))

                # AD 수급 로직 개선 (ZeroDivisionError 방지 및 추세 반영)
                v_mean = v.rolling(50).mean().bfill()
                pct_change = p.pct_change().fillna(0) * 100
                ad_raw = np.where(v_mean > 0, (v / v_mean) * pct_change, 0)
                ad_raw_sum = pd.Series(ad_raw).tail(65).sum()

                # 산업군 업데이트 (알 수 없는 경우)
                industry = base_df.loc[base_df['symbol'] == ticker, 'industry'].values[0]

                all_results.append({
                    'symbol': ticker, 'price': current_p, 'rs_raw': rs_raw,
                    'industry': industry, 'adv_50': adv_50, 'ad_raw': ad_raw_sum
                })
            except Exception as e:
                continue
        
        if i % 500 == 0 and i > 0: print(f" > {i}개 완료...")
        time.sleep(0.5)

    if not all_results: 
        print("🚨 수집된 데이터가 없습니다.")
        return
    
    df = pd.DataFrame(all_results)
    df['rs_score'] = (df['rs_raw'].rank(pct=True) * 99).astype(int)
    
    # AD 등급 산정 (중복값 에러 방지)
    df['ad_raw'] = df['ad_raw'].fillna(0)
    df['ad_grade'] = pd.qcut(df['ad_raw'].rank(method='first'), 5, labels=['E', 'D', 'C', 'B', 'A']).astype(str)

    print("🚀 2단계: SMR(재무) 등급 스마트 업데이트 진행 중...")
    
    smr_db = pd.read_sql("SELECT * FROM smr_cache", conn)
    smr_db['last_updated'] = pd.to_datetime(smr_db['last_updated'])
    df = pd.merge(df, smr_db, on='symbol', how='left')

    ninety_days_ago = datetime.now() - timedelta(days=90)
    # RS 70 이상 주도주이거나 기존에 데이터가 없는 종목 중 90일 지난 종목만 업데이트
    needs_smr_update = df[
        (df['rs_score'] >= 70) & 
        ((df['smr_acc'].isnull()) | (df['last_updated'] < ninety_days_ago))
    ]['symbol'].tolist()

    if needs_smr_update:
        print(f" > {len(needs_smr_update)}개 종목 재무 데이터 업데이트 중...")
        for idx, ticker in enumerate(needs_smr_update):
            try:
                tk = yf.Ticker(ticker)
                qf = tk.quarterly_financials
                
                # 다양한 야후 파이낸스 키값 대응
                rev_key = 'Total Revenue' if 'Total Revenue' in qf.index else 'Operating Revenue' if 'Operating Revenue' in qf.index else None
                net_key = 'Net Income' if 'Net Income' in qf.index else None

                if rev_key and not qf.empty:
                    rev = qf.loc[rev_key].dropna().values
                    net = qf.loc[net_key].dropna().values if net_key else [0]
                    
                    if len(rev) >= 3:
                        g0 = (rev[0] - rev[1]) / abs(rev[1]) if rev[1] != 0 else 0
                        g1 = (rev[1] - rev[2]) / abs(rev[2]) if rev[2] != 0 else 0
                        smr_acc = g0 - g1
                        is_prof = 1 if len(net) > 0 and net[0] > 0 else 0
                        
                        today_str = datetime.now().strftime('%Y-%m-%d')
                        conn.execute("INSERT OR REPLACE INTO smr_cache (symbol, smr_acc, is_prof, last_updated) VALUES (?, ?, ?, ?)", 
                                     (ticker, smr_acc, is_prof, today_str))
                        conn.commit()
                        df.loc[df['symbol'] == ticker, ['smr_acc', 'is_prof']] = [smr_acc, is_prof]
            except: pass
            if idx % 50 == 0 and idx > 0: time.sleep(1) 
            
    df['smr_acc'] = df['smr_acc'].fillna(0)
    df['is_prof'] = df['is_prof'].fillna(0)
    df['smr_val'] = df['smr_acc'].rank(pct=True) + (df['is_prof'] * 0.5)
    df['smr_grade'] = pd.qcut(df['smr_val'].rank(method='first'), 5, labels=['E', 'D', 'C', 'B', 'A']).astype(str)

    ind_rs = df.groupby('industry')['rs_raw'].mean().reset_index(name='ind_rs_raw')
    ind_rs['industry_rs_score'] = (ind_rs['ind_rs_raw'].rank(pct=True) * 99).astype(int)
    final_df = pd.merge(df, ind_rs[['industry', 'industry_rs_score']], on='industry')

    save_cols = ['symbol', 'price', 'rs_score', 'smr_grade', 'ad_grade', 'industry_rs_score', 'industry', 'adv_50']
    final_df[save_cols].to_sql('repo_results', conn, if_exists='replace', index=False)
    
    conn.close()
    print(f"✅ 업데이트 완료! 총 {len(final_df)}개 종목 저장됨.")

if __name__ == "__main__":
    update_database()
