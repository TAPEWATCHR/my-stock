# -*- coding: utf-8 -*-
import yfinance as yf
import pandas as pd
import sqlite3
import time
import numpy as np
from datetime import datetime, timedelta
import requests
import os
import sys

# GitHub Secrets에서 키를 가져옵니다.
FMP_API_KEY = os.environ.get("FMP_API_KEY", "").strip()

if not FMP_API_KEY:
    print("🚨 치명적 에러: FMP_API_KEY를 찾을 수 없습니다!")
    sys.exit(1)
else:
    print(f"🔑 사용 중인 API 키: {FMP_API_KEY[:4]}... (정상 로드됨)")

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
    conn.execute("""
        CREATE TABLE IF NOT EXISTS security_snapshot (
            date TEXT, symbol TEXT, company_name TEXT, industry TEXT,
            price REAL, volume REAL, adv_50 REAL, ad_grade TEXT,
            smr_grade TEXT, rs_score INTEGER, industry_rs_score INTEGER
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS rs_history (
            symbol TEXT, date TEXT, rs_score INTEGER
        )
    """)
    conn.close()

def get_pure_exchange_stocks():
    """FMP 최신 Stable Screener API를 사용하여 나스닥/NYSE 일반 주식만 정확히 가져옵니다."""
    try:
        stocks = []
        # 나스닥과 뉴욕거래소를 각각 호출하여 ETF/Fund를 서버단에서 완벽히 제외합니다.
        for exch in ['NASDAQ', 'NYSE']:
            url = f"https://financialmodelingprep.com/stable/company-screener?exchange={exch}&isEtf=false&isFund=false&isActivelyTrading=true&limit=10000&apikey={FMP_API_KEY}"
            res = requests.get(url, timeout=15)
            res.raise_for_status()
            data = res.json()
            if data:
                stocks.append(pd.DataFrame(data))
        
        if not stocks:
            print("🚨 스크리너 데이터가 비어있습니다. API 키나 네트워크를 확인하세요.")
            return pd.DataFrame()
            
        df = pd.concat(stocks, ignore_index=True)
        df['symbol'] = df['symbol'].str.upper().str.replace('.', '-')
        
        name_col = 'companyName' if 'companyName' in df.columns else 'name'
        result_df = df[['symbol', name_col]].rename(columns={name_col: 'name'})
        
        return result_df.copy()
        
    except Exception as e:
        print(f"🚨 FMP 스크리너 API 로드 실패: {e}")
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
    snapshot_rows = []
    
    print(f"🚀 1단계: 전체 {len(tickers)}개 종목 가격 및 AD 수급 분석 시작...")
    
    chunk_size = 100
    for i in range(0, len(tickers), chunk_size):
        chunk = tickers[i:i + chunk_size]
        try:
            price_data = yf.download(chunk, period="1y", interval="1d", progress=False, group_by='ticker')
        except:
            time.sleep(2); continue

        for ticker in chunk:
            try:
                if len(chunk) == 1:
                    hist = price_data.copy()
                else:
                    if ticker not in price_data.columns.levels[0]: continue
                    hist = price_data[ticker].copy()
                
                hist = hist.dropna(subset=['Close', 'Volume'])
                if len(hist) < 65: continue

                p = hist['Close']
                v = hist['Volume']
                
                current_p = float(p.iloc[-1])
                rs_raw = (current_p / p.iloc[-21] * 2) + (current_p / p.iloc[-63] * 2) if len(hist) >= 63 else 0
                adv_50 = float((v.tail(50).mean() * current_p))

                v_mean = v.rolling(50).mean().bfill()
                pct_change = p.pct_change().fillna(0) * 100
                ad_raw = np.where(v_mean > 0, (v / v_mean) * pct_change, 0)
                ad_raw_sum = pd.Series(ad_raw).tail(65).sum()

                industry = base_df.loc[base_df['symbol'] == ticker, 'industry'].values[0]

                all_results.append({
                    'symbol': ticker, 'price': current_p, 'rs_raw': rs_raw,
                    'industry': industry, 'adv_50': adv_50, 'ad_raw': ad_raw_sum
                })
                snapshot_rows.append({
                    'date': datetime.now().strftime('%Y-%m-%d'),
                    'symbol': ticker,
                    'company_name': base_df.loc[base_df['symbol'] == ticker, 'name'].values[0],
                    'industry': industry,
                    'price': current_p,
                    'volume': float(v.iloc[-1]),
                    'adv_50': adv_50
                })
            except Exception as e:
                continue
        
        if i % 500 == 0 and i > 0: print(f" > {i}개 종목 가격 분석 완료...")
        time.sleep(0.5)

    if not all_results: 
        print("🚨 수집된 데이터가 없습니다.")
        return
    
    df = pd.DataFrame(all_results)
    df['rs_score'] = (df['rs_raw'].rank(pct=True) * 99).astype(int)
    
    df['ad_raw'] = df['ad_raw'].fillna(0)
    df['ad_grade'] = pd.qcut(df['ad_raw'].rank(method='first'), 5, labels=['E', 'D', 'C', 'B', 'A']).astype(str)

    print("🚀 2단계: FMP 유료 API 기반 SMR(재무) 등급 업데이트 진행 중...")
    
    smr_db = pd.read_sql("SELECT * FROM smr_cache", conn)
    smr_db['last_updated'] = pd.to_datetime(smr_db['last_updated'])
    df = pd.merge(df, smr_db, on='symbol', how='left')

    ninety_days_ago = datetime.now() - timedelta(days=90)
    needs_smr_update = df[
        (df['rs_score'] >= 70) & 
        ((df['smr_acc'].isnull()) | (df['last_updated'] < ninety_days_ago))
    ]['symbol'].tolist()

    if needs_smr_update:
        print(f" > {len(needs_smr_update)}개 주도주 재무 데이터 갱신 중...")
        for idx, ticker in enumerate(needs_smr_update):
            try:
                url = f"https://financialmodelingprep.com/stable/income-statement?symbol={ticker}&period=quarter&limit=4&apikey={FMP_API_KEY}"
                res = requests.get(url, timeout=5)
                
                if res.status_code == 200:
                    qf = res.json()
                    if len(qf) >= 3:
                        rev = [item.get('revenue', 0) for item in qf]
                        net = [item.get('netIncome', 0) for item in qf]
                        
                        g0 = (rev[0] - rev[1]) / abs(rev[1]) if rev[1] != 0 else 0
                        g1 = (rev[1] - rev[2]) / abs(rev[2]) if rev[2] != 0 else 0
                        smr_acc = g0 - g1
                        is_prof = 1 if len(net) > 0 and net[0] > 0 else 0
                        
                        today_str = datetime.now().strftime('%Y-%m-%d')
                        conn.execute("INSERT OR REPLACE INTO smr_cache (symbol, smr_acc, is_prof, last_updated) VALUES (?, ?, ?, ?)", 
                                     (ticker, smr_acc, is_prof, today_str))
                        conn.commit()
                        df.loc[df['symbol'] == ticker, ['smr_acc', 'is_prof']] = [smr_acc, is_prof]
            except Exception as e:
                pass
            
            time.sleep(0.2)
            if idx % 100 == 0 and idx > 0: print(f"   ... {idx}개 재무 데이터 완료")
            
    df['smr_acc'] = df['smr_acc'].fillna(0)
    df['is_prof'] = df['is_prof'].fillna(0)
    df['smr_val'] = df['smr_acc'].rank(pct=True) + (df['is_prof'] * 0.5)
    df['smr_grade'] = pd.qcut(df['smr_val'].rank(method='first'), 5, labels=['E', 'D', 'C', 'B', 'A']).astype(str)

    ind_rs = df.groupby('industry')['rs_raw'].mean().reset_index(name='ind_rs_raw')
    ind_rs['industry_rs_score'] = (ind_rs['ind_rs_raw'].rank(pct=True) * 99).astype(int)
    final_df = pd.merge(df, ind_rs[['industry', 'industry_rs_score']], on='industry')

    save_cols = ['symbol', 'price', 'rs_score', 'smr_grade', 'ad_grade', 'industry_rs_score', 'industry', 'adv_50']
    final_df[save_cols].to_sql('repo_results', conn, if_exists='replace', index=False)

    today_str = datetime.now().strftime('%Y-%m-%d')
    rs_history_df = final_df[['symbol', 'rs_score']].copy()
    rs_history_df['date'] = today_str
    rs_history_df[['symbol', 'date', 'rs_score']].to_sql('rs_history', conn, if_exists='append', index=False)

    if snapshot_rows:
        snapshot_df = pd.DataFrame(snapshot_rows)
        snapshot_df = snapshot_df.merge(
            final_df[['symbol', 'ad_grade', 'smr_grade', 'rs_score', 'industry_rs_score']],
            on='symbol',
            how='left'
        )
        snapshot_df.to_sql('security_snapshot', conn, if_exists='replace', index=False)
    
    conn.close()
    print(f"✅ 업데이트 완료! 총 {len(final_df)}개 주식 저장 완료.")

if __name__ == "__main__":
    update_database()
