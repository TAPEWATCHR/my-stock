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

FMP_API_KEY = os.environ.get("FMP_API_KEY", "").strip()

if not FMP_API_KEY:
    print("🚨 치명적 에러: FMP_API_KEY를 찾을 수 없습니다!")
    sys.exit(1)
else:
    print(f"🔑 사용 중인 API 키: {FMP_API_KEY[:4]}... (정상 로드됨)")

def init_db():
    conn = sqlite3.connect('ibd_system.db')
    cursor = conn.cursor()
    
    # SMR 캐시 테이블의 스키마 변경 (오리지널 4대 요소 적용을 위한 마이그레이션)
    cursor.execute("PRAGMA table_info(smr_cache)")
    columns = [info[1] for info in cursor.fetchall()]
    if 'sales_growth' not in columns:
        print("💡 SMR 알고리즘 업그레이드 감지. 기존 캐시를 초기화하고 새로운 스키마를 적용합니다.")
        conn.execute("DROP TABLE IF EXISTS smr_cache")
        
    conn.execute("CREATE TABLE IF NOT EXISTS company_profiles (symbol TEXT PRIMARY KEY, industry TEXT, description TEXT)")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS smr_cache (
            symbol TEXT PRIMARY KEY, 
            sales_growth REAL, pre_tax_margin REAL, 
            after_tax_margin REAL, roe REAL, 
            last_updated DATE
        )
    """)
    conn.execute("CREATE TABLE IF NOT EXISTS security_snapshot (date TEXT, symbol TEXT, company_name TEXT, industry TEXT, price REAL, volume REAL, adv_50 REAL, ad_grade TEXT, smr_grade TEXT, rs_score INTEGER, industry_rs_score INTEGER)")
    conn.execute("CREATE TABLE IF NOT EXISTS rs_history (symbol TEXT, date TEXT, rs_score INTEGER)")
    conn.close()

def get_pure_exchange_stocks():
    try:
        stocks = []
        for exch in ['NASDAQ', 'NYSE']:
            url = f"https://financialmodelingprep.com/stable/company-screener?exchange={exch}&isEtf=false&isFund=false&isActivelyTrading=true&limit=10000&apikey={FMP_API_KEY}"
            res = requests.get(url, timeout=15)
            res.raise_for_status()
            data = res.json()
            if data:
                stocks.append(pd.DataFrame(data))
        
        if not stocks: return pd.DataFrame()
            
        df = pd.concat(stocks, ignore_index=True)
        df['symbol'] = df['symbol'].str.upper().str.replace('.', '-')
        
        name_col = 'companyName' if 'companyName' in df.columns else 'name'
        ind_col = 'industry' if 'industry' in df.columns else 'sector'
        
        result_df = df[['symbol', name_col, ind_col]].rename(columns={name_col: 'name', ind_col: 'industry'})
        result_df['industry'] = result_df['industry'].fillna('Unknown')
        return result_df.copy()
        
    except Exception as e:
        print(f"🚨 FMP 스크리너 API 로드 실패: {e}")
        return pd.DataFrame()

def update_database():
    init_db()
    base_df = get_pure_exchange_stocks()
    if base_df.empty: return

    conn = sqlite3.connect('ibd_system.db')
    
    for _, row in base_df.iterrows():
        conn.execute("INSERT OR IGNORE INTO company_profiles (symbol, industry, description) VALUES (?, ?, ?)", (row['symbol'], row['industry'], ''))
        conn.execute("UPDATE company_profiles SET industry = ? WHERE symbol = ? AND (industry = 'Unknown' OR industry IS NULL)", (row['industry'], row['symbol']))
    conn.commit()

    master_profiles = pd.read_sql("SELECT symbol, industry FROM company_profiles", conn)
    base_df = base_df.drop(columns=['industry']).merge(master_profiles, on='symbol', how='left')
    base_df['industry'] = base_df['industry'].fillna('Unknown')

    tickers = base_df['symbol'].tolist()
    all_results = []
    snapshot_rows = []
    
    print(f"🚀 1단계: 전체 {len(tickers)}개 종목 가격 추세 및 AD 수급 분석 시작...")
    
    chunk_size = 100
    for i in range(0, len(tickers), chunk_size):
        chunk = tickers[i:i + chunk_size]
        try:
            price_data = yf.download(chunk, period="1y", interval="1d", progress=False, group_by='ticker')
        except:
            time.sleep(2); continue

        for ticker in chunk:
            try:
                hist = price_data.copy() if len(chunk) == 1 else price_data[ticker].copy() if ticker in price_data.columns.levels[0] else pd.DataFrame()
                hist = hist.dropna(subset=['Close', 'Volume'])
                
                # 최소 65거래일(1개 분기) 이상 상장된 기업만 취급
                if len(hist) < 65: continue

                p = hist['Close']
                v = hist['Volume']
                
                c_0 = float(p.iloc[-1])
                adv_50 = float((v.tail(50).mean() * c_0))
                
                # [RS 로직] 12개월(252일) 가중 평균 수익률 (40% / 20% / 20% / 20%)
                c_63 = float(p.iloc[-63]) if len(p) >= 63 else c_0
                c_126 = float(p.iloc[-126]) if len(p) >= 126 else c_63
                c_189 = float(p.iloc[-189]) if len(p) >= 189 else c_126
                c_252 = float(p.iloc[-252]) if len(p) >= 252 else c_189

                ret_3m = (c_0 - c_63) / c_63 if c_63 != 0 else 0
                ret_6m = (c_0 - c_126) / c_126 if c_126 != 0 else 0
                ret_9m = (c_0 - c_189) / c_189 if c_189 != 0 else 0
                ret_12m = (c_0 - c_252) / c_252 if c_252 != 0 else 0
                
                rs_raw = (0.4 * ret_3m) + (0.2 * ret_6m) + (0.2 * ret_9m) + (0.2 * ret_12m)

                # [AD 로직] 지난 13주(65일) 거래량 가중 주가 모멘텀 누적 (Money Flow)
                p_65 = p.tail(65)
                v_65 = v.tail(65)
                pct_change_65 = p_65.pct_change().fillna(0)
                ad_raw = (pct_change_65 * v_65).sum() / v_65.sum() if v_65.sum() > 0 else 0

                industry = base_df.loc[base_df['symbol'] == ticker, 'industry'].values[0]

                all_results.append({'symbol': ticker, 'price': c_0, 'rs_raw': rs_raw, 'industry': industry, 'adv_50': adv_50, 'ad_raw': ad_raw})
                snapshot_rows.append({'date': datetime.now().strftime('%Y-%m-%d'), 'symbol': ticker, 'company_name': base_df.loc[base_df['symbol'] == ticker, 'name'].values[0], 'industry': industry, 'price': c_0, 'volume': float(v.iloc[-1]), 'adv_50': adv_50})
            except: continue
        
        if i % 500 == 0 and i > 0: print(f" > {i}개 완료...")
        time.sleep(0.5)

    if not all_results: return
    
    df = pd.DataFrame(all_results)
    df['rs_score'] = (df['rs_raw'].rank(pct=True) * 99).astype(int)
    df['ad_raw'] = df['ad_raw'].fillna(0)
    df['ad_grade'] = pd.qcut(df['ad_raw'].rank(method='first'), 5, labels=['E', 'D', 'C', 'B', 'A']).astype(str)

    print("🚀 2단계: FMP 유료 API 기반 SMR(재무) 4대 요소 정밀 수집 진행 중...")
    
    smr_db = pd.read_sql("SELECT * FROM smr_cache", conn)
    smr_db['last_updated'] = pd.to_datetime(smr_db['last_updated'])
    df = pd.merge(df, smr_db, on='symbol', how='left')

    ninety_days_ago = datetime.now() - timedelta(days=90)
    needs_smr_update = df[((df['sales_growth'].isnull()) | (df['last_updated'] < ninety_days_ago))]['symbol'].tolist()

    if needs_smr_update:
        print(f" > {len(needs_smr_update)}개 종목 재무 데이터 갱신 중 (IS 및 BS 동시 호출)...")
        for idx, ticker in enumerate(needs_smr_update):
            try:
                # 손익계산서(최대 5분기: YoY 계산용)와 대차대조표(자본 확인용) 호출
                url_is = f"https://financialmodelingprep.com/stable/income-statement?symbol={ticker}&period=quarter&limit=5&apikey={FMP_API_KEY}"
                url_bs = f"https://financialmodelingprep.com/stable/balance-sheet-statement?symbol={ticker}&period=quarter&limit=1&apikey={FMP_API_KEY}"
                
                res_is = requests.get(url_is, timeout=5)
                res_bs = requests.get(url_bs, timeout=5)
                
                if res_is.status_code == 200 and res_bs.status_code == 200:
                    qf = res_is.json()
                    bf = res_bs.json()
                    
                    if len(qf) >= 1 and len(bf) >= 1:
                        # 1. 매출 성장률 (최근 분기의 전년 동기 대비 성장률, 데이터 부족 시 0)
                        rev_0 = qf[0].get('revenue') or 0
                        rev_4 = qf[4].get('revenue') or 0 if len(qf) == 5 else (qf[-1].get('revenue') or 0)
                        sales_growth = (rev_0 - rev_4) / abs(rev_4) if rev_4 != 0 else 0
                        
                        # 2. 세전 이익률 (Pre-tax Margin)
                        inc_tax = qf[0].get('incomeBeforeTax') or 0
                        pre_tax_margin = inc_tax / rev_0 if rev_0 != 0 else 0
                        
                        # 3. 세후 이익률 (After-tax Margin)
                        net_inc = qf[0].get('netIncome') or 0
                        after_tax_margin = net_inc / rev_0 if rev_0 != 0 else 0
                        
                        # 4. ROE (최근 4개 분기 순이익 합산 / 자본)
                        ttm_net = sum([(item.get('netIncome') or 0) for item in qf[:4]])
                        equity = bf[0].get('totalStockholdersEquity') or 0
                        roe = ttm_net / equity if equity != 0 else 0
                        
                        conn.execute("""
                            INSERT OR REPLACE INTO smr_cache 
                            (symbol, sales_growth, pre_tax_margin, after_tax_margin, roe, last_updated) 
                            VALUES (?, ?, ?, ?, ?, ?)
                        """, (ticker, sales_growth, pre_tax_margin, after_tax_margin, roe, datetime.now().strftime('%Y-%m-%d')))
                        conn.commit()
                        
                        df.loc[df['symbol'] == ticker, ['sales_growth', 'pre_tax_margin', 'after_tax_margin', 'roe']] = [sales_growth, pre_tax_margin, after_tax_margin, roe]
            except: pass
            
            # API 한도 방어 (분당 300회 제한) - 2번 호출하므로 0.4초 대기
            time.sleep(0.4) 
            if idx % 100 == 0 and idx > 0: print(f"   ... {idx}개 재무 데이터 완료")
            
    # 누락된 데이터 0 처리
    for col in ['sales_growth', 'pre_tax_margin', 'after_tax_margin', 'roe']:
        df[col] = df[col].fillna(0)
    
    # [SMR 로직] 4가지 요소별 백분위 랭킹 산출 후 합산하여 최종 SMR 등급 부여
    df['rank_sg'] = df['sales_growth'].rank(pct=True)
    df['rank_ptm'] = df['pre_tax_margin'].rank(pct=True)
    df['rank_atm'] = df['after_tax_margin'].rank(pct=True)
    df['rank_roe'] = df['roe'].rank(pct=True)
    
    df['smr_val'] = df['rank_sg'] + df['rank_ptm'] + df['rank_atm'] + df['rank_roe']
    df['smr_grade'] = pd.qcut(df['smr_val'].rank(method='first'), 5, labels=['E', 'D', 'C', 'B', 'A']).astype(str)

    ind_rs = df.groupby('industry')['rs_raw'].mean().reset_index(name='ind_rs_raw')
    ind_rs['industry_rs_score'] = (ind_rs['ind_rs_raw'].rank(pct=True) * 99).astype(int)
    final_df = pd.merge(df, ind_rs[['industry', 'industry_rs_score']], on='industry')

    save_cols = ['symbol', 'price', 'rs_score', 'smr_grade', 'ad_grade', 'industry_rs_score', 'industry', 'adv_50']
    final_df[save_cols].to_sql('repo_results', conn, if_exists='replace', index=False)

    rs_history_df = final_df[['symbol', 'rs_score']].copy()
    rs_history_df['date'] = datetime.now().strftime('%Y-%m-%d')
    rs_history_df[['symbol', 'date', 'rs_score']].to_sql('rs_history', conn, if_exists='append', index=False)

    if snapshot_rows:
        snapshot_df = pd.DataFrame(snapshot_rows).merge(final_df[['symbol', 'ad_grade', 'smr_grade', 'rs_score', 'industry_rs_score']], on='symbol', how='left')
        snapshot_df.to_sql('security_snapshot', conn, if_exists='replace', index=False)
    
    conn.close()
    print(f"✅ 업데이트 완료! 총 {len(final_df)}개 주식 저장 완료.")

if __name__ == "__main__":
    update_database()
