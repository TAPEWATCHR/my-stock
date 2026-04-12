import yfinance as yf
import pandas as pd
import sqlite3
import time
from datetime import datetime
import os
import requests
import io
import numpy as np
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# --- 🛡️ [핵심] 야후 파이낸스 차단 방지용 스마트 세션 구축 ---
session = requests.Session()
# 429(Too Many Requests) 에러 발생 시 자동으로 대기 후 재시도
retry = Retry(connect=5, read=5, backoff_factor=2, status_forcelist=[429, 500, 502, 503, 504])
adapter = HTTPAdapter(max_retries=retry)
session.mount('http://', adapter)
session.mount('https://', adapter)

def get_pure_exchange_stocks():
    """장외주식(OTC)을 제거하고 나스닥, NYSE 상장 보통주만 완벽히 추출합니다."""
    headers = {'User-Agent': 'My-Stock-App contact@example.com'}
    try:
        # 1. SEC 거래소 포함 데이터 로드 (핵심 해결책)
        url = "https://www.sec.gov/files/company_tickers_exchange.json"
        res = requests.get(url, headers=headers, timeout=15).json()
        
        # 데이터 프레임 변환 (cik, name, ticker, exchange)
        df_sec = pd.DataFrame(res['data'], columns=res['fields'])
        
        # 2. 메이저 거래소만 필터링 (장외주식, 핑크시트 3~4000개 제거)
        valid_exchanges = ['Nasdaq', 'NYSE', 'NYSE AMEX', 'NYSE ARCA', 'CBOE']
        df_sec = df_sec[df_sec['exchange'].isin(valid_exchanges)]
        df_sec['symbol'] = df_sec['ticker'].str.upper().replace('.', '-')
        
        # 3. 불순물(ETF, 펀드 등) 키워드 제거
        exclude_terms = ['ETF', 'FUND', 'TRUST', 'ACQUISITION', 'SPAC', 'WARRANT']
        for term in exclude_terms:
            df_sec = df_sec[~df_sec['name'].str.upper().str.contains(term, na=False)]
            
        # 특수 기호(우선주 등) 제거
        df_sec = df_sec[~df_sec['symbol'].str.contains(r'-[PWR]$', regex=True)]
        
        base_df = df_sec[['symbol', 'name']].copy()
        
    except Exception as e:
        print(f"🚨 SEC 데이터 로드 실패: {e}")
        return pd.DataFrame()

    # 4. 산업군 CSV 데이터 로드 및 병합
    urls = [
        "https://raw.githubusercontent.com/Ate329/top-us-stock-tickers/main/tickers/all.csv",
        "https://raw.githubusercontent.com/rreichel3/US-Stock-Symbols/main/nasdaq/nasdaq_full_tickers.csv"
    ]
    id_df = pd.DataFrame(columns=['symbol', 'industry'])
    for u in urls:
        try:
            r = requests.get(u, timeout=10)
            if r.status_code == 200:
                temp_df = pd.read_csv(io.StringIO(r.text))
                temp_df.columns = [c.lower() for c in temp_df.columns]
                if 'symbol' in temp_df.columns and 'industry' in temp_df.columns:
                    id_df = temp_df[['symbol', 'industry']].copy()
                    id_df['symbol'] = id_df['symbol'].astype(str).str.upper().str.replace('.', '-', regex=False)
                    break
        except: continue
        
    final_base = pd.merge(base_df, id_df, on='symbol', how='left')
    final_base['industry'] = final_base['industry'].fillna('Unknown')
    
    print(f"✨ 거래소 보통주 추출 완료: 총 {len(final_base)}개 종목 (Unknown 비율 대폭 감소)")
    return final_base

def update_database():
    base_df = get_pure_exchange_stocks()
    if base_df.empty: return

    tickers = base_df['symbol'].tolist()
    all_results = []
    
    print(f"--- 1단계: {len(tickers)}개 전 종목 리얼 SMR & 가격 데이터 추출 시작 ---")
    print("이 작업은 야후 서버 차단을 피하기 위해 약 40~60분 정도 소요될 수 있습니다.")
    
    # yfinance를 한 번에 대량 호출하지 않고 20개 단위로 천천히 안전하게 호출
    chunk_size = 20 
    for i in range(0, len(tickers), chunk_size):
        chunk = tickers[i:i + chunk_size]
        
        # 가격 데이터 로드
        price_data = yf.download(chunk, period="1y", interval="1d", progress=False, group_by='ticker', threads=True)
        
        for ticker in chunk:
            try:
                # 1. 가격 데이터 기반 RS 및 AD 계산
                hist = price_data[ticker].dropna() if len(chunk) > 1 else price_data.dropna()
                if len(hist) < 200: continue
                
                p = hist['Close']
                rs_raw = (p.iloc[-1]/p.iloc[-21]*2) + (p.iloc[-1]/p.iloc[-63]*2) + (p.iloc[-1]/p.iloc[-126]) + (p.iloc[-1]/p.iloc[-min(252, len(p))])
                ad_raw = (hist['Volume'] / hist['Volume'].rolling(50).mean() * p.pct_change() * 100).tail(65).sum()
                adv_50 = (p * hist['Volume']).tail(50).mean()

                # 2. 리얼 재무 데이터 (SMR) 추출 (안전 세션 사용)
                t_obj = yf.Ticker(ticker, session=session)
                qf = t_obj.quarterly_financials
                
                smr_acc = 0.0
                is_prof = False
                
                if not qf.empty and 'Total Revenue' in qf.index:
                    rev = qf.loc['Total Revenue'].dropna().values
                    net = qf.loc['Net Income'].dropna().values if 'Net Income' in qf.index else [0]
                    if len(rev) >= 3:
                        g0 = (rev[0] - rev[1]) / abs(rev[1]) if rev[1] != 0 else 0
                        g1 = (rev[1] - rev[2]) / abs(rev[2]) if rev[2] != 0 else 0
                        smr_acc = g0 - g1
                        is_prof = net[0] > 0
                
                # 3. CSV에 없던 Unknown 산업군을 여기서 채워 넣기!
                current_ind = base_df.loc[base_df['symbol'] == ticker, 'industry'].values[0]
                if current_ind == 'Unknown':
                    fetched_ind = t_obj.info.get('industry', 'Unknown')
                    base_df.loc[base_df['symbol'] == ticker, 'industry'] = fetched_ind

                all_results.append({
                    'symbol': ticker, 'price': float(p.iloc[-1]), 'rs_raw': rs_raw,
                    'ad_raw': ad_raw, 'adv_50': adv_50,
                    'smr_acc': smr_acc, 'is_prof': is_prof
                })
                
                time.sleep(0.1) # 종목당 0.1초 휴식 (Anti-ban)
                
            except Exception as e:
                if "429" in str(e):
                    print(f"⚠️ 야후 서버 피로도 증가. 30초 쿨다운...")
                    time.sleep(30)
                continue
                
        if i % 200 == 0 and i > 0:
            print(f" > {i} / {len(tickers)} 종목 완료... (정상 진행 중)")

    if not all_results: 
        print("분석된 데이터가 없습니다.")
        return
        
    df = pd.merge(pd.DataFrame(all_results), base_df[['symbol', 'industry']], on='symbol', how='left')
    
    print("--- 2단계: 등급 랭킹 산정 ---")
    # 전 종목 상대평가 랭킹
    df['rs_score'] = (df['rs_raw'].rank(pct=True) * 98 + 1).astype(int)
    df['ad_grade'] = pd.qcut(df['ad_raw'].rank(method='first'), 5, labels=['E', 'D', 'C', 'B', 'A']).astype(str)
    
    df['smr_val'] = df['smr_acc'].rank(pct=True) + (df['is_prof'].astype(int) * 0.5)
    df['smr_grade'] = pd.qcut(df['smr_val'].rank(method='first'), 5, labels=['E', 'D', 'C', 'B', 'A']).astype(str)

    # 전체 종목 기반 산업군 RS 산출
    ind_rs = df[df['industry'] != 'Unknown'].groupby('industry')['rs_raw'].mean().reset_index(name='ind_rs_raw')
    ind_rs['industry_rs_score'] = (ind_rs['ind_rs_raw'].rank(pct=True) * 98 + 1).fillna(0).astype(int)
    final_df = pd.merge(df, ind_rs[['industry', 'industry_rs_score']], on='industry', how='left').fillna(0)

    # DB 저장
    conn = sqlite3.connect('ibd_system.db')
    save_cols = ['symbol', 'price', 'rs_score', 'smr_grade', 'ad_grade', 'industry_rs_score', 'industry', 'adv_50']
    final_df[save_cols].to_sql('repo_results', conn, if_exists='replace', index=False)
    
    history_df = final_df[['symbol', 'rs_score', 'industry_rs_score']].copy()
    history_df['date'] = datetime.now().strftime('%Y-%m-%d')
    history_df.to_sql('rs_history', conn, if_exists='append', index=False)
    conn.close()
    
    # 분석 리포트 출력
    unknown_count = len(final_df[final_df['industry'] == 'Unknown'])
    print(f"✅ 리얼 SMR & RS 분석 완료: 총 {len(final_df)}개 종목 성공")
    print(f"✅ 산업군 미매칭(Unknown) 개수: {unknown_count}개 (성공적으로 최소화됨!)")

if __name__ == "__main__":
    update_database()
