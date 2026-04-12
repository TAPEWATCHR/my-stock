import yfinance as yf
import pandas as pd
import sqlite3
import time
from datetime import datetime
import os
import requests
import json
import numpy as np

# --- [설정] FMP API 키 ---
FMP_API_KEY = os.environ.get('FMP_API_KEY', "1kJBflGjsp5fCgbancejhI5bN5iavEJF")

def get_official_ticker_list():
    """
    미국 증권거래위원회(SEC) 공식 JSON 데이터를 사용하여 
    미국 시장에 상장된 모든 종목 리스트를 가져옵니다. (가장 안정적)
    """
    try:
        print("📡 SEC(미 정부) 공식 서버에서 전 종목 리스트 로드 중...")
        # SEC 데이터는 반드시 User-Agent를 설정해야 접속을 허용합니다.
        headers = {
            'User-Agent': 'My-Stock-App contact@my-stock-app.com'
        }
        url = "https://www.sec.gov/files/company_tickers.json"
        response = requests.get(url, headers=headers, timeout=20)
        data = response.json()
        
        # JSON 데이터를 데이터프레임으로 변환
        ticker_list = []
        for key, val in data.items():
            ticker_list.append(val['ticker'].upper().replace('.', '-'))
            
        # 중복 제거 및 가비지 데이터 필터링
        ticker_list = list(set([t for t in ticker_list if t.isalpha() and len(t) <= 5]))
        
        print(f"✅ 성공: SEC 서버에서 {len(ticker_list)}개의 공식 티커를 확보했습니다.")
        return ticker_list
    except Exception as e:
        print(f"⚠️ SEC 서버 접속 실패: {e}")
        # 최후의 수단: 우리가 분석하고 싶은 주요 주도주 섹터 리스트 (이것도 꽤 많음)
        return ['AAPL', 'NVDA', 'MSFT', 'TSLA', 'GOOGL', 'AMZN', 'META', 'AVGO', 'COST', 'NFLX', 'AMD', 'SMCI', 'ARM']

def get_industry_fmp(ticker):
    """상위권 종목 상세 업종 정보 보강"""
    try:
        url = f"https://financialmodelingprep.com/api/v3/profile/{ticker}?apikey={FMP_API_KEY}"
        res = requests.get(url, timeout=5).json()
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
    # 1. SEC 소스에서 티커 가져오기
    tickers = get_official_ticker_list()
    
    all_results = []
    print(f"--- 1단계: {len(tickers)}개 종목 분석 시작 (yfinance) ---")
    
    # 작업 효율을 위해 50개씩 청크 처리
    chunk_size = 50
    for i in range(0, len(tickers), chunk_size):
        chunk = tickers[i:i + chunk_size]
        try:
            data = yf.download(chunk, period="1y", interval="1d", progress=False, group_by='ticker', threads=True)
            for ticker in chunk:
                try:
                    # 데이터 로드
                    if len(chunk) > 1:
                        if ticker not in data.columns.get_level_values(0): continue
                        hist = data[ticker].dropna()
                    else:
                        hist = data.dropna()

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
        if i % 500 == 0: print(f" > {i} / {len(tickers)} 분석 완료...")

    if not all_results:
        print("❌ 분석된 데이터가 없습니다.")
        return

    # 2. 결과 가공
    df = pd.DataFrame(all_results)
    df['rs_score'] = (df['rs_raw'].rank(pct=True) * 98 + 1).fillna(0).astype(int)
    # 타입 에러 방지를 위해 등급을 즉시 문자열로 변환
    df['ad_grade'] = pd.qcut(df['ad_raw'].rank(method='first'), 5, labels=['E', 'D', 'C', 'B', 'A']).astype(str)

    # 3. 상위 200개 종목 산업군 보강 (FMP 사용)
    print("--- 2단계: 주도주 산업군 정보 보강 ---")
    df['industry'] = 'Unknown'
    top_indices = df.sort_values('rs_score', ascending=False).head(200).index
    for idx in top_indices:
        df.at[idx, 'industry'] = get_industry_fmp(df.at[idx, 'symbol'])
        time.sleep(0.05)

    # 산업군 RS 점수
    ind_rs = df[df['industry'] != 'Unknown'].groupby('industry')['rs_raw'].mean().reset_index(name='ind_rs_raw')
    ind_rs['industry_rs_score'] = (ind_rs['ind_rs_raw'].rank(pct=True) * 98 + 1).fillna(0).astype(int)
    
    final_df = pd.merge(df, ind_rs[['industry', 'industry_rs_score']], on='industry', how='left')
    final_df['industry_rs_score'] = final_df['industry_rs_score'].fillna(0).astype(int)
    final_df['smr_grade'] = 'C'

    # 4. DB 저장
    conn = sqlite3.connect('ibd_system.db')
    save_cols = ['symbol', 'price', 'rs_score', 'smr_grade', 'ad_grade', 'industry_rs_score', 'industry', 'adv_50']
    final_df[save_cols].to_sql('repo_results', conn, if_exists='replace', index=False)
    
    history_df = final_df[['symbol', 'rs_score', 'industry_rs_score']].copy()
    history_df['date'] = datetime.now().strftime('%Y-%m-%d')
    history_df.to_sql('rs_history', conn, if_exists='append', index=False)
    conn.close()
    
    print(f"--- ✅ 전체 업데이트 성공 ({len(final_df)}개 종목) ---")

if __name__ == "__main__":
    update_database()
