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

def get_official_nasdaq_tickers():
    """나스닥 서버 접속 시도 및 실패 시 백업 리스트 반환"""
    try:
        print("📡 나스닥 공식 서버에서 종목 리스트 로드 중...")
        # 타임아웃을 20초로 늘려 안정성 확보
        url = "http://ftp.nasdaqtrader.com/SymbolDirectory/nasdaqlisted.txt"
        res = requests.get(url, timeout=20).text
        df_nasdaq = pd.read_csv(io.StringIO(res), sep="|")
        
        url_other = "http://ftp.nasdaqtrader.com/SymbolDirectory/otherlisted.txt"
        res_other = requests.get(url_other, timeout=20).text
        df_other = pd.read_csv(io.StringIO(res_other), sep="|")
        
        tickers = list(set(df_nasdaq['Symbol'].dropna().tolist() + df_other['NASDAQ Symbol'].dropna().tolist()))
        # 유효한 티커만 필터링 (불필요한 설명행 제외)
        tickers = [t for t in tickers if isinstance(t, str) and t.isalpha() and len(t) <= 5]
        print(f"✅ 성공: {len(tickers)}개의 티커를 확보했습니다.")
        return tickers
    except Exception as e:
        print(f"⚠️ 나스닥 서버 접속 지연/실패: {e}")
        # 접속 실패 시 분석할 최소한의 핵심 주도주 리스트 (시스템 중단 방지용)
        return ['AAPL', 'NVDA', 'MSFT', 'TSLA', 'GOOGL', 'AMZN', 'META', 'AVGO', 'COST', 'NFLX']

def get_industry_fmp(ticker):
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
    tickers = get_official_nasdaq_tickers()
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
        if i % 500 == 0: print(f" > {i} / {len(tickers)} 분석 진행 중...")

    if not all_results:
        print("❌ 분석된 데이터가 없습니다.")
        return

    # 2. 결과 가공
    df = pd.DataFrame(all_results)
    df['rs_score'] = (df['rs_raw'].rank(pct=True) * 98 + 1).fillna(0).astype(int)
    
    # 등급 매기기 (Categorical 에러 방지를 위해 결과를 즉시 문자열로 변환)
    df['ad_grade'] = pd.qcut(df['ad_raw'].rank(method='first'), 5, labels=['E', 'D', 'C', 'B', 'A']).astype(str)
    
    print("--- 2단계: 주도주 산업군 정보 보강 ---")
    df['industry'] = 'Unknown'
    top_indices = df.sort_values('rs_score', ascending=False).head(200).index
    for idx in top_indices:
        df.at[idx, 'industry'] = get_industry_fmp(df.at[idx, 'symbol'])
        time.sleep(0.05)

    # 산업군 RS 점수
    ind_rs = df[df['industry'] != 'Unknown'].groupby('industry')['rs_raw'].mean().reset_index(name='ind_rs_raw')
    ind_rs['industry_rs_score'] = (ind_rs['ind_rs_raw'].rank(pct=True) * 98 + 1).fillna(0).astype(int)
    
    # 병합 후 결측치 처리 (Categorical 타입이 아니므로 fillna(0) 가능)
    final_df = pd.merge(df, ind_rs[['industry', 'industry_rs_score']], on='industry', how='left')
    final_df['industry_rs_score'] = final_df['industry_rs_score'].fillna(0).astype(int)
    final_df['smr_grade'] = 'C'

    # 3. DB 저장
    conn = sqlite3.connect('ibd_system.db')
    save_cols = ['symbol', 'price', 'rs_score', 'smr_grade', 'ad_grade', 'industry_rs_score', 'industry', 'adv_50']
    final_df[save_cols].to_sql('repo_results', conn, if_exists='replace', index=False)
    
    # 히스토리 저장
    history_df = final_df[['symbol', 'rs_score', 'industry_rs_score']].copy()
    history_df['date'] = datetime.now().strftime('%Y-%m-%d')
    history_df.to_sql('rs_history', conn, if_exists='append', index=False)
    conn.close()
    
    print(f"--- ✅ 전체 업데이트 성공 ({len(final_df)}개 종목) ---")

if __name__ == "__main__":
    update_database()
