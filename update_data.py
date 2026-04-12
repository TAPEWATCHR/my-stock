import yfinance as yf
import pandas as pd
import sqlite3
import time
from datetime import datetime
import os
import requests
import io
import numpy as np

def get_official_nasdaq_tickers():
    """
    FMP API가 막혔을 때 사용하는 최후의 수단입니다.
    나스닥 공식 FTP 서버의 실시간 상장 종목 리스트를 가져옵니다.
    """
    try:
        print("📡 나스닥 공식 서버(ftp.nasdaqtrader.com)에서 종목 리스트 로드 중...")
        # 나스닥 상장 종목 리스트 (nasdaqlisted.txt)
        url = "http://ftp.nasdaqtrader.com/SymbolDirectory/nasdaqlisted.txt"
        res = requests.get(url).text
        df_nasdaq = pd.read_csv(io.StringIO(res), sep="|")
        
        # 기타 거래소 종목 리스트 (otherlisted.txt - NYSE, AMEX 포함)
        url_other = "http://ftp.nasdaqtrader.com/SymbolDirectory/otherlisted.txt"
        res_other = requests.get(url_other).text
        df_other = pd.read_csv(io.StringIO(res_other), sep="|")
        
        # 데이터 합치기
        tickers_nasdaq = df_nasdaq['Symbol'].dropna().tolist()
        tickers_other = df_other['NASDAQ Symbol'].dropna().tolist()
        
        all_tickers = list(set(tickers_nasdaq + tickers_other))
        
        # 파일 끝의 가비지 데이터(File Creation Time 등) 제거
        all_tickers = [t for t in all_tickers if t.isalpha() and len(t) <= 5]
        
        print(f"✅ 성공: 총 {len(all_tickers)}개의 공식 티커를 확보했습니다.")
        return all_tickers
    except Exception as e:
        print(f"❌ 나스닥 서버 접속 실패: {e}")
        # 최악의 경우를 대비한 하드코딩 백업
        return ['AAPL', 'NVDA', 'MSFT', 'TSLA', 'GOOGL', 'AMZN', 'META']

def get_industry_fmp(ticker):
    """상위권 종목 상세 업종 정보 (개별 호출은 아직 무료 키로 가능할 확률이 높음)"""
    FMP_API_KEY = os.environ.get('FMP_API_KEY', "1kJBflGjsp5fCgbancejhI5bN5iavEJF")
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
    # 1. 공식 소스에서 티커 가져오기
    tickers = get_official_nasdaq_tickers()
    
    all_results = []
    print(f"--- 1단계: {len(tickers)}개 종목 분석 시작 (yfinance) ---")
    
    chunk_size = 50
    for i in range(0, len(tickers), chunk_size):
        chunk = tickers[i:i + chunk_size]
        try:
            data = yf.download(chunk, period="1y", interval="1d", progress=False, group_by='ticker', threads=True)
            for ticker in chunk:
                try:
                    hist = data[ticker].dropna() if len(chunk) > 1 else data.dropna()
                    if len(hist) < 200: continue # 충분한 데이터가 있는 종목만

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

    # 2. 결과 가공 및 랭킹
    df = pd.DataFrame(all_results)
    df['rs_score'] = (df['rs_raw'].rank(pct=True) * 98 + 1).fillna(0).astype(int)
    df['ad_grade'] = pd.qcut(df['ad_raw'].rank(method='first'), 5, labels=['E', 'D', 'C', 'B', 'A'])

    # 3. 상위 200개 종목 산업군 보강
    print("--- 2단계: 주도주 산업군 정보 보강 ---")
    df['industry'] = 'Unknown'
    top_indices = df.sort_values('rs_score', ascending=False).head(200).index
    for idx in top_indices:
        df.at[idx, 'industry'] = get_industry_fmp(df.at[idx, 'symbol'])
        time.sleep(0.05)

    # 산업군 RS 점수
    ind_rs = df[df['industry'] != 'Unknown'].groupby('industry')['rs_raw'].mean().reset_index(name='ind_rs_raw')
    ind_rs['industry_rs_score'] = (ind_rs['ind_rs_raw'].rank(pct=True) * 98 + 1).fillna(0).astype(int)
    final_df = pd.merge(df, ind_rs[['industry', 'industry_rs_score']], on='industry', how='left').fillna(0)
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
