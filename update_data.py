import yfinance as yf
import pandas as pd
import sqlite3
import time
from datetime import datetime
import os
import requests
import numpy as np

# --- [설정] FMP API 키 ---
FMP_API_KEY = os.environ.get('FMP_API_KEY', "1kJBflGjsp5fCgbancejhI5bN5iavEJF")

def get_ticker_list_fmp():
    """
    403 에러를 피하기 위해 Legacy가 아닌 
    최신 'Exchange Symbol List' 엔드포인트를 사용합니다.
    """
    exchanges = ['NASDAQ', 'NYSE', 'AMEX']
    all_tickers = []
    
    print(f"📡 최신 Exchange API를 통해 종목 리스트 로드 시작...")
    
    for ex in exchanges:
        try:
            # v3/symbol/거래소 경로가 현재 신규 유저에게 권장되는 방식입니다.
            url = f"https://financialmodelingprep.com/api/v3/symbol/{ex}?apikey={FMP_API_KEY}"
            response = requests.get(url, timeout=15)
            
            if response.status_code == 200:
                res = response.json()
                if isinstance(res, list) and len(res) > 0:
                    for item in res:
                        all_tickers.append({
                            'symbol': item.get('symbol', '').strip().upper().replace('.', '-'),
                            'name': item.get('name', 'Unknown'),
                            'industry': item.get('type', 'Unknown') # 일부 API는 여기서 업종을 주기도 함
                        })
                    print(f"✅ {ex} 거래소 로드 완료: {len(res)}개")
                else:
                    print(f"⚠️ {ex} 응답 데이터가 비어있습니다.")
            else:
                print(f"❌ {ex} 서버 응답 오류: {response.status_code}")
                print(f"🔎 메시지: {response.text}")
        except Exception as e:
            print(f"❌ {ex} 접속 오류: {e}")
            
    return pd.DataFrame(all_tickers).drop_duplicates('symbol')

def get_industry_fmp(ticker):
    """상위권 종목 상세 업종 정보 획득"""
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
    df_basics = get_ticker_list_fmp()
    
    if df_basics.empty:
        print("❌ 종목 리스트를 확보하지 못했습니다. FMP API 키의 유효성을 확인하세요.")
        return

    tickers = df_basics['symbol'].unique().tolist()
    all_results = []
    
    print(f"--- 1단계: {len(tickers)}개 종목 가격 분석 시작 (yfinance) ---")
    chunk_size = 50
    for i in range(0, len(tickers), chunk_size):
        chunk = tickers[i:i + chunk_size]
        try:
            data = yf.download(chunk, period="1y", interval="1d", progress=False, group_by='ticker', threads=True)
            for ticker in chunk:
                try:
                    hist = data[ticker].dropna() if len(chunk) > 1 else data.dropna()
                    if len(hist) < 150: continue
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
        if i % 500 == 0: print(f" > {i} / {len(tickers)} 분석 중...")

    if not all_results: return
    df_prices = pd.DataFrame(all_results)
    df_prices['rs_score'] = (df_prices['rs_raw'].rank(pct=True) * 98 + 1).fillna(0).astype(int)

    # 2단계: 산업군 분석 (상위 200개 종목에 집중)
    print("--- 2단계: 주도주 상세 산업군 로드 ---")
    top_indices = df_prices.sort_values('rs_score', ascending=False).head(200).index
    df_prices['industry'] = 'Unknown'
    for idx in top_indices:
        df_prices.at[idx, 'industry'] = get_industry_fmp(df_prices.at[idx, 'symbol'])
        time.sleep(0.05)

    df_prices['ad_grade'] = pd.qcut(df_prices['ad_raw'].rank(method='first'), 5, labels=['E', 'D', 'C', 'B', 'A'])
    df_prices['smr_grade'] = 'C'
    
    ind_rs = df_prices[df_prices['industry'] != 'Unknown'].groupby('industry')['rs_raw'].mean().reset_index(name='ind_rs_raw')
    ind_rs['industry_rs_score'] = (ind_rs['ind_rs_raw'].rank(pct=True) * 98 + 1).fillna(0).astype(int)
    final_df = pd.merge(df_prices, ind_rs[['industry', 'industry_rs_score']], on='industry', how='left').fillna(0)

    conn = sqlite3.connect('ibd_system.db')
    save_cols = ['symbol', 'price', 'rs_score', 'smr_grade', 'ad_grade', 'industry_rs_score', 'industry', 'adv_50']
    final_df[save_cols].to_sql('repo_results', conn, if_exists='replace', index=False)
    
    history_df = final_df[['symbol', 'rs_score', 'industry_rs_score']].copy()
    history_df['date'] = datetime.now().strftime('%Y-%m-%d')
    history_df.to_sql('rs_history', conn, if_exists='append', index=False)
    conn.close()
    print(f"--- ✅ 업데이트 완료! ---")

if __name__ == "__main__":
    update_database()
