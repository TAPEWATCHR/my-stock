import yfinance as yf
import pandas as pd
import sqlite3
import time
from datetime import datetime
import os
import requests
import numpy as np

# --- [중요] API 키 설정 ---
# GitHub Actions에서는 Secrets에서 가져오고, 로컬 테스트 시에는 직접 입력한 키를 사용합니다.
FMP_API_KEY = os.environ.get('FMP_API_KEY', "1kJBflGjsp5fCgbancejhI5bN5iavEJF")

def get_all_stock_basics_fmp():
    """FMP 스크리너로 전 종목의 기초 데이터를 가져옵니다."""
    try:
        print(f"FMP에서 전 종목 기초 데이터를 로드 중... (Key: {FMP_API_KEY[:5]}***)")
        url = f"https://financialmodelingprep.com/api/v3/stock-screener?apikey={FMP_API_KEY}"
        response = requests.get(url)
        res = response.json()
        
        # 정상적으로 리스트(종목들)를 받았을 경우
        if isinstance(res, list):
            if len(res) == 0:
                print("FMP에서 빈 리스트를 반환했습니다. 검색 조건이나 키를 확인하세요.")
                return pd.DataFrame()
            df = pd.DataFrame(res)
            return df[['symbol', 'industry', 'marketCap', 'companyName']]
        
        # 리스트가 아닌 에러 메시지(dict)를 받았을 경우
        else:
            print(f"🚨 FMP API 응답 에러 발생: {res}")
            return pd.DataFrame()
            
    except Exception as e:
        print(f"🚨 FMP 데이터 로드 중 시스템 오류: {e}")
    return pd.DataFrame()

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
    df_basics = get_all_stock_basics_fmp()
    
    if df_basics.empty:
        print("❌ 기본 데이터를 가져오지 못해 업데이트를 중단합니다. 위 로그의 API 응답 에러를 확인하세요.")
        return

    tickers = df_basics['symbol'].unique().tolist()
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
                    if len(hist) < 150: continue

                    price = hist['Close'].iloc[-1]
                    idx_252 = -min(252, len(hist))
                    rs_raw = (price/hist['Close'].iloc[-21]*2) + (price/hist['Close'].iloc[-63]*2) + \
                             (price/hist['Close'].iloc[-126]) + (price/hist['Close'].iloc[idx_252])
                    
                    all_results.append({
                        'symbol': ticker, 
                        'price': float(price), 
                        'rs_raw': rs_raw,
                        'ad_raw': calculate_ad_raw(hist), 
                        'adv_50': (hist['Close']*hist['Volume']).tail(50).mean()
                    })
                except: continue
        except: continue
        
        if i % 500 == 0:
            print(f" > {i} / {len(tickers)} 진행 중...")

    if not all_results:
        print("❌ 분석된 종목 결과가 없습니다.")
        return

    df_prices = pd.DataFrame(all_results)
    final_df = pd.merge(df_prices, df_basics, on='symbol', how='left')
    
    final_df['rs_score'] = (final_df['rs_raw'].rank(pct=True) * 98 + 1).fillna(0).astype(int)
    final_df['ad_grade'] = pd.qcut(final_df['ad_raw'].rank(method='first'), 5, labels=['E', 'D', 'C', 'B', 'A'])
    
    ind_rs = final_df.groupby('industry')['rs_raw'].mean().reset_index(name='ind_rs_raw')
    ind_rs['industry_rs_score'] = (ind_rs['ind_rs_raw'].rank(pct=True) * 98 + 1).fillna(0).astype(int)
    final_df = pd.merge(final_df, ind_rs[['industry', 'industry_rs_score']], on='industry', how='left')
    final_df['smr_grade'] = 'C'

    conn = sqlite3.connect('ibd_system.db')
    save_cols = ['symbol', 'price', 'rs_score', 'smr_grade', 'ad_grade', 'industry_rs_score', 'industry', 'adv_50']
    final_df[save_cols].to_sql('repo_results', conn, if_exists='replace', index=False)
    
    history_df = final_df[['symbol', 'rs_score', 'industry_rs_score']].copy()
    history_df['date'] = datetime.now().strftime('%Y-%m-%d')
    history_df.to_sql('rs_history', conn, if_exists='append', index=False)
    
    conn.close()
    print(f"--- ✅ 업데이트 완료: 총 {len(final_df)}개 종목 저장됨 ---")

if __name__ == "__main__":
    update_database()
