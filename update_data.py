import yfinance as yf
import pandas as pd
import sqlite3
import time
from datetime import datetime
import os
import requests
import io
import numpy as np

def get_ticker_and_industry_list():
    """
    FMP API 대신 무료로 공개된 나스닥 스크리너 CSV를 활용하여 
    전 종목 리스트와 산업군 정보를 가져옵니다.
    """
    url = "https://raw.githubusercontent.com/yumoxu/stock-market-analysis/master/data/nasdaq_screener.csv"
    try:
        print("무료 데이터 소스에서 전 종목 리스트 및 산업군 로드 중...")
        response = requests.get(url).content
        df = pd.read_csv(io.StringIO(response.decode('utf-8')))
        
        # 필요한 컬럼만 추출 및 정리
        if 'Symbol' in df.columns and 'Industry' in df.columns:
            df = df[['Symbol', 'Industry', 'Name']].rename(
                columns={'Symbol': 'symbol', 'Industry': 'industry', 'Name': 'companyName'}
            )
            # 티커 기호 정리 (yfinance 호환용: .을 -로 변경)
            df['symbol'] = df['symbol'].astype(str).str.strip().str.upper().str.replace('.', '-', regex=False)
            df['industry'] = df['industry'].fillna('Unknown')
            return df
    except Exception as e:
        print(f"🚨 데이터 로드 실패: {e}")
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
    # 1. 무료 소스에서 전 종목 리스트 가져오기
    df_basics = get_ticker_and_industry_list()
    
    if df_basics.empty:
        print("❌ 종목 리스트를 가져오지 못해 중단합니다.")
        return

    tickers = df_basics['symbol'].unique().tolist()
    all_results = []
    
    print(f"--- 1단계: {len(tickers)}개 종목 주가 분석 시작 (yfinance 활용) ---")
    
    # 50개씩 끊어서 병렬 다운로드 (GitHub Actions IP 활용)
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
                    
                    # RS Raw 계산
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
            print(f" > {i} / {len(tickers)} 종목 처리 완료...")

    if not all_results:
        print("❌ 분석된 결과가 없습니다.")
        return

    # 2. 결과 결합 및 랭킹 산정
    df_prices = pd.DataFrame(all_results)
    final_df = pd.merge(df_prices, df_basics, on='symbol', how='left')
    
    final_df['rs_score'] = (final_df['rs_raw'].rank(pct=True) * 98 + 1).fillna(0).astype(int)
    final_df['ad_grade'] = pd.qcut(final_df['ad_raw'].rank(method='first'), 5, labels=['E', 'D', 'C', 'B', 'A'])
    
    # 산업군 RS 점수
    ind_rs = final_df.groupby('industry')['rs_raw'].mean().reset_index(name='ind_rs_raw')
    ind_rs['industry_rs_score'] = (ind_rs['ind_rs_raw'].rank(pct=True) * 98 + 1).fillna(0).astype(int)
    final_df = pd.merge(final_df, ind_rs[['industry', 'industry_rs_score']], on='industry', how='left')
    
    final_df['smr_grade'] = 'C' # 기본값

    # 3. DB 저장
    conn = sqlite3.connect('ibd_system.db')
    save_cols = ['symbol', 'price', 'rs_score', 'smr_grade', 'ad_grade', 'industry_rs_score', 'industry', 'adv_50']
    final_df[save_cols].to_sql('repo_results', conn, if_exists='replace', index=False)
    
    # 히스토리 저장
    history_df = final_df[['symbol', 'rs_score', 'industry_rs_score']].copy()
    history_df['date'] = datetime.now().strftime('%Y-%m-%d')
    history_df.to_sql('rs_history', conn, if_exists='append', index=False)
    
    conn.close()
    print(f"--- ✅ 전 종목 업데이트 완료: 총 {len(final_df)}개 종목 저장됨 ---")

if __name__ == "__main__":
    update_database()
