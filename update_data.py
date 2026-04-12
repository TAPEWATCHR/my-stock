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
    여러 무료 소스를 시도하여 미국 주식 리스트와 산업군 정보를 가져옵니다.
    """
    # 시도해볼 데이터 소스들 (유지보수가 잘 되는 경로들)
    sources = [
        {
            "url": "https://raw.githubusercontent.com/rreichel3/US-Stock-Symbols/main/nasdaq/nasdaq_full_tickers.csv",
            "sym_col": "symbol", "ind_col": "industry", "name_col": "name"
        },
        {
            "url": "https://raw.githubusercontent.com/yumoxu/stock-market-analysis/master/data/nasdaq_screener.csv",
            "sym_col": "Symbol", "ind_col": "Industry", "name_col": "Name"
        }
    ]

    for source in sources:
        try:
            print(f"데이터 소스 시도 중: {source['url']}")
            response = requests.get(source['url'], timeout=15)
            
            if response.status_code == 200:
                df = pd.read_csv(io.StringIO(response.text))
                # 컬럼명 표준화
                df = df.rename(columns={
                    source['sym_col']: 'symbol',
                    source['ind_col']: 'industry',
                    source['name_col']: 'companyName'
                })
                
                # 티커 기호 정리 (.을 -로 변경)
                df['symbol'] = df['symbol'].astype(str).str.strip().str.upper().str.replace('.', '-', regex=False)
                df['industry'] = df['industry'].fillna('Unknown')
                
                # 필수 컬럼만 남기기
                df = df[['symbol', 'industry', 'companyName']]
                print(f"✅ 성공적으로 {len(df)}개의 종목 리스트를 로드했습니다.")
                return df
            else:
                print(f"⚠️ 응답 실패 (상태 코드: {response.status_code})")
        except Exception as e:
            print(f"❌ 해당 소스 로드 중 에러: {e}")
            continue
            
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
    df_basics = get_ticker_and_industry_list()
    
    if df_basics.empty:
        print("❌ 모든 데이터 소스에서 리스트를 가져오는 데 실패했습니다. 중단합니다.")
        return

    tickers = df_basics['symbol'].unique().tolist()
    all_results = []
    
    print(f"--- 1단계: {len(tickers)}개 종목 주가 분석 시작 ---")
    
    # 작업량을 고려하여 50개씩 청크 처리
    chunk_size = 50
    for i in range(0, len(tickers), chunk_size):
        chunk = tickers[i:i + chunk_size]
        try:
            # GitHub Actions 환경의 IP를 활용해 다운로드
            data = yf.download(chunk, period="1y", interval="1d", progress=False, group_by='ticker', threads=True)
            
            for ticker in chunk:
                try:
                    # 데이터 구조 처리
                    if len(chunk) > 1:
                        if ticker not in data.columns.get_level_values(0): continue
                        hist = data[ticker].dropna()
                    else:
                        hist = data.dropna()

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
        except Exception as e:
            print(f"Chunk 처리 중 오류: {e}")
        
        if i % 500 == 0:
            print(f" > {i} / {len(tickers)} 종목 완료...")

    if not all_results:
        print("❌ 분석 결과가 없습니다.")
        return

    # 결과 결합 및 랭킹
    df_prices = pd.DataFrame(all_results)
    final_df = pd.merge(df_prices, df_basics, on='symbol', how='left')
    
    final_df['rs_score'] = (final_df['rs_raw'].rank(pct=True) * 98 + 1).fillna(0).astype(int)
    final_df['ad_grade'] = pd.qcut(final_df['ad_raw'].rank(method='first'), 5, labels=['E', 'D', 'C', 'B', 'A'])
    
    # 산업군 RS 점수
    ind_rs = final_df.groupby('industry')['rs_raw'].mean().reset_index(name='ind_rs_raw')
    ind_rs['industry_rs_score'] = (ind_rs['ind_rs_raw'].rank(pct=True) * 98 + 1).fillna(0).astype(int)
    final_df = pd.merge(final_df, ind_rs[['industry', 'industry_rs_score']], on='industry', how='left')
    
    final_df['smr_grade'] = 'C'

    # DB 저장
    conn = sqlite3.connect('ibd_system.db')
    save_cols = ['symbol', 'price', 'rs_score', 'smr_grade', 'ad_grade', 'industry_rs_score', 'industry', 'adv_50']
    final_df[save_cols].to_sql('repo_results', conn, if_exists='replace', index=False)
    
    # 히스토리 기록
    history_df = final_df[['symbol', 'rs_score', 'industry_rs_score']].copy()
    history_df['date'] = datetime.now().strftime('%Y-%m-%d')
    history_df.to_sql('rs_history', conn, if_exists='append', index=False)
    
    conn.close()
    print(f"--- ✅ 완료: 총 {len(final_df)}개 종목이 DB에 저장되었습니다. ---")

if __name__ == "__main__":
    update_database()
