import yfinance as yf
import pandas as pd
import sqlite3
import numpy as np
import requests
from io import StringIO
from datetime import datetime

def get_all_tickers():
    """미국 시장 주요 종목 리스트 통합 (S&P 500, Nasdaq 100 + 주요 상장사)"""
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    }
    
    all_data = []

    # 1. S&P 500 수집
    try:
        print("S&P 500 수집 중...")
        url_sp = 'https://en.wikipedia.org/wiki/List_of_S%26P_500_companies'
        resp_sp = requests.get(url_sp, headers=headers)
        df_sp = pd.read_html(StringIO(resp_sp.text))[0]
        all_data.append(pd.DataFrame({'symbol': df_sp.iloc[:, 0], 'sector': df_sp.iloc[:, 3]}))
    except Exception as e: print(f"S&P 500 수집 실패: {e}")

    # 2. Nasdaq 100 수집
    try:
        print("Nasdaq 100 수집 중...")
        url_nd = 'https://en.wikipedia.org/wiki/Nasdaq-100'
        resp_nd = requests.get(url_nd, headers=headers)
        tables = pd.read_html(StringIO(resp_nd.text))
        df_nd = next(t for t in tables if any(col in t.columns for col in ['Ticker', 'Symbol']))
        nd_sym = df_nd.iloc[:, 1] if 'Ticker' in df_nd.columns else df_nd.iloc[:, 0]
        nd_sec = df_nd.iloc[:, 2] if df_nd.shape[1] > 2 else "Technology"
        all_data.append(pd.DataFrame({'symbol': nd_sym, 'sector': nd_sec}))
    except Exception as e: print(f"Nasdaq 100 수집 실패: {e}")

    # 통합 및 중복 제거
    full_df = pd.concat(all_data).drop_duplicates('symbol')
    full_df['symbol'] = full_df['symbol'].str.replace('.', '-', regex=False).str.strip()
    return full_df

def update_database():
    print(f"[{datetime.now()}] Step 1: 전체 티커 정보 확보 중...")
    try:
        master_data = get_all_tickers()
        tickers = master_data['symbol'].tolist()
        
        print(f"Step 2: {len(tickers)}개 종목 주가 다운로드 중 (Yahoo Finance)...")
        # 전체 종목 다운로드 (속도를 위해 기간은 1년으로 제한)
        raw_data = yf.download(tickers, period="1y", interval="1d", progress=True)
        data = raw_data['Close']
        
        print("Step 3: RS 점수 및 지표 계산 중...")
        results = []
        for ticker in data.columns:
            series = data[ticker].dropna()
            if len(series) < 250: continue
            
            # 오닐 RS 가중치 (3개월 성과 강조)
            now, m3, m6, m9, m12 = series.iloc[-1], series.iloc[-63], series.iloc[-126], series.iloc[-189], series.iloc[0]
            raw_score = (now/m3 * 2) + (now/m6) + (now/m9) + (now/m12)
            
            # 기술적 지표 (수급 등급용)
            ma50, ma200 = series.rolling(50).mean().iloc[-1], series.rolling(200).mean().iloc[-1]
            
            results.append({
                'symbol': ticker, 'raw_score': raw_score, 'price': now, 'ma50': ma50, 'ma200': ma200
            })
        
        rs_df = pd.DataFrame(results)
        rs_df['rs_score'] = rs_df['raw_score'].rank(pct=True) * 99
        
        # 섹터 정보 결합
        final_df = pd.merge(rs_df, master_data, on='symbol', how='left')
        final_df['sector'] = final_df['sector'].fillna("General")

        print("Step 4: 산업군(Industry) RS 점수 산출 중...")
        # 산업군별 평균 계산 후 1~99 점수화
        ind_map = final_df.groupby('sector')['raw_score'].mean().rank(pct=True) * 99
        final_df['industry_rs_score'] = final_df['sector'].map(ind_map.to_dict())

        print("Step 5: DB 저장 중...")
        db_ready = []
        for _, row in final_df.iterrows():
            db_ready.append({
                'symbol': row['symbol'],
                'rs_score': int(row['rs_score']),
                'industry_rs_score': int(row['industry_rs_score']) if pd.notna(row['industry_rs_score']) else 50,
                'smr_grade': "A" if row['rs_score'] > 85 else ("B" if row['rs_score'] > 70 else "C"),
                'ad_rating': "A" if row['price'] > row['ma50'] > row['ma200'] else "C",
                'sector': row['sector'],
                'last_updated': datetime.now().strftime('%Y-%m-%d')
            })

        # DB 파일명 및 테이블명 유지
        conn = sqlite3.connect('ibd_system.db')
        pd.DataFrame(db_ready).to_sql('repo_results', conn, if_exists='replace', index=False)
        conn.close()
        
        print(f"[{datetime.now()}] 업데이트 완료! 총 {len(db_ready)}개 종목 저장됨.")

    except Exception as e:
        print(f"오류 발생: {e}")

if __name__ == "__main__":
    update_database()
