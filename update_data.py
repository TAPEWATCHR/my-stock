import yfinance as yf
import pandas as pd
import sqlite3
import time
from datetime import datetime
import os
import numpy as np

def calculate_acc_dist_rating(hist):
    """최근 65거래일 수급(A~E) 등급 계산"""
    if len(hist) < 20: return 'C'
    df = hist.iloc[-65:].copy()
    df['Price_Change'] = df['Close'].diff()
    up_vol = df[df['Price_Change'] > 0]['Volume'].sum()
    down_vol = df[df['Price_Change'] < 0]['Volume'].sum()
    if down_vol <= 0: return 'C'
    ratio = up_vol / down_vol
    if ratio >= 1.5: return 'A'
    elif ratio >= 1.2: return 'B'
    elif ratio >= 0.9: return 'C'
    elif ratio >= 0.7: return 'D'
    else: return 'E'

def update_database():
    if not os.path.exists('tickers.txt'):
        print("tickers.txt 파일이 없습니다.")
        return

    with open('tickers.txt', 'r') as f:
        tickers = [line.strip().upper() for line in f if line.strip()]
    
    all_results = []
    chunk_size = 40 
    
    print(f"--- IBD 분석 업데이트 시작 ({datetime.now()}) ---")

    for i in range(0, len(tickers), chunk_size):
        chunk = tickers[i:i + chunk_size]
        try:
            # 벌크 다운로드로 속도 향상
            data = yf.download(chunk, period="1y", interval="1d", progress=False, group_by='ticker', threads=True)
            
            for ticker in chunk:
                try:
                    if ticker not in data.columns.get_level_values(0): continue
                    hist = data[ticker].dropna()
                    if len(hist) < 200: continue

                    now_price = hist['Close'].iloc[-1]
                    ad_rating = calculate_acc_dist_rating(hist)

                    # RS 원천 점수 (가중치 적용)
                    close = hist['Close']
                    m3, m6, m9, m12 = close.iloc[-63], close.iloc[-126], close.iloc[-189], close.iloc[0]
                    rs_raw = (now_price/m3 * 2) + (now_price/m6) + (now_price/m9) + (now_price/m12)

                    # SMR용 기본 데이터 추출 (야후 info 활용)
                    roe, margin, growth, sector = 0, 0, 0, 'Unknown'
                    try:
                        t_obj = yf.Ticker(ticker)
                        info = t_obj.info
                        roe = info.get('returnOnEquity', 0)
                        margin = info.get('profitMargins', 0)
                        growth = info.get('revenueGrowth', 0)
                        sector = info.get('sector', 'Unknown')
                    except: pass

                    all_results.append({
                        'symbol': ticker,
                        'price': round(now_price, 2),
                        'rs_raw': rs_raw,
                        'roe': roe if roe is not None else 0,
                        'margin': margin if margin is not None else 0,
                        'sales_growth': growth if growth is not None else 0,
                        'ad_rating': ad_rating,
                        'sector': sector
                    })
                except: continue
        except Exception as e:
            print(f"Chunk error: {e}")
            continue
            
        time.sleep(1.5)

    if all_results:
        df = pd.DataFrame(all_results)
        
        # 1. 종목별 RS 점수 (상대평가)
        df['rs_score'] = (df['rs_raw'].rank(pct=True) * 98 + 1).astype(int)
        
        # 2. SMR 고도화 산식 (왜곡 방지 랭킹 합산법)
        # 각 지표를 0~1 사이의 순위로 변환하여 특정 수치 폭등의 영향력을 제한함
        df['roe_rank'] = df['roe'].rank(pct=True)
        df['margin_rank'] = df['margin'].rank(pct=True)
        df['growth_rank'] = df['sales_growth'].rank(pct=True)
        
        # 세 지표 순위의 평균값을 기준으로 최종 A~E 등급 부여
        df['smr_value'] = (df['roe_rank'] + df['margin_rank'] + df['growth_rank']) / 3
        df['smr_grade'] = pd.qcut(df['smr_value'].rank(method='first'), 5, labels=['E', 'D', 'C', 'B', 'A'])
        
        # 3. 산업군 RS (상대평가)
        sector_avg = df.groupby('sector')['rs_score'].mean().reset_index()
        sector_avg['industry_rs_score'] = (sector_avg['rs_score'].rank(pct=True) * 98 + 1).astype(int)
        sector_avg = sector_avg.drop(columns=['rs_score'])
        
        final_df = pd.merge(df, sector_avg, on='sector', how='left')
        final_df['industry_rs_score'] = final_df['industry_rs_score'].fillna(0).astype(int)

        # DB 저장 (필요한 컬럼만 선별)
        conn = sqlite3.connect('ibd_system.db')
        final_df[['symbol', 'price', 'rs_score', 'smr_grade', 'ad_rating', 'industry_rs_score', 'sector']].to_sql('repo_results', conn, if_exists='replace', index=False)
        conn.close()
        print(f"[{datetime.now()}] 업데이트 완료!")

if __name__ == "__main__":
    update_database()
