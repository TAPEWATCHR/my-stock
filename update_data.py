import yfinance as yf
import pandas as pd
import sqlite3
import time
from datetime import datetime
import os
import numpy as np

def calculate_acc_dist_rating(hist):
    """주가 상승/하락 시 거래량을 비교하여 수급(A~E) 등급 계산"""
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
    chunk_size = 40 # 가격 데이터는 40개씩 묶어서 다운로드
    
    print(f"--- IBD 종합 분석 시작 (총 {len(tickers)}개) ---")

    for i in range(0, len(tickers), chunk_size):
        chunk = tickers[i:i + chunk_size]
        print(f"[{i}/{len(tickers)}] 구간 수집 중...")
        
        try:
            # 1. 가격/거래량 데이터 일괄 수집 (속도 향상의 핵심)
            # group_by='column'을 사용하여 데이터를 관리하기 편하게 가져옵니다.
            data = yf.download(chunk, period="1y", interval="1d", progress=False, group_by='ticker', threads=True)
            
            for ticker in chunk:
                try:
                    # 데이터 존재 여부 확인
                    if ticker not in data.columns.get_level_values(0): continue
                    hist = data[ticker].dropna()
                    if len(hist) < 200: continue

                    # [수급 등급]
                    ad_rating = calculate_acc_dist_rating(hist)

                    # [RS 원천 점수]
                    close = hist['Close']
                    now = close.iloc[-1]
                    m3, m6, m9, m12 = close.iloc[-63], close.iloc[-126], close.iloc[-189], close.iloc[0]
                    rs_raw = (now/m3 * 2) + (now/m6) + (now/m9) + (now/m12)

                    # [재무 데이터 상세 조회] 
                    # info 호출은 차단 위험이 크므로 별도 예외 처리
                    roe, margin, growth, sector = 0, 0, 0, 'Unknown'
                    try:
                        t_info = yf.Ticker(ticker).info
                        roe = t_info.get('returnOnEquity', 0)
                        margin = t_info.get('profitMargins', 0)
                        growth = t_info.get('revenueGrowth', 0)
                        sector = t_info.get('sector', 'Unknown')
                    except:
                        # 차단되거나 데이터가 없으면 기본값 유지
                        pass

                    all_results.append({
                        'symbol': ticker,
                        'price': round(now, 2),
                        'rs_raw': rs_raw,
                        'roe': roe,
                        'margin': margin,
                        'sales_growth': growth,
                        'ad_rating': ad_rating,
                        'sector': sector
                    })
                except: continue
                
        except Exception as e:
            print(f"구간 처리 중 오류 발생: {e}")
            continue
            
        # 야후 차단 방지를 위한 휴식 (초당 요청 수 조절)
        time.sleep(2)

    if all_results:
        df = pd.DataFrame(all_results)
        
        # [종목별 RS 점수 1-99]
        df['rs_score'] = (df['rs_raw'].rank(pct=True) * 98 + 1).astype(int)
        
        # [SMR 등급 A-E]
        df['smr_value'] = df['roe'].fillna(0) + df['margin'].fillna(0) + df['sales_growth'].fillna(0)
        df['smr_grade'] = pd.qcut(df['smr_value'].rank(method='first'), 5, labels=['E', 'D', 'C', 'B', 'A'])
        
        # [섹터 RS 점수] - 종목 수가 많아지면 1~99로 넓게 퍼짐
        sector_avg = df.groupby('sector')['rs_score'].mean().reset_index()
        sector_avg.columns = ['sector', 'industry_rs_score']
        
        final_df = pd.merge(df, sector_avg, on='sector', how='left')
        final_df['industry_rs_score'] = final_df['industry_rs_score'].fillna(0).astype(int)

        conn = sqlite3.connect('ibd_system.db')
        final_df.to_sql('repo_results', conn, if_exists='replace', index=False)
        conn.close()
        print(f"[{datetime.now()}] 업데이트 완료! 총 {len(final_df)}개 분석됨.")

if __name__ == "__main__":
    update_database()
