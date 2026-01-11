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
    
    # 상승일 거래량 합계 vs 하락일 거래량 합계
    up_vol = df[df['Price_Change'] > 0]['Volume'].sum()
    down_vol = df[df['Price_Change'] < 0]['Volume'].sum()
    
    if down_vol <= 0: return 'C'
    ratio = up_vol / down_vol
    
    # IBD 스타일 등급 부여
    if ratio >= 1.5: return 'A'
    elif ratio >= 1.2: return 'B'
    elif ratio >= 0.9: return 'C'
    elif ratio >= 0.7: return 'D'
    else: return 'E'

def update_database():
    # 1. 티커 리스트 로드
    if not os.path.exists('tickers.txt'):
        print("에러: tickers.txt 파일이 없습니다.")
        return

    with open('tickers.txt', 'r') as f:
        tickers = [line.strip().upper() for line in f if line.strip()]
    
    all_results = []
    chunk_size = 40 # 야후 차단 방지를 위한 벌크 다운로드 단위
    
    print(f"--- IBD 종합 지표 분석 시작 (대상: {len(tickers)}개) ---")

    for i in range(0, len(tickers), chunk_size):
        chunk = tickers[i:i + chunk_size]
        print(f"[{i}/{len(tickers)}] 구간 데이터 수집 및 분석 중...")
        
        try:
            # 가격/거래량 데이터 일괄 수집
            data = yf.download(chunk, period="1y", interval="1d", progress=False, group_by='ticker', threads=True)
            
            for ticker in chunk:
                try:
                    if ticker not in data.columns.get_level_values(0): continue
                    hist = data[ticker].dropna()
                    if len(hist) < 200: continue

                    # [1] 수급 등급 계산 (ad_rating)
                    ad_rating = calculate_acc_dist_rating(hist)

                    # [2] RS 원천 데이터 계산
                    close = hist['Close']
                    now = close.iloc[-1]
                    m3, m6, m9, m12 = close.iloc[-63], close.iloc[-126], close.iloc[-189], close.iloc[0]
                    rs_raw = (now/m3 * 2) + (now/m6) + (now/m9) + (now/m12)

                    # [3] 재무 및 섹터 데이터 (info 호출 최소화)
                    roe, margin, growth, sector = 0, 0, 0, 'Unknown'
                    try:
                        t_info = yf.Ticker(ticker).info
                        roe = t_info.get('returnOnEquity', 0)
                        margin = t_info.get('profitMargins', 0)
                        growth = t_info.get('revenueGrowth', 0)
                        sector = t_info.get('sector', 'Unknown')
                    except:
                        pass # 데이터가 없으면 0/Unknown 유지

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
            print(f"구간 오류: {e}")
            continue
            
        # 차단 방지를 위한 간격
        time.sleep(2)

    if all_results:
        df = pd.DataFrame(all_results)
        
        # [4] 종목별 RS 점수 1-99 (상대 평가)
        df['rs_score'] = (df['rs_raw'].rank(pct=True) * 98 + 1).astype(int)
        
        # [5] SMR 등급 산정 (A-E)
        df['smr_value'] = df['roe'].fillna(0) + df['margin'].fillna(0) + df['sales_growth'].fillna(0)
        df['smr_grade'] = pd.qcut(df['smr_value'].rank(method='first'), 5, labels=['E', 'D', 'C', 'B', 'A'])
        
        # [6] 산업군 RS 점수 (2단계 상대 평가 적용)
        # Step 1: 섹터별 평균 계산
        sector_avg = df.groupby('sector')['rs_score'].mean().reset_index()
        # Step 2: [중요] 섹터 평균값들을 다시 1-99로 줄 세우기 (상대 평가)
        sector_avg['industry_rs_score'] = (sector_avg['rs_score'].rank(pct=True) * 98 + 1).astype(int)
        sector_avg = sector_avg.drop(columns=['rs_score'])
        
        # 최종 데이터 병합
        final_df = pd.merge(df, sector_avg, on='sector', how='left')
        final_df['industry_rs_score'] = final_df['industry_rs_score'].fillna(0).astype(int)

        # [7] SQLite DB 저장
        conn = sqlite3.connect('ibd_system.db')
        final_df.to_sql('repo_results', conn, if_exists='replace', index=False)
        conn.close()
        
        print(f"[{datetime.now()}] 업데이트 완료!")
        print(f"산업군 RS 변별력 강화(1-99) 및 {len(final_df)}개 종목 분석 성공.")

if __name__ == "__main__":
    update_database()
