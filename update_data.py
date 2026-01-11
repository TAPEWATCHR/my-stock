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
    df = hist.iloc[-65:].copy() # 최근 13주(약 3개월) 데이터
    df['Price_Change'] = df['Close'].diff()
    
    # 상승일 거래량 합계 vs 하락일 거래량 합계
    up_vol = df[df['Price_Change'] > 0]['Volume'].sum()
    down_vol = df[df['Price_Change'] < 0]['Volume'].sum()
    
    if down_vol == 0: return 'A'
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
    chunk_size = 20 # 서버 차단 방지를 위한 소규모 분할 처리
    
    print(f"--- IBD 종합 지표 분석 시작 (대상: {len(tickers)}개) ---")

    for i in range(0, len(tickers), chunk_size):
        chunk = tickers[i:i + chunk_size]
        print(f"[{i}/{len(tickers)}] 상세 분석 진행 중...")
        
        for ticker in chunk:
            try:
                t_obj = yf.Ticker(ticker)
                # 가격 및 거래량 데이터 수집
                hist = t_obj.history(period="1y")
                if len(hist) < 200: continue # 데이터 부족 종목 제외

                # [1] 수급 등급 계산 (ad_rating)
                ad_rating = calculate_acc_dist_rating(hist)

                # [2] RS 점수 계산용 원천 데이터
                close = hist['Close']
                now = close.iloc[-1]
                m3, m6, m9, m12 = close.iloc[-63], close.iloc[-126], close.iloc[-189], close.iloc[0]
                rs_raw = (now/m3 * 2) + (now/m6) + (now/m9) + (now/m12)

                # [3] 재무 데이터 및 섹터 정보
                info = t_obj.info
                roe = info.get('returnOnEquity', 0)
                margin = info.get('profitMargins', 0)
                sales_growth = info.get('revenueGrowth', 0)
                sector = info.get('sector', 'Unknown')

                all_results.append({
                    'symbol': ticker,
                    'price': round(now, 2),
                    'rs_raw': rs_raw,
                    'roe': roe,
                    'margin': margin,
                    'sales_growth': sales_growth,
                    'ad_rating': ad_rating, # 스트림릿 요구 컬럼명
                    'sector': sector
                })
            except Exception:
                continue
        
        # 야후 파이낸스 차단 방지를 위한 짧은 휴식
        time.sleep(1)

    if all_results:
        df = pd.DataFrame(all_results)
        
        # [4] 종목별 RS 점수 정규화 (1-99)
        df['rs_score'] = (df['rs_raw'].rank(pct=True) * 99).astype(int)
        
        # [5] SMR 등급 산정 (smr_grade)
        # ROE, Margin, Sales Growth 합산 기반 상대 평가
        df['smr_value'] = df['roe'].fillna(0) + df['margin'].fillna(0) + df['sales_growth'].fillna(0)
        df['smr_grade'] = pd.qcut(df['smr_value'].rank(method='first'), 5, labels=['E', 'D', 'C', 'B', 'A'])
        
        # [6] 섹터별 RS 평균 점수 (industry_rs_score)
        sector_avg = df.groupby('sector')['rs_score'].mean().reset_index()
        sector_avg.columns = ['sector', 'industry_rs_score']
        
        # 최종 데이터 병합
        final_df = pd.merge(df, sector_avg, on='sector', how='left')
        final_df['industry_rs_score'] = final_df['industry_rs_score'].fillna(0).astype(int)

        # [7] SQLite DB 저장
        conn = sqlite3.connect('ibd_system.db')
        # 스트림릿이 사용하는 테이블 명인 'repo_results'로 저장
        final_df.to_sql('repo_results', conn, if_exists='replace', index=False)
        conn.close()
        
        print(f"[{datetime.now()}] 업데이트 완료! 총 {len(final_df)}개 종목 분석 성공.")
        print(f"반영된 컬럼: rs_score, smr_grade, ad_rating, industry_rs_score")

if __name__ == "__main__":
    update_database()
