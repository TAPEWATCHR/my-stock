import yfinance as yf
import pandas as pd
import sqlite3
import time
from datetime import datetime
import os

def calculate_acc_dist_rating(hist):
    """주가 상승/하락 시 거래량을 비교하여 수급(A~E) 등급 계산"""
    if len(hist) < 20: return 'C'
    df = hist.iloc[-65:].copy() # 최근 13주
    df['Price_Change'] = df['Close'].diff()
    up_vol = df[df['Price_Change'] > 0]['Volume'].sum()
    down_vol = df[df['Price_Change'] < 0]['Volume'].sum()
    if down_vol == 0: return 'A'
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
    chunk_size = 20 
    print(f"--- IBD 종합 분석 시작 (SMR + 수급 + 섹터RS) ---")

    for i in range(0, len(tickers), chunk_size):
        chunk = tickers[i:i + chunk_size]
        print(f"[{i}/{len(tickers)}] 상세 분석 진행 중...")
        
        for ticker in chunk:
            try:
                t_obj = yf.Ticker(ticker)
                # 수급 분석을 위해 1년치 데이터 필요
                hist = t_obj.history(period="1y")
                if len(hist) < 200: continue

                # 1. 수급(A/D) 등급
                acc_dist = calculate_acc_dist_rating(hist)

                # 2. RS 원천 데이터
                close = hist['Close']
                now = close.iloc[-1]
                m3, m6, m9, m12 = close.iloc[-63], close.iloc[-126], close.iloc[-189], close.iloc[0]
                rs_raw = (now/m3 * 2) + (now/m6) + (now/m9) + (now/m12)

                # 3. 재무(SMR) 및 섹터 데이터
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
                    'acc_dist': acc_dist,
                    'sector': sector
                })
            except: continue
        time.sleep(1)

# ... (앞부분 함수들은 동일) ...

    if all_results:
        df = pd.DataFrame(all_results)
        
        # 1. RS 점수 정규화 (1-99)
        df['rs_score'] = (df['rs_raw'].rank(pct=True) * 99).astype(int)
        
        # 2. SMR 등급 산정 (A-E) 
        # [수정] 스트림릿 에러 방지를 위해 컬럼명을 'smr_grade'로 변경
        df['smr_value'] = df['roe'] + df['margin'] + df['sales_growth']
        df['smr_grade'] = pd.qcut(df['smr_value'].rank(method='first'), 5, labels=['E', 'D', 'C', 'B', 'A'])
        
        # 3. 수급 등급 컬럼명 확인
        # 만약 앱에서 'acc_dist_rating'을 찾는다면 이름을 맞춰줘야 합니다. 
        # 여기서는 일단 'acc_dist'로 유지하되, 오류가 나면 'acc_dist_rating'으로 바꾸면 됩니다.
        
        # 4. 섹터별 RS 평균 점수 (기존과 동일)
        sector_avg = df.groupby('sector')['rs_score'].mean().reset_index()
        sector_avg.columns = ['sector', 'industry_rs_score']
        
        # 데이터 병합
        final_df = pd.merge(df, sector_avg, on='sector', how='left')
        final_df['industry_rs_score'] = final_df['industry_rs_score'].fillna(0).astype(int)

        # DB 저장
        conn = sqlite3.connect('ibd_system.db')
        final_df.to_sql('repo_results', conn, if_exists='replace', index=False)
        conn.close()
        print(f"[{datetime.now()}] 업데이트 완료! smr_grade 컬럼 생성 성공.")

if __name__ == "__main__":
    update_database()
