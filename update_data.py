import yfinance as yf
import pandas as pd
import sqlite3
import time
from datetime import datetime

def calculate_acc_dist_rating(hist):
    """
    IBD 스타일의 Accumulation/Distribution (수급) 등급 계산
    주가 상승일 거래량 합산 vs 주가 하락일 거래량 합산 비교
    """
    if len(hist) < 20: return 'C'
    
    # 최근 13주(65일) 데이터 사용
    df = hist.iloc[-65:].copy()
    df['Price_Change'] = df['Close'].diff()
    
    # 상승한 날의 거래량(매수 강도) vs 하락한 날의 거래량(매도 강도)
    up_vol = df[df['Price_Change'] > 0]['Volume'].sum()
    down_vol = df[df['Price_Change'] < 0]['Volume'].sum()
    
    if down_vol == 0: return 'A'
    ratio = up_vol / down_vol
    
    # 비율에 따른 등급 부여 (일반적인 IBD 기준 우회 적용)
    if ratio >= 1.5: return 'A'
    elif ratio >= 1.2: return 'B'
    elif ratio >= 0.9: return 'C'
    elif ratio >= 0.7: return 'D'
    else: return 'E'

def update_database():
    with open('tickers.txt', 'r') as f:
        tickers = [line.strip().upper() for line in f if line.strip()]
    
    all_results = []
    chunk_size = 20 # 정밀 분석을 위해 청크 크기 축소
    
    print(f"--- SMR + 수급(A/D) 정밀 분석 시작 ---")

    for i in range(0, len(tickers), chunk_size):
        chunk = tickers[i:i + chunk_size]
        print(f"[{i}/{len(tickers)}] 수급 및 재무 데이터 분석 중...")
        
        for ticker in chunk:
            try:
                t_obj = yf.Ticker(ticker)
                hist = t_obj.history(period="1y")
                if len(hist) < 200: continue

                # 1. 수급 등급 계산 (누적 매수/매도 비교)
                acc_dist_rating = calculate_acc_dist_rating(hist)

                # 2. RS 점수 계산
                close = hist['Close']
                now = close.iloc[-1]
                m3, m6, m9, m12 = close.iloc[-63], close.iloc[-126], close.iloc[-189], close.iloc[0]
                rs_raw = (now/m3 * 2) + (now/m6) + (now/m9) + (now/m12)

                # 3. SMR 재무 데이터 (Sales, Margin, ROE)
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
                    'acc_dist': acc_dist_rating, # 수급 등급 추가
                    'sector': sector
                })
            except: continue
        time.sleep(1) # 차단 방지

    if all_results:
        df = pd.DataFrame(all_results)
        
        # RS 점수 정규화
        df['rs_score'] = (df['rs_raw'].rank(pct=True) * 99).astype(int)
        
        # SMR 등급 계산 (A~E)
        df['smr_value'] = df['roe'] + df['margin'] + df['sales_growth']
        df['smr_rating'] = pd.qcut(df['smr_value'].rank(method='first'), 5, labels=['E', 'D', 'C', 'B', 'A'])
        
        conn = sqlite3.connect('ibd_system.db')
        # 최종적으로 스트림릿에서 요구하는 모든 컬럼 저장
        df.to_sql('repo_results', conn, if_exists='replace', index=False)
        conn.close()
        print(f"성공! 총 {len(df)}개 종목의 IBD 종합 지표(SMR, Acc/Dist) 업데이트 완료.")

if __name__ == "__main__":
    update_database()
