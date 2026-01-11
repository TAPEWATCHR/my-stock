import yfinance as yf
import pandas as pd
import sqlite3
import time
from datetime import datetime

def update_database():
    with open('tickers.txt', 'r') as f:
        tickers = [line.strip().upper() for line in f if line.strip()]
    
    all_results = []
    chunk_size = 30 # 재무 데이터까지 가져오므로 청크를 더 작게 조절
    
    print(f"--- SMR + 수급 분석 시작: {len(tickers)}개 ---")

    for i in range(0, len(tickers), chunk_size):
        chunk = tickers[i:i + chunk_size]
        print(f"[{i}/{len(tickers)}] 상세 분석 중...")
        
        # 1. 가격 및 거래량 데이터 한 번에 다운로드 (수급 분석용)
        group = yf.Tickers(' '.join(chunk))
        
        for ticker in chunk:
            try:
                t_obj = group.tickers[ticker]
                hist = t_obj.history(period="1y")
                if len(hist) < 200: continue

                # --- [수급 분석] ---
                # 최근 20일 평균 거래량 vs 1년 평균 거래량 비교
                avg_vol = hist['Volume'].mean()
                curr_vol = hist['Volume'].iloc[-20:].mean()
                supply_demand = round((curr_vol / avg_vol) * 100, 2)

                # --- [RS 점수 계산] ---
                close = hist['Close']
                now = close.iloc[-1]
                m3, m6, m9, m12 = close.iloc[-63], close.iloc[-126], close.iloc[-189], close.iloc[0]
                rs_raw = (now/m3 * 2) + (now/m6) + (now/m9) + (now/m12)

                # --- [SMR 분석 (재무)] ---
                info = t_obj.info
                # Sales Growth(매출성장), Profit Margin(이익률), ROE(자기자본이익률)
                roe = info.get('returnOnEquity', 0) * 100
                margin = info.get('profitMargins', 0) * 100
                sales_growth = info.get('revenueGrowth', 0) * 100
                sector = info.get('sector', 'Unknown')

                all_results.append({
                    'symbol': ticker,
                    'price': round(now, 2),
                    'rs_raw': rs_raw,
                    'roe': round(roe, 2),
                    'margin': round(margin, 2),
                    'sales_growth': round(sales_growth, 2),
                    'supply_demand': supply_demand,
                    'sector': sector
                })
            except: continue
        time.sleep(1)

    if all_results:
        df = pd.DataFrame(all_results)
        
        # 점수 정규화 (1-99)
        df['rs_score'] = (df['rs_raw'].rank(pct=True) * 99).astype(int)
        
        # SMR 등급 산정 로직 (단순화: ROE와 Margin 기준)
        # 실제 IBD는 복잡하지만, 여기서는 상위 20%에게 A등급 부여 방식
        df['smr_rating'] = pd.qcut(df['roe'] + df['margin'], 5, labels=['E', 'D', 'C', 'B', 'A'])
        
        conn = sqlite3.connect('ibd_system.db')
        df.to_sql('repo_results', conn, if_exists='replace', index=False)
        conn.close()
        print(f"업데이트 완료! SMR 및 수급 데이터가 반영되었습니다.")

if __name__ == "__main__":
    update_database()
