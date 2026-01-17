import yfinance as yf
import pandas as pd
import sqlite3
import time
from datetime import datetime
import os

def get_sector_master_map():
    """섹터 데이터 로드"""
    url = "https://raw.githubusercontent.com/rreichel3/US-Stock-Symbols/main/all_tickers.csv"
    try:
        df_master = pd.read_csv(url)
        df_master['Symbol'] = df_master['Symbol'].str.strip().upper().str.replace('.', '-', regex=False)
        return dict(zip(df_master['Symbol'], df_master['Sector']))
    except:
        return {}

def calculate_acc_dist_rating(hist):
    if len(hist) < 20: return 'C'
    df = hist.iloc[-65:].copy()
    df['Price_Change'] = df['Close'].diff()
    up_vol = df[df['Price_Change'] > 0]['Volume'].sum()
    down_vol = df[df['Price_Change'] < 0]['Volume'].sum()
    if down_vol <= 0: return 'C'
    ratio = up_vol / down_vol
    return 'A' if ratio >= 1.5 else 'B' if ratio >= 1.2 else 'C' if ratio >= 0.9 else 'D' if ratio >= 0.7 else 'E'

def get_tickers():
    """tickers.txt가 없으면 샘플 티커를 반환하여 에러 방지"""
    if os.path.exists('tickers.txt'):
        with open('tickers.txt', 'r') as f:
            return [line.strip().upper().replace('.', '-') for line in f if line.strip()]
    else:
        print("Warning: 'tickers.txt' not found. Using sample tickers.")
        return ['AAPL', 'NVDA', 'MSFT', 'TSLA'] # 테스트용 샘플

def update_database():
    tickers = get_tickers()
    sector_master = get_sector_master_map()
    
    all_results = []
    chunk_size = 30 
    
    print(f"--- IBD SMR 강화 시스템 시작 ({datetime.now()}) ---")
    print(f"--- 총 {len(tickers)}개 종목 분석 예정 ---")

    for i in range(0, len(tickers), chunk_size):
        chunk = tickers[i:i + chunk_size]
        try:
            # yfinance 데이터 다운로드 (에러 무시 옵션 추가)
            data = yf.download(chunk, period="1y", interval="1d", progress=False, group_by='ticker', threads=True)
            
            if data.empty:
                print(f" > {i}~{i+chunk_size}: 데이터 없음, 건너뜀")
                continue

            for ticker in chunk:
                try:
                    # MultiIndex 컬럼 처리 (단일 티커일 때와 다를 수 있음)
                    if len(chunk) > 1:
                        if ticker not in data.columns.get_level_values(0): continue
                        hist = data[ticker].dropna()
                    else:
                        hist = data.dropna() # 단일 티커일 경우

                    if len(hist) < 150: continue

                    now_price = hist['Close'].iloc[-1]
                    ad_rating = calculate_acc_dist_rating(hist)
                    close = hist['Close']
                    rs_raw = (now_price/close.iloc[-63]*2) + (now_price/close.iloc[-126]) + (now_price/close.iloc[-189]) + (now_price/close.iloc[0])

                    # --- 재무 데이터(SMR) 수집 부분 (가장 에러가 잦은 곳) ---
                    sector = sector_master.get(ticker, "Unknown")
                    roe, margin, growth = 0, 0, 0

                    try:
                        # GitHub Action에서는 이 부분에서 차단될 확률이 높음 -> 예외처리 강화
                        t_obj = yf.Ticker(ticker)
                        info = t_obj.info
                        
                        if info: # info가 None이 아닐 때만
                            roe = info.get('returnOnEquity', 0)
                            margin = info.get('profitMargins', 0)
                            growth = info.get('revenueGrowth', 0)
                            if sector == "Unknown":
                                sector = info.get('sector', 'Unknown')
                    except Exception as e:
                        # 재무 데이터 못 가져와도 가격 데이터는 살림
                        pass 

                    all_results.append({
                        'symbol': ticker, 'price': float(now_price), 'rs_raw': rs_raw,
                        'roe': roe if roe else 0, 'margin': margin if margin else 0,
                        'sales_growth': growth if growth else 0,
                        'ad_rating': ad_rating, 'sector': sector
                    })
                except Exception as inner_e:
                    continue # 개별 종목 에러 시 다음 종목으로

            print(f" > {min(i+chunk_size, len(tickers))} / {len(tickers)} 처리 완료")
            time.sleep(2) # 딜레이

        except Exception as e:
            print(f"Chunk Error: {e}")
            time.sleep(5)

    # 결과 저장 로직
    if all_results:
        try:
            df = pd.DataFrame(all_results)
            df['rs_score'] = (df['rs_raw'].rank(pct=True) * 98 + 1).fillna(0).astype(int)
            
            df['smr_val'] = df['roe'].rank(pct=True) + df['margin'].rank(pct=True) + df['sales_growth'].rank(pct=True)
            df['smr_grade'] = pd.qcut(df['smr_val'].rank(method='first'), 5, labels=['E', 'D', 'C', 'B', 'A'])
            
            sector_avg = df.groupby('sector')['rs_score'].mean().reset_index()
            sector_avg['industry_rs_score'] = (sector_avg['rs_score'].rank(pct=True) * 98 + 1).fillna(0).astype(int)
            
            final_df = pd.merge(df, sector_avg[['sector', 'industry_rs_score']], on='sector', how='left')

            conn = sqlite3.connect('ibd_system.db')
            final_df[['symbol', 'price', 'rs_score', 'smr_grade', 'ad_rating', 'industry_rs_score', 'sector']].to_sql('repo_results', conn, if_exists='replace', index=False)
            conn.close()
            print(f"--- 업데이트 완료: {len(final_df)} 종목 저장됨 ---")
        except Exception as db_e:
            print(f"DB 저장 중 에러 발생: {db_e}")
    else:
        print("--- 결과 데이터가 없습니다. (tickers.txt 확인 필요) ---")

if __name__ == "__main__":
    update_database()
