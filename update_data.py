import yfinance as yf
import pandas as pd
import sqlite3
import time
from datetime import datetime
import os

def get_sector_master_map():
    """섹터 'Unknown' 방지를 위한 마스터 데이터 로드"""
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

def update_database():
    if not os.path.exists('tickers.txt'): return
    
    sector_master = get_sector_master_map()
    with open('tickers.txt', 'r') as f:
        tickers = [line.strip().upper().replace('.', '-') for line in f if line.strip()]
    
    all_results = []
    chunk_size = 30 # 차단 방지를 위해 청크 사이즈를 약간 줄임
    
    print(f"--- IBD SMR 강화 시스템 시작 ({datetime.now()}) ---")

    for i in range(0, len(tickers), chunk_size):
        chunk = tickers[i:i + chunk_size]
        try:
            # 1. 가격 데이터는 벌크로 빠르게 가져옴
            data = yf.download(chunk, period="1y", interval="1d", progress=False, group_by='ticker', threads=True)
            
            for ticker in chunk:
                try:
                    if ticker not in data.columns.get_level_values(0): continue
                    hist = data[ticker].dropna()
                    if len(hist) < 150: continue

                    # RS & Acc/Dist 계산
                    now_price = hist['Close'].iloc[-1]
                    ad_rating = calculate_acc_dist_rating(hist)
                    close = hist['Close']
                    rs_raw = (now_price/close.iloc[-63]*2) + (now_price/close.iloc[-126]) + (now_price/close.iloc[-189]) + (now_price/close.iloc[0])

                    # 2. [핵심] 재무 데이터(SMR) 수집
                    # 섹터는 우선 마스터에서 가져와서 'Unknown' 방지
                    sector = sector_master.get(ticker, "Unknown")
                    roe, margin, growth = 0, 0, 0

                    try:
                        t_obj = yf.Ticker(ticker)
                        info = t_obj.info
                        
                        # SMR 필수 데이터 추출
                        roe = info.get('returnOnEquity', 0)
                        margin = info.get('profitMargins', 0)
                        growth = info.get('revenueGrowth', 0)
                        
                        # 마스터에 섹터가 없었다면 여기서 보완
                        if sector == "Unknown":
                            sector = info.get('sector', 'Unknown')
                    except:
                        # API 차단 시 잠시 대기
                        time.sleep(1) 

                    all_results.append({
                        'symbol': ticker, 'price': round(now_price, 2), 'rs_raw': rs_raw,
                        'roe': roe if roe else 0, 'margin': margin if margin else 0,
                        'sales_growth': growth if growth else 0,
                        'ad_rating': ad_rating, 'sector': sector
                    })
                except: continue
            
            print(f" > {i+chunk_size}개 처리 중... (Unknown 섹터 방어 중)")
            time.sleep(2.5) # API 부하 분산

        except Exception as e:
            print(f"Error: {e}")
            time.sleep(5)

    if all_results:
        df = pd.DataFrame(all_results)
        
        # RS 및 SMR 등급 계산
        df['rs_score'] = (df['rs_raw'].rank(pct=True) * 98 + 1).astype(int)
        
        # SMR 등급 (ROE, Margin, Growth 합산 평가)
        df['smr_val'] = df['roe'].rank(pct=True) + df['margin'].rank(pct=True) + df['sales_growth'].rank(pct=True)
        df['smr_grade'] = pd.qcut(df['smr_val'].rank(method='first'), 5, labels=['E', 'D', 'C', 'B', 'A'])
        
        # 산업군 RS 계산
        sector_avg = df.groupby('sector')['rs_score'].mean().reset_index()
        sector_avg['industry_rs_score'] = (sector_avg['rs_score'].rank(pct=True) * 98 + 1).astype(int)
        
        final_df = pd.merge(df, sector_avg[['sector', 'industry_rs_score']], on='sector', how='left')

        # DB 저장
        conn = sqlite3.connect('ibd_system.db')
        final_df[['symbol', 'price', 'rs_score', 'smr_grade', 'ad_rating', 'industry_rs_score', 'sector']].to_sql('repo_results', conn, if_exists='replace', index=False)
        conn.close()
        print(f"--- 업데이트 완료: {len(final_df)} 종목 ---")

if __name__ == "__main__":
    update_database()
