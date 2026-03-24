import yfinance as yf
import pandas as pd
import sqlite3
import time
from datetime import datetime, timedelta
import os
import requests
import io

def get_sector_master_map():
    sector_map = {}
    url1 = "https://raw.githubusercontent.com/rreichel3/US-Stock-Symbols/main/all_tickers.csv"
    try:
        print("Loading Sector Map Source 1...")
        df1 = pd.read_csv(url1)
        df1['Symbol'] = df1['Symbol'].astype(str).str.strip().str.upper().str.replace('.', '-', regex=False)
        sector_map.update(dict(zip(df1['Symbol'], df1['Sector'])))
    except Exception as e:
        print(f"Warning: Source 1 로드 실패 ({e})")

    url2 = "https://raw.githubusercontent.com/yumoxu/stock-market-analysis/master/data/nasdaq_screener.csv"
    try:
        print("Loading Sector Map Source 2...")
        s = requests.get(url2).content
        df2 = pd.read_csv(io.StringIO(s.decode('utf-8')))
        if 'Symbol' in df2.columns and 'Sector' in df2.columns:
            df2['Symbol'] = df2['Symbol'].astype(str).str.strip().str.upper().str.replace('.', '-', regex=False)
            new_map = dict(zip(df2['Symbol'], df2['Sector']))
            for sym, sec in new_map.items():
                if sym not in sector_map or pd.isna(sector_map[sym]):
                    if isinstance(sec, str): 
                        sector_map[sym] = sec
    except Exception as e:
        print(f"Warning: Source 2 로드 실패 ({e})")
        
    print(f"Total Sector Map Size: {len(sector_map)} symbols")
    return sector_map

def calculate_acc_dist_rating(hist):
    if len(hist) < 20: return 'C'
    df = hist.iloc[-65:].copy()
    high_low_range = df['High'] - df['Low']
    clv = ((df['Close'] - df['Low']) - (df['High'] - df['Close'])) / high_low_range.replace(0, 0.001)
    money_flow_volume = clv * df['Volume']
    total_volume = df['Volume'].sum()
    if total_volume == 0: return 'C'
    
    cmf = money_flow_volume.sum() / total_volume
    if cmf >= 0.15: return 'A'
    elif cmf >= 0.05: return 'B'
    elif cmf >= -0.05: return 'C'
    elif cmf >= -0.15: return 'D'
    else: return 'E'

def get_tickers():
    if os.path.exists('tickers.txt'):
        with open('tickers.txt', 'r') as f:
            tickers = [line.strip().upper().replace('.', '-') for line in f if line.strip()]
            return list(set(tickers))
    else:
        print("Warning: 'tickers.txt' not found. Using sample tickers.")
        return ['AAPL', 'NVDA', 'MSFT', 'TSLA']

def fetch_info_with_retry(ticker_obj, retries=2):
    for attempt in range(retries + 1):
        try:
            info = ticker_obj.info
            if info and 'sector' in info:
                return info
            if attempt < retries: time.sleep(1)
        except:
            if attempt < retries: time.sleep(1)
            else: return None
    return None

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
            data = yf.download(chunk, period="1y", interval="1d", progress=False, group_by='ticker', threads=True)
            if data.empty: continue

            for ticker in chunk:
                try:
                    if len(chunk) > 1:
                        if ticker not in data.columns.get_level_values(0): continue
                        hist = data[ticker].dropna()
                    else:
                        hist = data.dropna()

                    if len(hist) < 63: continue

                    now_price = hist['Close'].iloc[-1]
                    # [수정됨] 1개월(약 21일) 인덱스 추가 및 적용
                    idx_21 = -21 if len(hist) >= 21 else 0
                    idx_63 = -63 if len(hist) >= 63 else 0
                    idx_126 = -126 if len(hist) >= 126 else 0
                    idx_189 = -189 if len(hist) >= 189 else 0
                    idx_252 = -252 if len(hist) >= 252 else 0

                    # [수정됨] 1개월(idx_21) 수익률 추가 및 가중치 * 2 적용
                    rs_raw = (now_price / hist['Close'].iloc[idx_21] * 2) + \
                             (now_price / hist['Close'].iloc[idx_63] * 2) + \
                             (now_price / hist['Close'].iloc[idx_126]) + \
                             (now_price / hist['Close'].iloc[idx_189]) + \
                             (now_price / hist['Close'].iloc[idx_252])

                    ad_rating = calculate_acc_dist_rating(hist)
                    
                    # [추가됨] 50일 평균 거래대금(ADV) 계산
                    adv_50 = (hist['Close'] * hist['Volume']).tail(50).mean()

                    sector = sector_master.get(ticker, "Unknown")
                    roe, margin, growth = 0, 0, 0
                    
                    try:
                        t_obj = yf.Ticker(ticker)
                        if sector == "Unknown":
                            info = fetch_info_with_retry(t_obj, retries=2)
                        else:
                            info = t_obj.info
                        
                        if info:
                            roe = info.get('returnOnEquity', 0)
                            margin = info.get('profitMargins', 0)
                            growth = info.get('revenueGrowth', 0)
                            if info.get('sector'): sector = info.get('sector')
                    except Exception:
                        pass

                    if pd.isna(sector) or sector == "nan": sector = "Unknown"

                    # [수정됨] 딕셔너리에 adv_50 추가
                    all_results.append({
                        'symbol': ticker, 'price': float(now_price), 'rs_raw': rs_raw,
                        'roe': roe if roe else 0, 'margin': margin if margin else 0,
                        'sales_growth': growth if growth else 0,
                        'ad_rating': ad_rating, 'sector': sector,
                        'adv_50': adv_50
                    })
                except Exception as inner_e:
                    continue 

            print(f" > {min(i+chunk_size, len(tickers))} / {len(tickers)} 완료 | 최근 섹터 예시: {sector}")
            time.sleep(1) 

        except Exception as e:
            print(f"Chunk Error: {e}")
            time.sleep(5)

    if all_results:
        df = pd.DataFrame(all_results)
        df['rs_score'] = (df['rs_raw'].rank(pct=True) * 98 + 1).fillna(0).astype(int)
        df['smr_val'] = df['roe'].rank(pct=True) + df['margin'].rank(pct=True) + df['sales_growth'].rank(pct=True)
        df['smr_grade'] = pd.qcut(df['smr_val'].rank(method='first'), 5, labels=['E', 'D', 'C', 'B', 'A'])
        
        sector_avg = df.groupby('sector')['rs_raw'].mean().reset_index()
        sector_avg['industry_rs_score'] = (sector_avg['rs_raw'].rank(pct=True) * 98 + 1).fillna(0).astype(int)
        
        final_df = pd.merge(df, sector_avg[['sector', 'industry_rs_score']], on='sector', how='left')
        final_df['industry_rs_score'] = final_df['industry_rs_score'].fillna(0).astype(int)

        conn = sqlite3.connect('ibd_system.db')
        try:
            # [수정됨] repo_results 테이블에 adv_50 컬럼 추가 저장
            final_df[['symbol', 'price', 'rs_score', 'smr_grade', 'ad_rating', 'industry_rs_score', 'sector', 'adv_50']].to_sql('repo_results', conn, if_exists='replace', index=False)
            
            today_str = datetime.now().strftime('%Y-%m-%d')
            history_df = final_df[['symbol', 'rs_score']].copy()
            history_df['date'] = today_str
            history_df.to_sql('rs_history', conn, if_exists='append', index=False)
            
            print("--- 데이터베이스 최적화 및 정리 시작 ---")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_symbol ON rs_history (symbol)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_date ON rs_history (date)")
            one_year_ago = (datetime.now() - timedelta(days=365)).strftime('%Y-%m-%d')
            conn.execute(f"DELETE FROM rs_history WHERE date < '{one_year_ago}'")
            conn.execute("VACUUM")
            
            print(f"--- DB 저장 및 최적화 완벽 처리 완료 ({today_str}) ---")
            
        except Exception as db_e:
            print(f"DB 저장 및 최적화 에러: {db_e}")
        finally:
            conn.close()
    else:
        print("--- 결과 데이터가 없습니다. ---")

if __name__ == "__main__":
    update_database()
