import yfinance as yf
import pandas as pd
import sqlite3
import time
from datetime import datetime, timedelta
import os
import requests
import io
import numpy as np

# --- [설정] FMP API 키 ---
FMP_API_KEY = "1kJBflGjsp5fCgbancejhI5bN5iavEJF"

def get_industry_master_map():
    industry_map = {}
    url2 = "https://raw.githubusercontent.com/yumoxu/stock-market-analysis/master/data/nasdaq_screener.csv"
    try:
        print("Loading Industry Map Source...")
        s = requests.get(url2).content
        df2 = pd.read_csv(io.StringIO(s.decode('utf-8')))
        if 'Symbol' in df2.columns and 'Industry' in df2.columns:
            df2['Symbol'] = df2['Symbol'].astype(str).str.strip().str.upper().str.replace('.', '-', regex=False)
            new_map = dict(zip(df2['Symbol'], df2['Industry']))
            for sym, ind in new_map.items():
                if isinstance(ind, str) and not pd.isna(ind): 
                    industry_map[sym] = ind
    except Exception as e:
        print(f"Warning: Industry Source 로드 실패 ({e})")
    return industry_map

def calculate_ad_raw(hist):
    if len(hist) < 65: return 0
    df = hist.copy()
    df['daily_return'] = df['Close'].pct_change()
    df['vol_50ma'] = df['Volume'].rolling(50).mean()
    df = df.dropna(subset=['daily_return', 'vol_50ma'])
    if len(df) < 65: return 0
    df = df.tail(65)
    df['ad_daily'] = (df['Volume'] / df['vol_50ma']) * df['daily_return'] * 100
    recent_20 = df['ad_daily'].tail(20).sum() * 0.7
    past_45 = df['ad_daily'].head(45).sum() * 0.3
    return recent_20 + past_45

def get_smr_data_fmp(ticker):
    """
    FMP API를 사용하여 정교한 SMR 데이터를 가져옵니다.
    무료 키 제한을 고려하여 예외 처리를 강화했습니다.
    """
    sales_accel, margin_accel, roe_accel = np.nan, np.nan, np.nan
    is_profitable = False
    
    try:
        # 1. 분기 손익계산서 (매출, 순이익)
        url_inc = f"https://financialmodelingprep.com/api/v3/income-statement/{ticker}?period=quarter&limit=4&apikey={FMP_API_KEY}"
        inc_res = requests.get(url_inc).json()
        
        if isinstance(inc_res, list) and len(inc_res) >= 3:
            rev = [x['revenue'] for x in inc_res]
            net = [x['netIncome'] for x in inc_res]
            
            # 매출 가속도 (최근 분기 성장률 - 이전 분기 성장률)
            g0 = (rev[0] - rev[1]) / abs(rev[1]) if rev[1] != 0 else 0
            g1 = (rev[1] - rev[2]) / abs(rev[2]) if rev[2] != 0 else 0
            sales_accel = g0 - g1
            
            # 마진 가속도
            m0 = net[0] / rev[0] if rev[0] != 0 else 0
            m1 = net[1] / rev[1] if rev[1] != 0 else 0
            margin_accel = m0 - m1
            
            # 수익성 확인
            if net[0] > 0: is_profitable = True

        # 2. 재무 비율 (ROE 등)
        url_ratio = f"https://financialmodelingprep.com/api/v3/ratios/{ticker}?period=quarter&limit=2&apikey={FMP_API_KEY}"
        ratio_res = requests.get(url_ratio).json()
        if isinstance(ratio_res, list) and len(ratio_res) > 0:
            roe_accel = ratio_res[0].get('returnOnEquity', np.nan)

    except Exception as e:
        print(f"FMP API Error for {ticker}: {e}")
        
    return sales_accel, margin_accel, roe_accel, is_profitable

def get_tickers():
    if os.path.exists('tickers.txt'):
        with open('tickers.txt', 'r') as f:
            tickers = [line.strip().upper().replace('.', '-') for line in f if line.strip()]
            return list(set(tickers))
    return ['AAPL', 'NVDA', 'MSFT', 'TSLA']

def update_database():
    tickers = get_tickers()
    industry_master = get_industry_master_map()
    all_results = []
    chunk_size = 50 
    
    print(f"--- IBD 시스템 시작 ({datetime.now()}) ---")

    for i in range(0, len(tickers), chunk_size):
        chunk = tickers[i:i + chunk_size]
        try:
            # 주가 데이터는 GitHub Actions IP를 이용해 yfinance로 대량 다운로드 (속도 향상)
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
                    # RS Raw 계산 (오닐 방식 가중치)
                    idx_21, idx_63, idx_126, idx_189, idx_252 = -21, -63, -126, -189, -min(252, len(hist))
                    rs_raw = (now_price / hist['Close'].iloc[idx_21] * 2) + \
                             (now_price / hist['Close'].iloc[idx_63] * 2) + \
                             (now_price / hist['Close'].iloc[idx_126]) + \
                             (now_price / hist['Close'].iloc[idx_189]) + \
                             (now_price / hist['Close'].iloc[idx_252])

                    ad_raw = calculate_ad_raw(hist)
                    adv_50 = (hist['Close'] * hist['Volume']).tail(50).mean()
                    industry = industry_master.get(ticker, "Unknown")
                    
                    # --- 핵심: FMP API 사용 (SMR 데이터) ---
                    # 무료 키 제한 때문에 상위 RS 종목 위주로 가져오거나 
                    # 여기서는 구조를 보여드리기 위해 매 종목 호출 시도 (한도 초과 시 nan 반환)
                    sales_acc, margin_acc, roe_acc, is_profitable = get_smr_data_fmp(ticker)
                    
                    # FMP 무료 키 속도 제한 준수 (약간의 대기)
                    time.sleep(0.1) 

                    all_results.append({
                        'symbol': ticker, 'price': float(now_price), 'rs_raw': rs_raw,
                        'ad_raw': ad_raw, 'adv_50': adv_50, 'mcap': 1.0, # 시총은 필요시 FMP에서 추가 가능
                        'sales_acc': sales_acc, 'margin_acc': margin_acc, 
                        'pretax_acc': np.nan, 'roe_acc': roe_acc,
                        'is_profitable': is_profitable,
                        'industry': industry
                    })
                except Exception: continue

            print(f" > {min(i+chunk_size, len(tickers))} / {len(tickers)} 분석 중...")
        except Exception as e:
            print(f"Chunk Error: {e}")

    if all_results:
        df = pd.DataFrame(all_results)
        # 등급 매기기
        df['rs_score'] = (df['rs_raw'].rank(pct=True) * 98 + 1).fillna(0).astype(int)
        df['ad_grade'] = pd.qcut(df['ad_raw'].rank(method='first'), 5, labels=['E', 'D', 'C', 'B', 'A'])
        
        # SMR 점수 계산
        df['smr_val'] = (df['sales_acc'].rank(pct=True, na_option='bottom').fillna(0) * 0.5) + \
                        (df['margin_acc'].rank(pct=True, na_option='bottom').fillna(0) * 0.3) + \
                        (df['roe_acc'].rank(pct=True, na_option='bottom').fillna(0) * 0.2)
        df.loc[df['is_profitable'] == False, 'smr_val'] -= 1.0
        df['smr_grade'] = pd.qcut(df['smr_val'].rank(method='first'), 5, labels=['E', 'D', 'C', 'B', 'A'])
        
        # 산업군 RS 계산
        industry_data = df.groupby('industry')['rs_raw'].mean().reset_index(name='ind_rs_raw')
        industry_data['industry_rs_score'] = (industry_data['ind_rs_raw'].rank(pct=True) * 98 + 1).fillna(0).astype(int)
        final_df = pd.merge(df, industry_data[['industry', 'industry_rs_score']], on='industry', how='left')

        # 데이터베이스 저장
        conn = sqlite3.connect('ibd_system.db')
        try:
            save_cols = ['symbol', 'price', 'rs_score', 'smr_grade', 'ad_grade', 'industry_rs_score', 'industry', 'adv_50']
            final_df[save_cols].to_sql('repo_results', conn, if_exists='replace', index=False)
            
            # 히스토리 저장
            today_str = datetime.now().strftime('%Y-%m-%d')
            history_df = final_df[['symbol', 'rs_score', 'industry_rs_score']].copy()
            history_df['date'] = today_str
            history_df.to_sql('rs_history', conn, if_exists='append', index=False)
            conn.execute("VACUUM")
        except Exception as e:
            print(f"DB Save Error: {e}")
        finally:
            conn.close()
            print(f"--- 업데이트 완료 ({datetime.now()}) ---")

if __name__ == "__main__":
    update_database()
