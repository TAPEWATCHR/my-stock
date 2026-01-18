import yfinance as yf
import pandas as pd
import sqlite3
import time
from datetime import datetime
import os
import requests
import io

def get_sector_master_map():
    """
    섹터 데이터를 여러 소스에서 로드하여 병합합니다.
    소스 1이 실패하면 소스 2에서 찾는 방식으로 커버리지를 높입니다.
    """
    sector_map = {}
    
    # --- 소스 1: 기존 GitHub 데이터 ---
    url1 = "https://raw.githubusercontent.com/rreichel3/US-Stock-Symbols/main/all_tickers.csv"
    try:
        print("Loading Sector Map Source 1...")
        df1 = pd.read_csv(url1)
        # 심볼 정규화 (공백 제거, 대문자, .을 -로 변경)
        df1['Symbol'] = df1['Symbol'].astype(str).str.strip().str.upper().str.replace('.', '-', regex=False)
        sector_map.update(dict(zip(df1['Symbol'], df1['Sector'])))
    except Exception as e:
        print(f"Warning: Source 1 로드 실패 ({e})")

    # --- 소스 2: NASDAQ Screener 백업 데이터 (섹터 정보가 풍부함) ---
    url2 = "https://raw.githubusercontent.com/yumoxu/stock-market-analysis/master/data/nasdaq_screener.csv"
    try:
        print("Loading Sector Map Source 2...")
        s = requests.get(url2).content
        df2 = pd.read_csv(io.StringIO(s.decode('utf-8')))
        
        # 컬럼명이 다를 수 있으므로 확인
        if 'Symbol' in df2.columns and 'Sector' in df2.columns:
            df2['Symbol'] = df2['Symbol'].astype(str).str.strip().str.upper().str.replace('.', '-', regex=False)
            # 기존 맵에 없는 것만 추가 (Source 1 우선, 없으면 Source 2)
            new_map = dict(zip(df2['Symbol'], df2['Sector']))
            for sym, sec in new_map.items():
                if sym not in sector_map or pd.isna(sector_map[sym]):
                    if isinstance(sec, str): # 유효한 문자열 섹터만 저장
                        sector_map[sym] = sec
    except Exception as e:
        print(f"Warning: Source 2 로드 실패 ({e})")
        
    print(f"Total Sector Map Size: {len(sector_map)} symbols")
    return sector_map

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
    if os.path.exists('tickers.txt'):
        with open('tickers.txt', 'r') as f:
            # 특수문자 제거 및 정규화 강화
            tickers = [line.strip().upper().replace('.', '-') for line in f if line.strip()]
            return list(set(tickers)) # 중복 제거
    else:
        print("Warning: 'tickers.txt' not found. Using sample tickers.")
        return ['AAPL', 'NVDA', 'MSFT', 'TSLA']

def fetch_info_with_retry(ticker_obj, retries=2):
    """
    yfinance info 호출이 실패할 경우 재시도하는 헬퍼 함수
    """
    for attempt in range(retries + 1):
        try:
            info = ticker_obj.info
            if info and 'sector' in info:
                return info
            if attempt < retries:
                time.sleep(1) # 실패 시 1초 대기 후 재시도
        except:
            if attempt < retries:
                time.sleep(1)
            else:
                return None
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
            # 멀티스레드 다운로드
            data = yf.download(chunk, period="1y", interval="1d", progress=False, group_by='ticker', threads=True)
            
            if data.empty:
                print(f" > {i}~{i+chunk_size}: 데이터 없음")
                continue

            for ticker in chunk:
                try:
                    # 데이터 프레임 추출
                    if len(chunk) > 1:
                        if ticker not in data.columns.get_level_values(0): continue
                        hist = data[ticker].dropna()
                    else:
                        hist = data.dropna()

                    if len(hist) < 150: continue

                    now_price = hist['Close'].iloc[-1]
                    ad_rating = calculate_acc_dist_rating(hist)
                    close = hist['Close']
                    rs_raw = (now_price/close.iloc[-63]*2) + (now_price/close.iloc[-126]) + (now_price/close.iloc[-189]) + (now_price/close.iloc[0])

                    # --- 섹터 및 재무 데이터 처리 ---
                    # 1차 시도: 마스터 맵에서 조회
                    sector = sector_master.get(ticker, "Unknown")
                    roe, margin, growth = 0, 0, 0
                    
                    # 맵에 없거나 재무 데이터가 필요한 경우 yfinance 호출
                    # (Unknown인 경우에만 info를 호출하거나, 재무 데이터 수집을 위해 호출)
                    try:
                        t_obj = yf.Ticker(ticker)
                        # 섹터가 Unknown이면 재시도 로직을 통해 꼼꼼히 찾음
                        if sector == "Unknown":
                            info = fetch_info_with_retry(t_obj, retries=2)
                        else:
                            # 섹터를 이미 알면 한 번만 시도 (재무 데이터용)
                            info = t_obj.info
                        
                        if info:
                            roe = info.get('returnOnEquity', 0)
                            margin = info.get('profitMargins', 0)
                            growth = info.get('revenueGrowth', 0)
                            
                            # API에서 섹터 정보를 찾았다면 업데이트
                            api_sector = info.get('sector')
                            if api_sector:
                                sector = api_sector

                    except Exception:
                        pass # API 실패해도 가격 데이터는 저장

                    # 최종적으로도 Unknown이면 'Other' 등으로 분류하거나 유지
                    if pd.isna(sector) or sector == "nan":
                        sector = "Unknown"

                    all_results.append({
                        'symbol': ticker, 'price': float(now_price), 'rs_raw': rs_raw,
                        'roe': roe if roe else 0, 'margin': margin if margin else 0,
                        'sales_growth': growth if growth else 0,
                        'ad_rating': ad_rating, 'sector': sector
                    })
                except Exception as inner_e:
                    continue 

            print(f" > {min(i+chunk_size, len(tickers))} / {len(tickers)} 완료 | 최근 섹터 예시: {sector}")
            time.sleep(1) # 청크 간 딜레이 (API 보호)

        except Exception as e:
            print(f"Chunk Error: {e}")
            time.sleep(5)

    # 저장 로직 (이전과 동일, 안전장치 추가)
    if all_results:
        try:
            df = pd.DataFrame(all_results)
            
            # 섹터가 여전히 Unknown인 비율 확인
            unknown_count = len(df[df['sector'] == 'Unknown'])
            print(f"--- 분석 완료: 총 {len(df)}개 중 Unknown 섹터: {unknown_count}개 ---")

            df['rs_score'] = (df['rs_raw'].rank(pct=True) * 98 + 1).fillna(0).astype(int)
            
            df['smr_val'] = df['roe'].rank(pct=True) + df['margin'].rank(pct=True) + df['sales_growth'].rank(pct=True)
            df['smr_grade'] = pd.qcut(df['smr_val'].rank(method='first'), 5, labels=['E', 'D', 'C', 'B', 'A'])
            
            # 섹터별 평균 계산 시 Unknown은 제외하거나 별도 처리 가능
            sector_avg = df.groupby('sector')['rs_score'].mean().reset_index()
            sector_avg['industry_rs_score'] = (sector_avg['rs_score'].rank(pct=True) * 98 + 1).fillna(0).astype(int)
            
            final_df = pd.merge(df, sector_avg[['sector', 'industry_rs_score']], on='sector', how='left')
            
            # 결측치 0 처리
            final_df['industry_rs_score'] = final_df['industry_rs_score'].fillna(0).astype(int)

            conn = sqlite3.connect('ibd_system.db')
            final_df[['symbol', 'price', 'rs_score', 'smr_grade', 'ad_rating', 'industry_rs_score', 'sector']].to_sql('repo_results', conn, if_exists='replace', index=False)
            conn.close()
            print("--- DB 저장 완료 ---")
        except Exception as db_e:
            print(f"DB 저장 에러: {db_e}")
    else:
        print("--- 결과 데이터가 없습니다. ---")

if __name__ == "__main__":
    update_database()
