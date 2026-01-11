import yfinance as yf
import pandas as pd
import sqlite3
import time
from datetime import datetime
import os
import numpy as np

def calculate_acc_dist_rating(hist):
    """최근 65거래일 수급(A~E) 등급 계산"""
    if len(hist) < 20: return 'C'
    df = hist.iloc[-65:].copy()
    df['Price_Change'] = df['Close'].diff()
    up_vol = df[df['Price_Change'] > 0]['Volume'].sum()
    down_vol = df[df['Price_Change'] < 0]['Volume'].sum()
    if down_vol <= 0: return 'C'
    ratio = up_vol / down_vol
    if ratio >= 1.5: return 'A'
    elif ratio >= 1.2: return 'B'
    elif ratio >= 0.9: return 'C'
    elif ratio >= 0.7: return 'D'
    else: return 'E'

def get_sector_safe(ticker_obj, symbol):
    """섹터 정보를 가져오기 위해 다각도로 시도"""
    # 1차 시도: info API
    try:
        s = ticker_obj.info.get('sector')
        if s and s != 'Unknown': return s
    except: pass

    # 2차 시도: fast_info (info 누락 시 대안)
    try:
        s = ticker_obj.fast_info.get('sector')
        if s and s != 'Unknown': return s
    except: pass

    # 3차 시도: 짧은 대기 후 재시도
    try:
        time.sleep(0.3)
        s = yf.Ticker(symbol).info.get('sector')
        if s: return s
    except: pass
    
    return 'Unknown'

def update_database():
    if not os.path.exists('tickers.txt'):
        print("tickers.txt 파일이 없습니다.")
        return

    with open('tickers.txt', 'r') as f:
        tickers = [line.strip().upper() for line in f if line.strip()]
    
    all_results = []
    chunk_size = 40 
    
    print(f"--- IBD 시스템 분석 및 강화 업데이트 시작 ({datetime.now()}) ---")

    for i in range(0, len(tickers), chunk_size):
        chunk = tickers[i:i + chunk_size]
        try:
            # 벌크 다운로드
            data = yf.download(chunk, period="1y", interval="1d", progress=False, group_by='ticker', threads=True)
            
            for ticker in chunk:
                try:
                    if ticker not in data.columns.get_level_values(0): continue
                    hist = data[ticker].dropna()
                    if len(hist) < 200: continue

                    now_price = hist['Close'].iloc[-1]
                    ad_rating = calculate_acc_dist_rating(hist)

                    # RS 원천 점수 계산
                    close = hist['Close']
                    m3, m6, m9, m12 = close.iloc[-63], close.iloc[-126], close.iloc[-189], close.iloc[0]
                    rs_raw = (now_price/m3 * 2) + (now_price/m6) + (now_price/m9) + (now_price/m12)

                    # [강화] 섹터 및 재무 데이터 정밀 수집
                    t_obj = yf.Ticker(ticker)
                    sector = get_sector_safe(t_obj, ticker)
                    
                    try:
                        info = t_obj.info
                        roe = info.get('returnOnEquity', 0)
                        margin = info.get('profitMargins', 0)
                        growth = info.get('revenueGrowth', 0)
                    except:
                        roe, margin, growth = 0, 0, 0

                    all_results.append({
                        'symbol': ticker,
                        'price': round(now_price, 2),
                        'rs_raw': rs_raw,
                        'roe': roe if roe is not None else 0,
                        'margin': margin if margin is not None else 0,
                        'sales_growth': growth if growth is not None else 0,
                        'ad_rating': ad_rating,
                        'sector': sector
                    })
                    print(f" > {ticker}: {sector} 분석 완료")
                except Exception as e:
                    continue
        except Exception as e:
            print(f"Chunk error: {e}")
            continue
            
        # 서버 차단 방지를 위한 유휴 시간
        time.sleep(2.0)

    if all_results:
        df = pd.DataFrame(all_results)
        
        # 1. RS 점수 (상대평가)
        df['rs_score'] = (df['rs_raw'].rank(pct=True) * 98 + 1).fillna(1).astype(int)
        
        # 2. SMR 고도화 (백분위 랭킹 합산)
        df['roe_rank'] = df['roe'].rank(pct=True)
        df['margin_rank'] = df['margin'].rank(pct=True)
        df['growth_rank'] = df['sales_growth'].rank(pct=True)
        
        df['smr_value'] = (df['roe_rank'] + df['margin_rank'] + df['growth_rank']) / 3
        # 데이터가 부족한 경우 대비 method='first' 사용
        df['smr_grade'] = pd.qcut(df['smr_value'].rank(method='first'), 5, labels=['E', 'D', 'C', 'B', 'A'])
        
        # 3. 산업군 RS (섹터별 평균 RS의 상대평가)
        sector_avg = df.groupby('sector')['rs_score'].mean().reset_index()
        sector_avg['industry_rs_score'] = (sector_avg['rs_score'].rank(pct=True) * 98 + 1).astype(int)
        sector_avg = sector_avg.drop(columns=['rs_score'])
        
        final_df = pd.merge(df, sector_avg, on='sector', how='left')
        final_df['industry_rs_score'] = final_df['industry_rs_score'].fillna(0).astype(int)

        # DB 저장
        conn = sqlite3.connect('ibd_system.db')
        # 대시보드에서 필요로 하는 컬럼들 저장
        final_df[['symbol', 'price', 'rs_score', 'smr_grade', 'ad_rating', 'industry_rs_score', 'sector']].to_sql('repo_results', conn, if_exists='replace', index=False)
        conn.close()
        
        print(f"\n--- 업데이트 완료 ({datetime.now()}) ---")
        print(f"총 분석 종목: {len(final_df)}개")
        print(f"Unknown 섹터 종목: {len(final_df[final_df['sector'] == 'Unknown'])}개")

if __name__ == "__main__":
    update_database()
