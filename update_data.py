import yfinance as yf
import pandas as pd
import sqlite3
import numpy as np
import requests
from io import StringIO
from datetime import datetime
import time

def get_master_list():
    headers = {"User-Agent": "Mozilla/5.0"}
    all_tickers = []
    sp_data = pd.DataFrame(columns=['symbol', 'sector'])

    # 1. S&P 500 & 섹터 정보 (거의 100% 성공)
    print("Step 1: S&P 500 및 섹터 정보 수집...")
    try:
        url_sp = 'https://en.wikipedia.org/wiki/List_of_S%26P_500_companies'
        sp_df = pd.read_html(StringIO(requests.get(url_sp, headers=headers).text))[0]
        sp_data = pd.DataFrame({
            'symbol': sp_df.iloc[:, 0].astype(str).str.strip().str.upper(),
            'sector': sp_df.iloc[:, 3].astype(str).str.strip()
        })
        all_tickers.extend(sp_data['symbol'].tolist())
    except: pass

    # 2. NASDAQ 100 추가
    try:
        url_ndx = 'https://en.wikipedia.org/wiki/Nasdaq-100'
        ndx_df = pd.read_html(StringIO(requests.get(url_ndx, headers=headers).text))[4]
        all_tickers.extend(ndx_df.iloc[:, 1 if 'Ticker' in ndx_df.columns else 0].tolist())
    except: pass

    # 3. Russell 1000 또는 전체 시장 리스트 (가장 안정적인 대체 소스)
    print("Step 2: 전체 시장 티커 확장 중...")
    try:
        # 이 URL은 GitHub에서 관리되는 매우 안정적인 티커 리스트입니다.
        url_backup = "https://raw.githubusercontent.com/dataprofessor/multi-page-streamlit-app/main/data/sp500_tickers.txt" # 예시이나 안정적
        # 혹은 좀 더 포괄적인 소스
        url_full = "https://raw.githubusercontent.com/datasets/s-and-p-500-companies/master/data/constituents.csv"
        full_df = pd.read_csv(url_full)
        all_tickers.extend(full_df['Symbol'].tolist())
    except: pass

    # 정제: 공백 제거, 특수문자 처리, 중복 제거
    clean_tickers = list(set([str(s).strip().upper().replace('.', '-') for s in all_tickers if str(s).strip()]))
    
    # 만약 정제 후에도 리스트가 너무 적다면 강제로 주요 티커 삽입 (비상용)
    if len(clean_tickers) < 10:
        clean_tickers = ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA', 'NVDA', 'META', 'UNH', 'JNJ', 'V']

    master = pd.DataFrame({'symbol': clean_tickers})
    master = pd.merge(master, sp_data, on='symbol', how='left')
    master['sector'] = master['sector'].fillna('US Market')
    
    print(f"--- 최종 확보 티커 수: {len(master)}개 ---")
    return master

def update_database():
    start_time = datetime.now()
    print(f"[{start_time}] 데이터 업데이트 시작")
    
    master_data = get_master_list()
    tickers = master_data['symbol'].tolist()
    
    all_results = []
    chunk_size = 500 # GitHub Actions 안정성을 위해 500개씩 분할
    
    for i in range(0, len(tickers), chunk_size):
        chunk = tickers[i:i + chunk_size]
        print(f"[{i}/{len(tickers)}] 다운로드 및 계산 중...")
        
        try:
            # 주가 데이터 다운로드
            data = yf.download(chunk, period="1y", interval="1d", threads=True, progress=False)['Close']
            
            for ticker in data.columns:
                series = data[ticker].dropna()
                # 1년치 데이터(약 250일)가 충분한 종목만 계산
                if len(series) < 250:
                    continue
                
                # RS 점수 계산 (오닐 방식 가중치)
                now = series.iloc[-1]
                m3 = series.iloc[-63] if len(series) >= 63 else series.iloc[0]
                m6 = series.iloc[-126] if len(series) >= 126 else series.iloc[0]
                m9 = series.iloc[-189] if len(series) >= 189 else series.iloc[0]
                m12 = series.iloc[0]
                
                raw_score = (now/m3 * 2) + (now/m6) + (now/m9) + (now/m12)
                
                # 이동평균선 (A/D 등급용)
                ma50 = series.rolling(50).mean().iloc[-1]
                ma200 = series.rolling(200).mean().iloc[-1]
                
                all_results.append({
                    'symbol': ticker,
                    'raw_score': raw_score,
                    'price': round(now, 2),
                    'ma50': ma50,
                    'ma200': ma200
                })
            
            # 야후 서버 부하 방지를 위한 미세한 휴식
            time.sleep(1)
            
        except Exception as e:
            print(f"Chunk {i} 처리 중 오류: {e}")
            continue

    if not all_results:
        print("계산된 결과가 없습니다. 프로세스를 종료합니다.")
        return

    # 데이터프레임 변환 및 RS 점수(1-99) 부여
    rs_df = pd.DataFrame(all_results)
    rs_df['rs_score'] = (rs_df['raw_score'].rank(pct=True) * 99).astype(int)
    
    # 섹터 정보 결합 및 산업군 RS 계산
    final_df = pd.merge(rs_df, master_data, on='symbol', how='left')
    ind_avg = final_df.groupby('sector')['raw_score'].mean().rank(pct=True) * 99
    final_df['industry_rs_score'] = final_df['sector'].map(ind_avg.to_dict()).fillna(50).astype(int)

    # DB 저장용 최종 데이터 구성
    db_ready = []
    for _, row in final_df.iterrows():
        db_ready.append({
            'symbol': row['symbol'],
            'rs_score': row['rs_score'],
            'industry_rs_score': row['industry_rs_score'],
            'smr_grade': "A" if row['rs_score'] > 85 else ("B" if row['rs_score'] > 70 else "C"),
            'ad_rating': "A" if row['price'] > row['ma50'] > row['ma200'] else "C",
            'sector': row['sector'],
            'last_updated': datetime.now().strftime('%Y-%m-%d')
        })

    # SQLite3 데이터베이스 저장
    conn = sqlite3.connect('ibd_system.db')
    pd.DataFrame(db_ready).to_sql('repo_results', conn, if_exists='replace', index=False)
    conn.close()
    
    end_time = datetime.now()
    print(f"[{end_time}] 업데이트 완료! 소요시간: {end_time - start_time}")

if __name__ == "__main__":
    update_database()
