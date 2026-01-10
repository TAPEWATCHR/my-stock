import yfinance as yf
import pandas as pd
import sqlite3
import numpy as np
import requests
from io import StringIO
from datetime import datetime
import time

def get_master_list():
    """나스닥 서버가 죽었을 때를 대비해 다른 안정적인 경로로 티커 수집"""
    headers = {"User-Agent": "Mozilla/5.0"}
    
    # 1. S&P 500 섹터 정보 (위키피디아는 매우 안정적임)
    sp_data = pd.DataFrame(columns=['symbol', 'sector'])
    try:
        url_sp = 'https://en.wikipedia.org/wiki/List_of_S%26P_500_companies'
        resp = requests.get(url_sp, headers=headers, timeout=10)
        sp_df = pd.read_html(StringIO(resp.text))[0]
        sp_data = pd.DataFrame({
            'symbol': sp_df.iloc[:, 0].astype(str).str.strip().str.upper(),
            'sector': sp_df.iloc[:, 3].astype(str).str.strip()
        })
    except: pass

    # 2. 전체 티커 확보 (전략 변경: 나스닥 서버 대신 다른 안정적인 소스 활용)
    all_tickers = []
    print("전체 티커 리스트 확보 시도 중...")
    try:
        # 방법 1: 안정적인 실시간 소스 (fmp cloud 기반 오픈 데이터 등)
        url = "https://raw.githubusercontent.com/rreichel3/US-Stock-Symbols/main/all_tickers.txt"
        resp = requests.get(url, timeout=10)
        if resp.status_code == 200:
            all_tickers = [s.strip().upper() for s in resp.text.split('\n') if s.strip()]
    except:
        print("백업 소스 시도...")
        # 방법 2: S&P 500 리스트라도 활용
        all_tickers = sp_data['symbol'].tolist()

    # 정제
    clean_tickers = []
    for s in all_tickers:
        if s and ' ' not in s and 1 <= len(s) <= 5:
            clean_tickers.append(s.replace('.', '-'))
            
    all_data = pd.DataFrame({'symbol': list(set(clean_tickers))})
    master = pd.merge(all_data, sp_data, on='symbol', how='left')
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
