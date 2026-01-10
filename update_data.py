import yfinance as yf
import pandas as pd
import sqlite3
import requests
from io import StringIO
from datetime import datetime
import time

def get_total_market_tickers():
    """모든 수단을 동원해 미국 시장 전체 티커 5,000개+ 확보"""
    headers = {"User-Agent": "Mozilla/5.0"}
    all_tickers = set()
    
    # 1. 나스닥 공식 FTP 데이터 (가장 정확하지만 차단이 잦음)
    print("Source 1: 나스닥 공식 서버 접속 중...")
    try:
        url = "https://tda.nasdaqtrader.com/dynamic/SymDir/nasdaqlisted.txt"
        res = requests.get(url, timeout=10)
        df = pd.read_csv(StringIO(res.text), sep='|')
        # 심볼만 추출 및 파일 정보 텍스트 제거
        valid_syms = df['Symbol'].dropna().astype(str).tolist()
        all_tickers.update([s for s in valid_syms if "File Creation Time" not in s])
    except:
        print("Source 1 실패. 다음 소스로 이동...")

    # 2. 대안 소스: GitHub에 관리되는 대규모 티커 리스트
    if len(all_tickers) < 3000:
        print("Source 2: 오픈 데이터 리포지토리 활용...")
        try:
            # 실시간으로 업데이트되는 8,000개 이상의 티커 CSV
            url_alt = "https://raw.githubusercontent.com/rreichel3/US-Stock-Symbols/main/all_tickers.txt"
            res_alt = requests.get(url_alt, timeout=10)
            all_tickers.update([s.strip().upper() for s in res_alt.text.split('\n') if s.strip()])
        except:
            print("Source 2 실패.")

    # 3. 데이터 정제: 특수문자나 기업명 오입력 방지
    clean_list = []
    for s in all_tickers:
        s = s.replace('.', '-')
        # 티커는 보통 1-5자, 공백 없음, 숫자만 있는 경우 제외
        if 1 <= len(s) <= 5 and s.replace('-', '').isalpha():
            clean_list.append(s)
    
    print(f"--- 최종 확보된 전체 티커 수: {len(clean_list)}개 ---")
    return list(set(clean_list))

def update_database():
    start_time = datetime.now()
    tickers = get_total_market_tickers()
    
    if len(tickers) < 3000:
        print(f"경고: 확보된 종목이 {len(tickers)}개로 너무 적습니다. 전체 수집에 실패했을 가능성이 큼.")
    
    all_results = []
    # 5,000개 이상이므로 야후 차단 방지를 위해 200개씩 신중하게 처리
    chunk_size = 200 
    
    for i in range(0, len(tickers), chunk_size):
        chunk = tickers[i:i + chunk_size]
        print(f"[{i}/{len(tickers)}] 처리 중...")
        
        try:
            # threads=False로 설정하여 야후의 공격적 감지 회피
            data = yf.download(chunk, period="1y", interval="1d", progress=False, threads=False)['Close']
            
            if isinstance(data, pd.Series): data = data.to_frame()

            for ticker in data.columns:
                series = data[ticker].dropna()
                if len(series) < 240: continue # 상장 1년 미만 제외
                
                now = series.iloc[-1]
                m3, m6, m9, m12 = series.iloc[-63], series.iloc[-126], series.iloc[-189], series.iloc[0]
                raw_score = (now/m3 * 2) + (now/m6) + (now/m9) + (now/m12)
                
                all_results.append({
                    'symbol': ticker, 'raw_score': raw_score, 'price': round(now, 2)
                })
        except: continue
        time.sleep(0.5) # 야후 서버에 숨 돌릴 시간 제공

    # 전체 종목 대비 RS 점수 산출 (1-99)
    if all_results:
        final_df = pd.DataFrame(all_results)
        final_df['rs_score'] = (final_df['raw_score'].rank(pct=True) * 99).astype(int)
        
        conn = sqlite3.connect('ibd_system.db')
        final_df[['symbol', 'rs_score', 'price']].to_sql('repo_results', conn, if_exists='replace', index=False)
        conn.close()
        print(f"업데이트 완료! 총 {len(final_df)}개 종목 분석.")

if __name__ == "__main__":
    update_database()
