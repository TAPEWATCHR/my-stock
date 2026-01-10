import yfinance as yf
import pandas as pd
import sqlite3
import requests
from io import StringIO
from datetime import datetime
import time

def get_total_market_tickers():
    """나스닥 공식 데이터를 직접 파싱하여 5,000~8,000개 티커 확보"""
    all_tickers = set()
    
    # 전략: 나스닥 공식 리스트 서버 (가장 확실함)
    print("Source: NASDAQ Trader 공식 리스트 서버 접속 중...")
    try:
        # 나스닥 상장 종목
        url_nasdaq = "https://tda.nasdaqtrader.com/dynamic/SymDir/nasdaqlisted.txt"
        res = requests.get(url_nasdaq, timeout=15)
        df_nasdaq = pd.read_csv(StringIO(res.text), sep='|')
        all_tickers.update(df_nasdaq['Symbol'].dropna().astype(str).tolist())
        
        # 기타 거래소(NYSE, AMEX) 종목
        url_other = "https://tda.nasdaqtrader.com/dynamic/SymDir/otherlisted.txt"
        res_other = requests.get(url_other, timeout=15)
        df_other = pd.read_csv(StringIO(res_other.text), sep='|')
        all_tickers.update(df_other['NASDAQ Symbol'].dropna().astype(str).tolist())
    except Exception as e:
        print(f"공식 서버 접속 실패: {e}. 위키피디아 백업 가동.")
        # 최악의 경우에도 1,000개는 확보하도록 설계
        url_sp = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
        all_tickers.update(pd.read_html(url_sp)[0]['Symbol'].tolist())

    # 티커 정제 (특수문자 제거 및 유효성 검사)
    clean_list = []
    for s in all_tickers:
        s = s.strip().upper()
        # 테스트/기타 심볼 제외 (보통 1-5자 영문)
        if 1 <= len(s) <= 5 and s.isalpha():
            clean_list.append(s)
    
    final_list = sorted(list(set(clean_list)))
    print(f"--- [성공] 최종 확보된 전체 티커 수: {len(final_list)}개 ---")
    return final_list

def update_database():
    start_time = datetime.now()
    tickers = get_total_market_tickers()
    
    if len(tickers) < 3000:
        print(f"주의: 수집된 종목이 {len(tickers)}개로 전체 시장 대비 적습니다.")

    all_results = []
    # 5,000개 이상 대량 처리를 위한 설정
    chunk_size = 100 # 차단 방지를 위해 더 작게 쪼갬
    
    for i in range(0, len(tickers), chunk_size):
        chunk = tickers[i:i + chunk_size]
        print(f"[{i}/{len(tickers)}] 전체 시장 분석 진행 중...")
        
        try:
            # 1년치 데이터 다운로드
            data = yf.download(chunk, period="1y", interval="1d", progress=False, threads=False)['Close']
            
            if isinstance(data, pd.Series): data = data.to_frame()

            for ticker in data.columns:
                series = data[ticker].dropna()
                if len(series) < 240: continue # 상장된 지 1년 안 된 종목 제외
                
                now = series.iloc[-1]
                # IBD 방식 RS 점수 산출 가중치
                m3, m6, m9, m12 = series.iloc[-63], series.iloc[-126], series.iloc[-189], series.iloc[0]
                raw_score = (now/m3 * 2) + (now/m6) + (now/m9) + (now/m12)
                
                all_results.append({
                    'symbol': ticker, 'raw_score': raw_score, 'price': round(now, 2)
                })
        except: continue
        time.sleep(0.3) # 야후 서버 부하 조절

    if all_results:
        final_df = pd.DataFrame(all_results)
        # 전체 5,000~8,000개 중 백분위 순위 산출
        final_df['rs_score'] = (final_df['raw_score'].rank(pct=True) * 99).astype(int)
        
        conn = sqlite3.connect('ibd_system.db')
        final_df[['symbol', 'rs_score', 'price']].to_sql('repo_results', conn, if_exists='replace', index=False)
        conn.close()
        print(f"[{datetime.now()}] 업데이트 성공! 총 {len(final_df)}개 종목의 전체 순위 반영 완료.")

if __name__ == "__main__":
    update_database()
