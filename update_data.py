import yfinance as yf
import pandas as pd
import sqlite3
import requests
from io import StringIO
from datetime import datetime
import time

def get_clean_tickers():
    """위키피디아에서 S&P 500, 나스닥 100 리스트를 가장 안전하게 추출"""
    headers = {"User-Agent": "Mozilla/5.0"}
    tickers_dict = {} # {symbol: sector}

    print("Step 1: 핵심 우량주 리스트 수집 시작...")
    
    # 1. S&P 500 수집
    try:
        url_sp = 'https://en.wikipedia.org/wiki/List_of_S%26P_500_companies'
        sp_tables = pd.read_html(StringIO(requests.get(url_sp, headers=headers).text))
        for _, row in sp_tables[0].iterrows():
            sym = str(row[0]).strip().replace('.', '-')
            if len(sym) <= 5 and sym.isalpha(): # 이름 혼입 방지: 5자 이하 영문만
                tickers_dict[sym] = str(row[3])
    except Exception as e: print(f"S&P 500 실패: {e}")

    # 2. NASDAQ 100 추가
    try:
        url_ndx = 'https://en.wikipedia.org/wiki/Nasdaq-100'
        ndx_tables = pd.read_html(StringIO(requests.get(url_ndx, headers=headers).text))
        # 나스닥 100 테이블 위치 찾기 (보통 3~5번째)
        target_df = ndx_tables[4] if len(ndx_tables) > 4 else ndx_tables[3]
        for _, row in target_df.iterrows():
            sym = str(row[1] if 'Ticker' in target_df.columns else row[0]).strip().replace('.', '-')
            if sym not in tickers_dict and len(sym) <= 5 and sym.isalpha():
                tickers_dict[sym] = "Technology/Growth"
    except Exception as e: print(f"Nasdaq 100 실패: {e}")

    # 데이터프레임 변환
    master = pd.DataFrame([{'symbol': k, 'sector': v} for k, v in tickers_dict.items()])
    print(f"--- 최종 확인된 유효 티커 수: {len(master)}개 ---")
    return master

def update_database():
    start_time = datetime.now()
    print(f"[{start_time}] 업데이트 프로세스 가동")
    
    master_data = get_clean_tickers()
    if master_data.empty:
        print("티커 리스트가 비어있어 종료합니다.")
        return

    tickers = master_data['symbol'].tolist()
    all_results = []

    # 안전하게 100개씩 끊어서 다운로드 (에러 최소화)
    chunk_size = 100
    for i in range(0, len(tickers), chunk_size):
        chunk = tickers[i:i + chunk_size]
        print(f"진행 중: {i} / {len(tickers)}...")
        
        try:
            # group_by=False로 설정하여 멀티인덱스 방지
            data = yf.download(chunk, period="1y", interval="1d", progress=False, threads=True)['Close']
            
            # 단일 종목일 경우와 다중 종목일 경우 처리
            if isinstance(data, pd.Series):
                data = data.to_frame()

            for ticker in data.columns:
                series = data[ticker].dropna()
                if len(series) < 200: continue
                
                # RS 점수 계산
                now = series.iloc[-1]
                m3, m6, m9, m12 = series.iloc[-63], series.iloc[-126], series.iloc[-189], series.iloc[0]
                raw_score = (now/m3 * 2) + (now/m6) + (now/m9) + (now/m12)
                
                # 이동평균선
                ma50 = series.rolling(50).mean().iloc[-1]
                ma200 = series.rolling(200).mean().iloc[-1]
                
                all_results.append({
                    'symbol': ticker, 'raw_score': raw_score, 'price': round(now, 2),
                    'ma50': ma50, 'ma200': ma200
                })
        except: continue
        time.sleep(1)

    # 결과 정리 및 DB 저장
    if all_results:
        final_df = pd.DataFrame(all_results)
        final_df['rs_score'] = (final_df['raw_score'].rank(pct=True) * 99).astype(int)
        
        # 섹터 정보 다시 입히기
        final_df = pd.merge(final_df, master_data, on='symbol', how='left')
        ind_avg = final_df.groupby('sector')['raw_score'].mean().rank(pct=True) * 99
        final_df['industry_rs_score'] = final_df['sector'].map(ind_avg.to_dict()).fillna(50).astype(int)

        db_ready = []
        for _, row in final_df.iterrows():
            db_ready.append({
                'symbol': row['symbol'], 'rs_score': row['rs_score'],
                'industry_rs_score': row['industry_rs_score'],
                'smr_grade': "A" if row['rs_score'] > 85 else "C",
                'ad_rating': "A" if row['price'] > row['ma50'] > row['ma200'] else "C",
                'sector': row['sector'], 'last_updated': datetime.now().strftime('%Y-%m-%d')
            })

        conn = sqlite3.connect('ibd_system.db')
        pd.DataFrame(db_ready).to_sql('repo_results', conn, if_exists='replace', index=False)
        conn.close()
        print(f"업데이트 완료! 소요시간: {datetime.now() - start_time}")

if __name__ == "__main__":
    update_database()
