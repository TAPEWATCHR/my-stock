import yfinance as yf
import pandas as pd
import sqlite3
import numpy as np
import requests
from io import StringIO
from datetime import datetime
import time

def get_master_list():
    """나스닥 리스트 실패 시를 대비해 더 강력한 백업 소스 활용"""
    headers = {"User-Agent": "Mozilla/5.0"}
    
    print("Step 1-1: 주요 종목 섹터 정보 확보 중...")
    try:
        url_sp = 'https://en.wikipedia.org/wiki/List_of_S%26P_500_companies'
        sp500 = pd.read_html(StringIO(requests.get(url_sp, headers=headers).text))[0]
        sp_data = pd.DataFrame({'symbol': sp500.iloc[:, 0], 'sector': sp500.iloc[:, 3]})
    except:
        sp_data = pd.DataFrame(columns=['symbol', 'sector'])

    print("Step 1-2: 미국 시장 전체 티커 확보 중...")
    # 방법 A: 나스닥 공식 (기존 방식)
    # 방법 B: 백업 소스 (일반적인 상장 티커 패턴 활용)
    try:
        url_all = "https://tda.nasdaqtrader.com/dynamic/SymDir/nasdaqlisted.txt"
        resp = requests.get(url_all, timeout=10)
        all_df = pd.read_csv(StringIO(resp.text), sep='|')
        all_df = all_df[all_df['Symbol'].notna() & (all_df['Test Issue'] == 'N')]
        all_tickers = all_df['Symbol'].tolist()
    except:
        print("나스닥 서버 연결 실패. 백업 티커 생성 모드 가동...")
        # 나스닥 서버가 죽었을 때를 대비해 S&P 500 + 주요 Nasdaq 100이라도 합칩니다.
        all_tickers = sp_data['symbol'].tolist()
        try:
            url_ndx = 'https://en.wikipedia.org/wiki/Nasdaq-100'
            ndx = pd.read_html(StringIO(requests.get(url_ndx, headers=headers).text))[4]
            all_tickers.extend(ndx.iloc[:, 1 if 'Ticker' in ndx.columns else 0].tolist())
        except: pass

    all_data = pd.DataFrame({'symbol': list(set(all_tickers))}) # 중복 제거
    all_data['symbol'] = all_data['symbol'].str.replace('.', '-', regex=False).str.strip()
    
    # .B 주식들을 -B로 변환 (BRK.B -> BRK-B)
    all_data['symbol'] = all_data['symbol'].apply(lambda x: x.replace('.B', '-B') if isinstance(x, str) else x)

    master = pd.merge(all_data, sp_data, on='symbol', how='left')
    master['sector'] = master['sector'].fillna('US Market')
    return master.drop_duplicates('symbol')

def update_database():
    start_time = datetime.now()
    print(f"[{start_time}] 데이터 업데이트 프로세스 시작")
    
    try:
        master_data = get_master_list()
        tickers = master_data['symbol'].tolist()
        
        print(f"Step 2: {len(tickers)}개 종목 주가 다운로드 중 (분할 처리)...")
        # 5,000개 한꺼번에 요청 시 서버 차단 방지를 위해 2,000개씩 끊어서 처리
        chunk_size = 2000
        all_close_data = []
        
        for i in range(0, len(tickers), chunk_size):
            chunk = tickers[i:i + chunk_size]
            print(f"다운로드 중: {i} ~ {min(i+chunk_size, len(tickers))}...")
            # threads=True로 속도 업, auto_adjust=True로 수정주가 반영
            data = yf.download(chunk, period="1y", interval="1d", threads=True, progress=False)['Close']
            all_close_data.append(data)
            time.sleep(2) # 서버 과부하 방지 2초 휴식
            
        full_close_data = pd.concat(all_close_data, axis=1)
        
        print("Step 3: RS 점수 및 기술적 지표 산출 중...")
        results = []
        for ticker in full_close_data.columns:
            series = full_close_data[ticker].dropna()
            if len(series) < 250: continue # 상장 1년 미만 제외
            
            # 오닐 방식 RS (최근 3개월에 가중치 2배)
            now, m3, m6, m9, m12 = series.iloc[-1], series.iloc[-63], series.iloc[-126], series.iloc[-189], series.iloc[0]
            raw_score = (now/m3 * 2) + (now/m6) + (now/m9) + (now/m12)
            
            # AD 등급용 이평선
            ma50, ma200 = series.rolling(50).mean().iloc[-1], series.rolling(200).mean().iloc[-1]
            
            results.append({
                'symbol': ticker, 'raw_score': raw_score, 'price': now, 'ma50': ma50, 'ma200': ma200
            })
            
        rs_df = pd.DataFrame(results)
        # 전체 시장 대비 상대강도 백분위 (1~99)
        rs_df['rs_score'] = rs_df['raw_score'].rank(pct=True) * 99
        
        # 섹터 정보 결합 및 산업군 RS 계산
        final_df = pd.merge(rs_df, master_data, on='symbol', how='left')
        ind_avg = final_df.groupby('sector')['raw_score'].mean().rank(pct=True) * 99
        final_df['industry_rs_score'] = final_df['sector'].map(ind_avg.to_dict())

        print("Step 4: 최종 DB 파일 생성 중...")
        db_ready = []
        for _, row in final_df.iterrows():
            db_ready.append({
                'symbol': row['symbol'],
                'rs_score': int(row['rs_score']),
                'industry_rs_score': int(row['industry_rs_score']),
                'smr_grade': "A" if row['rs_score'] > 85 else ("B" if row['rs_score'] > 70 else "C"),
                'ad_rating': "A" if row['price'] > row['ma50'] > row['ma200'] else "C",
                'sector': row['sector'],
                'last_updated': datetime.now().strftime('%Y-%m-%d')
            })

        # SQLite 저장
        conn = sqlite3.connect('ibd_system.db')
        pd.DataFrame(db_ready).to_sql('repo_results', conn, if_exists='replace', index=False)
        conn.close()
        
        duration = datetime.now() - start_time
        print(f"[{datetime.now()}] 업데이트 완료! 소요시간: {duration}")

    except Exception as e:
        print(f"시스템 에러 발생: {e}")

if __name__ == "__main__":
    update_database()
