# -*- coding: utf-8 -*-
import yfinance as yf
import pandas as pd
import sqlite3
import time
from datetime import datetime
import requests

def init_master_db():
    conn = sqlite3.connect('ibd_system.db')
    # 산업군/개요 보존용 마스터 테이블
    conn.execute("""
        CREATE TABLE IF NOT EXISTS company_profiles (
            symbol TEXT PRIMARY KEY, industry TEXT, description TEXT
        )
    """)
    conn.close()

def get_pure_exchange_stocks():
    """SEC와 외부 CSV를 병합하여 기본 종목 리스트 생성"""
    # [핵심 수정] SEC 규정에 맞게 User-Agent에 앱 이름과 본인 이메일을 기재해야 차단되지 않음
    headers = {'User-Agent': 'MarketLeadersTerminal gkfapwkd@gmail.com'} 
    
    try:
        url = "https://www.sec.gov/files/company_tickers_exchange.json"
        res = requests.get(url, headers=headers, timeout=10)
        
        # 접속에 실패(403 차단 등)하면 여기서 에러를 발생시켜 except로 보냄
        res.raise_for_status() 
        
        res_json = res.json()
        df_sec = pd.DataFrame(res_json['data'], columns=res_json['fields'])
        df_sec = df_sec[df_sec['exchange'].isin(['Nasdaq', 'NYSE'])]
        df_sec['symbol'] = df_sec['ticker'].str.upper().replace('.', '-')
        
        print(f"✅ SEC 거래소 종목 로드 성공: 총 {len(df_sec)}개")
        return df_sec[['symbol', 'name']].copy()
        
    except Exception as e: 
        # 에러를 숨기지 않고 출력하여 원인을 파악할 수 있게 함
        print(f"🚨 SEC 데이터 로드 실패: {e}") 
        return pd.DataFrame()

def update_database():
    init_master_db()
    base_df = get_pure_exchange_stocks()
    if base_df.empty: return

    conn = sqlite3.connect('ibd_system.db')
    # 1. DB에 이미 저장된 산업군 정보 불러오기 (마스터 데이터 활용)
    master_profiles = pd.read_sql("SELECT symbol, industry FROM company_profiles", conn)
    base_df = pd.merge(base_df, master_profiles, on='symbol', how='left')
    base_df['industry'] = base_df['industry'].fillna('Unknown')

    tickers = base_df['symbol'].tolist()
    all_results = []
    
    print(f"🚀 {len(tickers)}개 종목 분석 시작...")

    chunk_size = 50 # 한 번에 가져올 뭉치
    for i in range(0, len(tickers), chunk_size):
        chunk = tickers[i:i + chunk_size]
        
        # 2. 가격 데이터는 최신성이 중요하므로 호출
        try:
            price_data = yf.download(chunk, period="1y", interval="1d", progress=False, group_by='ticker')
        except:
            print("⚠️ 다운로드 오류 - 잠시 대기")
            time.sleep(10); continue

        for ticker in chunk:
            try:
                hist = price_data[ticker].dropna() if len(chunk) > 1 else price_data.dropna()
                if len(hist) < 150: continue

                p = hist['Close']
                rs_raw = (p.iloc[-1]/p.iloc[-21]*2) + (p.iloc[-1]/p.iloc[-63]*2) # 단순화된 RS
                adv_50 = (p * hist['Volume']).tail(50).mean()

                # 3. [핵심] 산업군이 Unknown인 경우만 yfinance.info 호출 (차단 방지 핵심)
                row = base_df[base_df['symbol'] == ticker].iloc[0]
                industry = row['industry']
                
                if industry == 'Unknown':
                    t_obj = yf.Ticker(ticker)
                    info = t_obj.info
                    industry = info.get('industry', 'Unknown')
                    desc = info.get('longBusinessSummary', '')
                    # DB 마스터 테이블에 영구 저장
                    if industry != 'Unknown':
                        conn.execute("INSERT OR REPLACE INTO company_profiles (symbol, industry, description) VALUES (?, ?, ?)",
                                     (ticker, industry, desc))
                        conn.commit()

                all_results.append({
                    'symbol': ticker, 'price': float(p.iloc[-1]), 'rs_raw': rs_raw,
                    'industry': industry, 'adv_50': adv_50,
                    'smr_grade': 'C', 'ad_grade': 'C' # SMR은 FMP API로 대시보드에서 실시간 보정 권장
                })
            except: continue
        
        print(f" > {i+chunk_size}개 완료...")
        time.sleep(2) # 뭉치 사이의 휴식

    if all_results:
        final_df = pd.DataFrame(all_results)
        # 등급 계산
        final_df['rs_score'] = (final_df['rs_raw'].rank(pct=True) * 99).astype(int)
        
        # 산업군 RS 계산
        ind_rs = final_df.groupby('industry')['rs_raw'].mean().reset_index(name='ind_rs_raw')
        ind_rs['industry_rs_score'] = (ind_rs['ind_rs_raw'].rank(pct=True) * 99).astype(int)
        final_df = pd.merge(final_df, ind_rs[['industry', 'industry_rs_score']], on='industry')

        final_df.to_sql('repo_results', conn, if_exists='replace', index=False)
        print("✅ 업데이트 성공")
    
    conn.close()

if __name__ == "__main__":
    update_database()
