import yfinance as yf
import pandas as pd
import sqlite3
import time
from datetime import datetime, timedelta
import os
import requests
import io
import numpy as np

def get_industry_master_map():
    """Sector 대신 더 세분화된 Industry 데이터를 가져옵니다."""
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
        
    print(f"Total Industry Map Size: {len(industry_map)} symbols")
    return industry_map

def calculate_ad_raw(hist):
    """50일 평균 거래량과 가격 변동을 이용한 기관 매집(AD) 에너지 계산"""
    if len(hist) < 65: return 0
    df = hist.copy()
    df['daily_return'] = df['Close'].pct_change()
    df['vol_50ma'] = df['Volume'].rolling(50).mean()
    df = df.dropna(subset=['daily_return', 'vol_50ma'])
    
    if len(df) < 65: return 0
    df = df.tail(65)
    
    # 당일 매집/분산 에너지 = (당일거래량 / 50일평균거래량) * 당일수익률
    df['ad_daily'] = (df['Volume'] / df['vol_50ma']) * df['daily_return'] * 100
    
    # 최근 20일에 70% 가중치, 이전 45일에 30% 가중치 부여
    recent_20 = df['ad_daily'].tail(20).sum() * 0.7
    past_45 = df['ad_daily'].head(45).sum() * 0.3
    
    return recent_20 + past_45

def get_smr_acceleration(t_obj):
    """최근 3분기/3개년 재무 데이터를 통한 가속도(Delta) 및 흑자 여부 계산"""
    sales_accel, margin_accel, pretax_accel, roe_accel = np.nan, np.nan, np.nan, np.nan
    is_profitable = False 
    
    try:
        # 1. 분기별 데이터 (매출, 세후이익)
        qf = t_obj.quarterly_financials
        if not qf.empty and 'Total Revenue' in qf.index and 'Net Income' in qf.index:
            rev = qf.loc['Total Revenue'].dropna().values
            net = qf.loc['Net Income'].dropna().values
            
            if len(net) > 0 and net[0] > 0:
                is_profitable = True
                
            if len(rev) >= 3:
                g0 = (rev[0] - rev[1]) / abs(rev[1]) if rev[1] != 0 else 0
                g1 = (rev[1] - rev[2]) / abs(rev[2]) if rev[2] != 0 else 0
                sales_accel = g0 - g1
                
            if len(net) >= 3 and len(rev) >= 3:
                m0 = net[0] / rev[0] if rev[0] != 0 else 0
                m1 = net[1] / rev[1] if rev[1] != 0 else 0
                m2 = net[2] / rev[2] if rev[2] != 0 else 0
                margin_accel = (m0 - m1) + (m1 - m2)

        # 2. 연간 데이터 (세전이익, ROE)
        yf_fin = t_obj.financials
        bs = t_obj.balance_sheet
        if not yf_fin.empty and 'Pretax Income' in yf_fin.index and 'Total Revenue' in yf_fin.index:
            pretax = yf_fin.loc['Pretax Income'].dropna().values
            rev_a = yf_fin.loc['Total Revenue'].dropna().values
            if len(pretax) >= 3 and len(rev_a) >= 3:
                pm0 = pretax[0] / rev_a[0] if rev_a[0] != 0 else 0
                pm1 = pretax[1] / rev_a[1] if rev_a[1] != 0 else 0
                pm2 = pretax[2] / rev_a[2] if rev_a[2] != 0 else 0
                pretax_accel = (pm0 - pm1) + (pm1 - pm2)

        if not yf_fin.empty and not bs.empty and 'Net Income' in yf_fin.index and 'Stockholders Equity' in bs.index:
            net_a = yf_fin.loc['Net Income'].dropna().values
            eq = bs.loc['Stockholders Equity'].dropna().values
            min_len = min(len(net_a), len(eq))
            if min_len >= 3:
                roe0 = net_a[0] / eq[0] if eq[0] != 0 else 0
                roe1 = net_a[1] / eq[1] if eq[1] != 0 else 0
                roe2 = net_a[2] / eq[2] if eq[2] != 0 else 0
                roe_accel = (roe0 - roe1) + (roe1 - roe2)

    except Exception:
        # 데이터가 비어있어 발생하는 정상적인 처리 (Pass)
        pass 

    return sales_accel, margin_accel, pretax_accel, roe_accel, is_profitable

def get_tickers():
    if os.path.exists('tickers.txt'):
        with open('tickers.txt', 'r') as f:
            tickers = [line.strip().upper().replace('.', '-') for line in f if line.strip()]
            return list(set(tickers))
    else:
        print("Warning: 'tickers.txt' not found. Using sample tickers.")
        return ['AAPL', 'NVDA', 'MSFT', 'TSLA', 'TNGX'] 

def update_database():
    tickers = get_tickers()
    industry_master = get_industry_master_map()
    all_results = []
    chunk_size = 30 
    
    print(f"--- IBD 시스템 시작 (가속도 및 시총 가중치 반영) ({datetime.now()}) ---")
    print(f"--- 총 {len(tickers)}개 종목 분석 예정 ---")

    for i in range(0, len(tickers), chunk_size):
        chunk = tickers[i:i + chunk_size]
        try:
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
                    idx_21 = -21 if len(hist) >= 21 else 0
                    idx_63 = -63 if len(hist) >= 63 else 0
                    idx_126 = -126 if len(hist) >= 126 else 0
                    idx_189 = -189 if len(hist) >= 189 else 0
                    idx_252 = -252 if len(hist) >= 252 else 0

                    rs_raw = (now_price / hist['Close'].iloc[idx_21] * 2) + \
                             (now_price / hist['Close'].iloc[idx_63] * 2) + \
                             (now_price / hist['Close'].iloc[idx_126]) + \
                             (now_price / hist['Close'].iloc[idx_189]) + \
                             (now_price / hist['Close'].iloc[idx_252])

                    ad_raw = calculate_ad_raw(hist)
                    adv_50 = (hist['Close'] * hist['Volume']).tail(50).mean()

                    industry = industry_master.get(ticker, "Unknown")
                    mcap = 1 
                    
                    # --- 야후 파이낸스 API 차단 방어 (Retry 로직) ---
                    t_obj = yf.Ticker(ticker)
                    sales_acc, margin_acc, pretax_acc, roe_acc, is_profitable = np.nan, np.nan, np.nan, np.nan, False
                    info = {}
                    
                    max_retries = 3
                    for attempt in range(max_retries):
                        try:
                            # 여기서 서버 요청이 이루어짐
                            sales_acc, margin_acc, pretax_acc, roe_acc, is_profitable = get_smr_acceleration(t_obj)
                            info = t_obj.info
                            break # 정상적으로 데이터를 가져오면 탈출
                        except Exception as e:
                            if attempt < max_retries - 1:
                                time.sleep(2) # 차단당하면 2초 대기 후 다시 시도
                            else:
                                print(f"  > [API 경고] {ticker} 세부 정보 수집 실패 (서버 지연)")
                    
                    # 수집된 info에서 정보 추출
                    if info:
                        mcap = info.get('marketCap', 1)
                        if info.get('industry'): industry = info.get('industry')

                    if pd.isna(industry) or industry == "nan": industry = "Unknown"

                    all_results.append({
                        'symbol': ticker, 'price': float(now_price), 'rs_raw': rs_raw,
                        'ad_raw': ad_raw, 'adv_50': adv_50, 'mcap': float(mcap),
                        'sales_acc': sales_acc, 'margin_acc': margin_acc, 
                        'pretax_acc': pretax_acc, 'roe_acc': roe_acc,
                        'is_profitable': is_profitable,
                        'industry': industry
                    })
                    
                    # 종목당 아주 짧은 딜레이를 주어 API 차단을 근본적으로 예방
                    time.sleep(0.3) 

                except Exception as inner_e:
                    continue 

            print(f" > {min(i+chunk_size, len(tickers))} / {len(tickers)} 완료 | 최근 산업군 예시: {industry}")
            time.sleep(1) # 청크 단위 휴식

        except Exception as e:
            print(f"Chunk Error: {e}")
            time.sleep(5)

    if all_results:
        df = pd.DataFrame(all_results)
        
        # 1. 개별 종목 RS Score (1~99)
        df['rs_score'] = (df['rs_raw'].rank(pct=True) * 98 + 1).fillna(0).astype(int)
        
        # 2. AD Rating (상대평가 백분위로 A~E 부여)
        df['ad_grade'] = pd.qcut(df['ad_raw'].rank(method='first'), 5, labels=['E', 'D', 'C', 'B', 'A'])
        
        # 3. SMR Grade 수정 (결측치 하위 처리 및 적자기업 패널티)
        df['smr_val'] = (df['sales_acc'].rank(pct=True, na_option='bottom').fillna(0) * 0.4) + \
                        (df['margin_acc'].rank(pct=True, na_option='bottom').fillna(0) * 0.3) + \
                        (df['roe_acc'].rank(pct=True, na_option='bottom').fillna(0) * 0.2) + \
                        (df['pretax_acc'].rank(pct=True, na_option='bottom').fillna(0) * 0.1)
                        
        df.loc[df['is_profitable'] == False, 'smr_val'] -= 1.0
        
        df['smr_grade'] = pd.qcut(df['smr_val'].rank(method='first'), 5, labels=['E', 'D', 'C', 'B', 'A'])
        
        # 4. Industry RS Score (시가총액 가중 평균 적용)
        df['weighted_rs'] = df['rs_raw'] * df['mcap']
        industry_data = df.groupby('industry').apply(
            lambda x: x['weighted_rs'].sum() / x['mcap'].sum() if x['mcap'].sum() > 0 else x['rs_raw'].mean()
        ).reset_index(name='ind_rs_raw')
        
        industry_data['industry_rs_score'] = (industry_data['ind_rs_raw'].rank(pct=True) * 98 + 1).fillna(0).astype(int)
        
        final_df = pd.merge(df, industry_data[['industry', 'industry_rs_score']], on='industry', how='left')
        final_df['industry_rs_score'] = final_df['industry_rs_score'].fillna(0).astype(int)

        conn = sqlite3.connect('ibd_system.db')
        try:
            # === [데이터베이스 저장 및 업데이트 로직 변경 구간] ===
            save_cols = ['symbol', 'price', 'rs_score', 'smr_grade', 'ad_grade', 'industry_rs_score', 'industry', 'adv_50']
            final_df[save_cols].to_sql('repo_results', conn, if_exists='replace', index=False)
            
            today_str = datetime.now().strftime('%Y-%m-%d')
            
            # 기존 rs_history 테이블에 industry_rs_score 컬럼이 없다면 추가 시도
            try:
                conn.execute("ALTER TABLE rs_history ADD COLUMN industry_rs_score INTEGER;")
            except sqlite3.OperationalError:
                # 이미 컬럼이 존재하면 무시하고 넘어감
                pass
            
            # 과거 이력 저장용 데이터프레임 구성 (industry_rs_score 포함)
            history_df = final_df[['symbol', 'rs_score', 'industry_rs_score']].copy()
            history_df['date'] = today_str
            history_df.to_sql('rs_history', conn, if_exists='append', index=False)
            
            print("--- 데이터베이스 최적화 및 정리 시작 ---")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_symbol ON rs_history (symbol)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_date ON rs_history (date)")
            one_year_ago = (datetime.now() - timedelta(days=365)).strftime('%Y-%m-%d')
            conn.execute(f"DELETE FROM rs_history WHERE date < '{one_year_ago}'")
            conn.execute("VACUUM")
            
            print(f"--- DB 저장 및 최적화 완벽 처리 완료 ({today_str}) ---")
            
        except Exception as db_e:
            print(f"DB 저장 및 최적화 에러: {db_e}")
        finally:
            conn.close()
    else:
        print("--- 결과 데이터가 없습니다. ---")

if __name__ == "__main__":
    update_database()
