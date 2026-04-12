# -*- coding: utf-8 -*-
import streamlit as st
import pandas as pd
import sqlite3
import requests
import streamlit.components.v1 as components
import os
import altair as alt
from datetime import datetime, timedelta

# --- [설정] FMP API 키 ---
FMP_API_KEY = os.environ.get('FMP_API_KEY', "1kJBflGjsp5fCgbancejhI5bN5iavEJF")

# --- 즐겨찾기 및 데이터 로드 함수 ---
def get_data():
    if not os.path.exists('ibd_system.db'): return pd.DataFrame()
    conn = sqlite3.connect('ibd_system.db')
    try:
        df = pd.read_sql("SELECT * FROM repo_results", conn)
        # 데이터가 비어있을 경우를 대비해 기본 등급 채우기
        if 'smr_grade' not in df.columns: df['smr_grade'] = 'C'
        if 'ad_grade' not in df.columns: df['ad_grade'] = 'C'
        df['smr_grade'] = df['smr_grade'].fillna('C')
        df['ad_grade'] = df['ad_grade'].fillna('C')
    except: df = pd.DataFrame()
    finally: conn.close()
    return df

def get_rs_history(ticker):
    if not os.path.exists('ibd_system.db'): return pd.DataFrame()
    conn = sqlite3.connect('ibd_system.db')
    try:
        hist_df = pd.read_sql(f"SELECT * FROM rs_history WHERE symbol = '{ticker}' ORDER BY date ASC", conn)
    except: hist_df = pd.DataFrame()
    finally: conn.close()
    return hist_df

# --- 상세 정보 및 체크리스트용 데이터 (FMP 활용) ---
@st.cache_data(ttl=3600)
def get_detailed_info_fmp(ticker):
    try:
        # 1. 프로필 및 재무제표
        profile = requests.get(f"https://financialmodelingprep.com/api/v3/profile/{ticker}?apikey={FMP_API_KEY}").json()
        income = requests.get(f"https://financialmodelingprep.com/api/v3/income-statement/{ticker}?period=quarter&limit=12&apikey={FMP_API_KEY}").json()
        
        # 2. 체크리스트용 가격 데이터 (최근 1년)
        price_hist = requests.get(f"https://financialmodelingprep.com/api/v3/historical-price-full/{ticker}?serietype=line&apikey={FMP_API_KEY}").json()
        
        info = profile[0] if profile else {}
        q_inc = pd.DataFrame(income).set_index('date').T if income else pd.DataFrame()
        prices = pd.DataFrame(price_hist.get('historical', []))
        
        return q_inc, info, prices
    except:
        return pd.DataFrame(), {}, pd.DataFrame()

# --- 페이지 설정 및 스타일 ---
st.set_page_config(layout="wide", page_title="Institutional Stock Terminal")
st.markdown("""
<style>
    .stApp { background-color: #161C27 !important; color: #ccd6f6; }
    .overview-panel { background: #252b3b; padding: 1.5rem; border-radius: 12px; border: 1px solid #4a5161; }
    .metric-card { background: #363C4C; padding: 15px; border-radius: 10px; text-align: center; border: 1px solid #4a5161; }
</style>
""", unsafe_allow_html=True)

# --- 메인 로직 ---
df = get_data()

if not df.empty:
    # --- 1. 사이드바 (필터 복구) ---
    with st.sidebar:
        st.header("🎛️ Terminal Control")
        
        # 기본 필터
        min_price = st.number_input("최소 주가 ($)", value=5.0)
        min_adv = st.number_input("최소 거래대금 ($M)", value=1.0)
        rs_min = st.slider("최소 RS 점수", 1, 99, 70)
        
        # SMR/AD 등급 필터 (멀티 선택으로 복구)
        smr_filter = st.multiselect("SMR 등급", options=['A', 'B', 'C', 'D', 'E'], default=['A', 'B', 'C'])
        ad_filter = st.multiselect("AD 수급 등급", options=['A', 'B', 'C', 'D', 'E'], default=['A', 'B', 'C'])
        
        # 산업군 필터
        all_industries = sorted(df['industry'].unique().tolist())
        ind_filter = st.multiselect("산업군 선택", options=all_industries, default=all_industries)

        # 필터링 적용
        mask = (df['price'] >= min_price) & \
               (df['adv_50'] >= min_adv * 1_000_000) & \
               (df['rs_score'] >= rs_min) & \
               (df['smr_grade'].isin(smr_filter)) & \
               (df['ad_grade'].isin(ad_filter)) & \
               (df['industry'].isin(ind_filter))
        
        f_df = df[mask].sort_values('rs_score', ascending=False)

    # --- 2. 메인 화면 (리스트 및 상세 보기) ---
    col_l, col_r = st.columns([4, 3])

    with col_l:
        st.subheader(f"Leaders List ({len(f_df)})")
        display_df = f_df.copy()
        display_df['ADV($M)'] = (display_df['adv_50'] / 1_000_000).round(1)
        
        sel = st.dataframe(
            display_df[['symbol', 'price', 'rs_score', 'smr_grade', 'ad_grade', 'industry_rs_score', 'industry', 'ADV($M)']],
            hide_index=True, on_select="rerun", selection_mode="single-row", height=800, use_container_width=True
        )

    with col_r:
        if len(sel.selection.rows) > 0:
            row = f_df.iloc[sel.selection.rows[0]]
            ticker = row['symbol']
            
            st.markdown(f"## {ticker}")
            st.write(f"**{row['industry']}** | RS: {row['rs_score']} | SMR: {row['smr_grade']} | AD: {row['ad_grade']}")

            # 데이터 로드
            q_inc, info, price_df = get_detailed_info_fmp(ticker)
            
            # --- 탭 구성 (복구 완료) ---
            t_chart, t_fin, t_check, t_biz = st.tabs(["📊 차트", "🧾 재무제표", "🛡️ 체크리스트", "🏢 개요"])

            with t_chart:
                components.html(f"""
                    <script type="text/javascript" src="https://s3.tradingview.com/tv.js"></script>
                    <div id="tv_chart" style="height: 500px;"></div>
                    <script type="text/javascript">
                    new TradingView.widget({{"autosize":true,"symbol":"{ticker}","interval":"D","theme":"dark","style":"1","locale":"kr","container_id":"tv_chart"}});
                    </script>
                """, height=500)
                
                # RS 트렌드 차트
                rs_h = get_rs_history(ticker)
                if not rs_h.empty:
                    rs_chart = alt.Chart(rs_h).mark_line(color='#64ffda').encode(x='date:T', y='rs_score:Q').properties(height=200)
                    st.altair_chart(rs_chart, use_container_width=True)

            with t_fin:
                if not q_inc.empty:
                    st.write("#### 최근 분기 실적 (FMP Data)")
                    # 주요 항목 매핑
                    fin_display = q_inc.reindex(['revenue', 'netIncome', 'eps', 'operatingIncome']).dropna()
                    fin_display.index = ['매출액', '순이익', 'EPS', '영업이익']
                    st.dataframe(fin_display.style.format("{:,.0f}"), use_container_width=True)
                else:
                    st.warning("재무 데이터를 불러올 수 없습니다.")

            with t_check:
                st.subheader("🛡️ 주도주 판별 시스템")
                if not price_df.empty:
                    # 가격 데이터 기반 기술적 분석
                    p = price_df.sort_values('date')
                    curr_p = p['close'].iloc[-1]
                    ma50 = p['close'].rolling(50).mean().iloc[-1]
                    ma150 = p['close'].rolling(150).mean().iloc[-1]
                    ma200 = p['close'].rolling(200).mean().iloc[-1]
                    low52 = p['close'].min()
                    high52 = p['close'].max()

                    c1, c2 = st.columns(2)
                    with c1:
                        st.markdown("### 🟢 CANSLIM")
                        st.checkbox("C/A: 분기/연간 성장성 (SMR 등급 참고)", value=row['smr_grade'] in ['A', 'B'])
                        st.checkbox(f"L: 시장 주도주 (RS 80↑: {row['rs_score']})", value=row['rs_score'] >= 80)
                        st.checkbox(f"N: 52주 고가 근접 ({((curr_p/high52)-1)*100:.1f}%)", value=curr_p >= high52 * 0.9)
                        st.checkbox(f"S: 수급 양호 (AD 등급: {row['ad_grade']})", value=row['ad_grade'] in ['A', 'B'])
                    
                    with c2:
                        st.markdown("### 🔵 트렌드 템플릿")
                        st.checkbox("주가 > 150MA & 200MA", value=curr_p > ma150 and curr_p > ma200)
                        st.checkbox("150MA > 200MA", value=ma150 > ma200)
                        st.checkbox("50MA > 150MA", value=ma50 > ma150)
                        st.checkbox(f"저가 대비 30%↑", value=curr_p >= low52 * 1.3)
                else:
                    st.error("기술적 분석을 위한 가격 데이터를 가져오지 못했습니다.")

            with t_biz:
                st.write("#### 기업 개요")
                summary = info.get('description', '설명이 없습니다.')
                st.markdown(f'<div class="overview-panel">{summary}</div>', unsafe_allow_html=True)
        else:
            st.info("👈 왼쪽 리스트에서 종목을 선택해 주세요.")
else:
    st.error("데이터베이스를 찾을 수 없거나 데이터가 비어있습니다. 'update_data.py'가 정상적으로 완료되었는지 확인하세요.")
