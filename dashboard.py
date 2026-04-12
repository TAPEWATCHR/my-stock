# -*- coding: utf-8 -*-
import streamlit as st
import pandas as pd
import sqlite3
import requests
import streamlit.components.v1 as components
import os
import altair as alt

FMP_API_KEY = "1kJBflGjsp5fCgbancejhI5bN5iavEJF"

# --- 즐겨찾기 로직 ---
def init_db():
    conn = sqlite3.connect('ibd_system.db')
    conn.execute("CREATE TABLE IF NOT EXISTS favorites (symbol TEXT PRIMARY KEY)")
    conn.close()

def toggle_fav(ticker):
    conn = sqlite3.connect('ibd_system.db')
    cur = conn.cursor()
    cur.execute("SELECT symbol FROM favorites WHERE symbol=?", (ticker,))
    if cur.fetchone(): cur.execute("DELETE FROM favorites WHERE symbol=?", (ticker,))
    else: cur.execute("INSERT INTO favorites VALUES (?)", (ticker,))
    conn.commit()
    conn.close()

def get_favs():
    try:
        conn = sqlite3.connect('ibd_system.db')
        df = pd.read_sql("SELECT symbol FROM favorites", conn)
        conn.close()
        return df['symbol'].tolist()
    except: return []

# --- 페이지 설정 ---
st.set_page_config(layout="wide", page_title="Market Leaders Terminal")
st.markdown("""
<style>
    .stApp { background-color: #161C27 !important; color: #FFFFFF !important; }
    /* 텍스트 밝기 최대화 */
    h1, h2, h3, h4, h5, p, span, label, div { color: #FFFFFF !important; }
    .stMarkdown, [data-baseweb="tab"] { color: #FFFFFF !important; }
    /* 버튼 글씨 크기 조정 */
    .stButton > button { width: 100%; font-size: 11px !important; height: 32px; color: #FFFFFF !important; }
    .overview-panel { background: #252b3b; padding: 1.5rem; border-radius: 10px; border: 1px solid #4a5161; color: #FFFFFF !important; }
    [data-testid="stTable"] { background-color: #252b3b; }
    [data-testid="stTable"] td { color: #FFFFFF !important; }
</style>
""", unsafe_allow_html=True)

init_db()
conn = sqlite3.connect('ibd_system.db')
try: df = pd.read_sql("SELECT * FROM repo_results", conn)
except: df = pd.DataFrame()
conn.close()

if not df.empty:
    with st.sidebar:
        st.header("🎛️ Terminal Control")
        min_p = st.number_input("최소 주가 ($)", value=10.0)
        min_adv = st.number_input("최소 거래대금 ($M)", value=2.0)
        rs_m = st.slider("최소 RS 점수", 1, 99, 80)
        ind_rs_m = st.slider("최소 산업군 RS", 1, 99, 50)

        def btn_filter(label, key):
            if key not in st.session_state: st.session_state[key] = ["A", "B", "C"]
            st.caption(label)
            cols = st.columns(3)
            for i, g in enumerate(["A", "B", "C", "D", "E", "전체"]):
                with cols[i%3]:
                    sel = g in st.session_state[key] if g != "전체" else len(st.session_state[key]) == 5
                    if st.button(f"{'●' if sel else '○'} {g}", key=f"{key}_{g}"):
                        if g == "전체": st.session_state[key] = ["A","B","C","D","E"] if not sel else []
                        else:
                            if g in st.session_state[key]: st.session_state[key].remove(g)
                            else: st.session_state[key].append(g)
                        st.rerun()
            return st.session_state[key]

        smr_sel = btn_filter("SMR 등급", "smr_sel")
        ad_sel = btn_filter("AD 수급 등급", "ad_sel")
        
        all_inds = sorted(df['industry'].unique().tolist())
        ind_sel = st.multiselect("산업군 선택", options=all_inds, default=all_inds)

        mask = (df['price'] >= min_p) & (df['rs_score'] >= rs_m) & \
               (df['adv_50'] >= min_adv * 1000000) & \
               (df['industry_rs_score'] >= ind_rs_m) & \
               (df['smr_grade'].isin(smr_sel)) & (df['ad_grade'].isin(ad_sel)) & \
               (df['industry'].isin(ind_sel))
        f_df = df[mask].sort_values('rs_score', ascending=False)

    col_l, col_r = st.columns([4, 3])
    with col_l:
        st.subheader(f"Leaders List ({len(f_df)})")
        sel_row = st.dataframe(f_df[['symbol', 'price', 'rs_score', 'smr_grade', 'ad_grade', 'industry_rs_score', 'industry']],
                               hide_index=True, on_select="rerun", selection_mode="single-row", height=800, use_container_width=True)

    with col_r:
        if len(sel_row.selection.rows) > 0:
            target = f_df.iloc[sel_row.selection.rows[0]]
            ticker = target['symbol']
            
            c1, c2 = st.columns([4, 1])
            with c1: st.markdown(f"## {ticker}")
            with c2: 
                if st.button("⭐ 저장/삭제"):
                    toggle_fav(ticker)
                    st.rerun()
            
            st.write(f"**Industry:** {target['industry']} | **Ind RS:** {target['industry_rs_score']}")

            # FMP 상세 데이터 로드
            try:
                prof = requests.get(f"https://financialmodelingprep.com/api/v3/profile/{ticker}?apikey={FMP_API_KEY}").json()
                inc = requests.get(f"https://financialmodelingprep.com/api/v3/income-statement/{ticker}?period=quarter&limit=4&apikey={FMP_API_KEY}").json()
                price_h = requests.get(f"https://financialmodelingprep.com/api/v3/historical-price-full/{ticker}?serietype=line&apikey={FMP_API_KEY}").json()
                
                t_chart, t_fin, t_check, t_biz = st.tabs(["📊 차트", "🧾 재무제표", "🛡️ 체크리스트", "🏢 개요"])
                
                with t_chart:
                    components.html(f'<script src="https://s3.tradingview.com/tv.js"></script><div id="tv_chart" style="height: 500px;"></div><script>new TradingView.widget({{"autosize":true,"symbol":"{ticker}","theme":"dark","container_id":"tv_chart"}});</script>', height=500)

                with t_fin:
                    if inc:
                        st.write("#### 최근 분기 실적")
                        st.table(pd.DataFrame(inc)[['date','revenue','netIncome','eps']].rename(columns={'revenue':'매출', 'netIncome':'순이익'}))
                    else: st.info("재무 데이터가 없습니다.")

                with t_check:
                    if price_h.get('historical'):
                        p_df = pd.DataFrame(price_h['historical'])
                        curr = p_df['close'].iloc[0]
                        high52 = p_df['close'].max()
                        st.checkbox("RS 80점 이상", value=target['rs_score'] >= 80, disabled=True)
                        st.checkbox("52주 고가 근접 (90%)", value=curr >= high52 * 0.9, disabled=True)
                        st.checkbox("수급 양호 (AD A/B)", value=target['ad_grade'] in ['A','B'], disabled=True)
                    else: st.info("가격 데이터를 불러올 수 없습니다.")

                with t_biz:
                    desc = prof[0].get('description', '설명이 없습니다.') if prof else "정보 없음"
                    st.markdown(f'<div class="overview-panel">{desc}</div>', unsafe_allow_html=True)
            except: st.error("상세 데이터를 가져오는 중 오류가 발생했습니다. (API 한도 초과 등)")
        else:
            st.info("👈 리스트에서 종목을 선택해 주세요.")
else:
    st.error("데이터베이스를 먼저 업데이트하세요.")
