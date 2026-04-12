# -*- coding: utf-8 -*-
import streamlit as st
import pandas as pd
import sqlite3
import requests
import streamlit.components.v1 as components
import os
import altair as alt

FMP_API_KEY = "1kJBflGjsp5fCgbancejhI5bN5iavEJF"

def get_data():
    if not os.path.exists('ibd_system.db'): return pd.DataFrame()
    conn = sqlite3.connect('ibd_system.db')
    df = pd.read_sql("SELECT * FROM repo_results", conn)
    conn.close()
    return df

@st.cache_data(ttl=3600)
def get_detailed_info(ticker):
    """상세 정보는 FMP API 사용 (IP 차단 방지)"""
    try:
        profile = requests.get(f"https://financialmodelingprep.com/api/v3/profile/{ticker}?apikey={FMP_API_KEY}").json()
        income = requests.get(f"https://financialmodelingprep.com/api/v3/income-statement/{ticker}?period=quarter&limit=8&apikey={FMP_API_KEY}").json()
        prices = requests.get(f"https://financialmodelingprep.com/api/v3/historical-price-full/{ticker}?serietype=line&apikey={FMP_API_KEY}").json()
        return pd.DataFrame(income), profile[0] if profile else {}, pd.DataFrame(prices.get('historical', []))
    except: return pd.DataFrame(), {}, pd.DataFrame()

# --- 스타일 ---
st.set_page_config(layout="wide", page_title="Market Leaders Terminal")
st.markdown("<style>.stApp { background-color: #161C27 !important; color: #ccd6f6; } .stButton>button { width:100%; height:35px; }</style>", unsafe_allow_html=True)

df = get_data()

if not df.empty:
    with st.sidebar:
        st.header("🎛️ Terminal Control")
        min_p = st.number_input("최소 주가 ($)", value=10.0)
        rs_m = st.slider("최소 RS 점수", 1, 99, 80)
        ind_rs_m = st.slider("최소 산업군 RS", 1, 99, 50)

        def btn_filter(label, key):
            if key not in st.session_state: st.session_state[key] = ["A", "B", "C"]
            st.caption(label)
            cols = st.columns(3)
            grades = ["A", "B", "C", "D", "E", "전체"]
            for i, g in enumerate(grades):
                with cols[i%3]:
                    is_sel = g in st.session_state[key] if g != "전체" else len(st.session_state[key]) == 5
                    if st.button(f"{'●' if is_sel else '○'} {g}", key=f"{key}_{g}"):
                        if g == "전체": st.session_state[key] = ["A","B","C","D","E"] if not is_sel else []
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
            row = f_df.iloc[sel_row.selection.rows[0]]
            ticker = row['symbol']
            st.markdown(f"## {ticker}")
            q_inc, info, p_hist = get_detailed_info(ticker)
            
            t_chart, t_fin, t_check, t_biz = st.tabs(["📊 차트", "🧾 재무제표", "🛡️ 체크리스트", "🏢 개요"])
            with t_chart:
                components.html(f'<script src="https://s3.tradingview.com/tv.js"></script><div id="tv_chart" style="height: 500px;"></div><script>new TradingView.widget({{"autosize":true,"symbol":"{ticker}","theme":"dark","container_id":"tv_chart"}});</script>', height=500)
            with t_fin:
                if not q_inc.empty: st.table(q_inc[['date','revenue','netIncome','eps']].head(4))
            with t_check:
                if not p_hist.empty:
                    curr = p_hist['close'].iloc[0]
                    high52 = p_hist['close'].max()
                    st.checkbox("RS 80↑", value=row['rs_score'] >= 80)
                    st.checkbox("52주 고가 근접", value=curr >= high52 * 0.9)
                    st.checkbox("SMR A/B", value=row['smr_grade'] in ['A','B'])
            with t_biz:
                st.write(info.get('description', '정보 없음'))
