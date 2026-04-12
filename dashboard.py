# -*- coding: utf-8 -*-
import streamlit as st
import pandas as pd
import sqlite3
import requests
import streamlit.components.v1 as components
import os
import altair as alt

FMP_API_KEY = "1kJBflGjsp5fCgbancejhI5bN5iavEJF"

# --- 즐겨찾기 함수 ---
def toggle_favorite(ticker):
    conn = sqlite3.connect('ibd_system.db')
    cursor = conn.cursor()
    cursor.execute("CREATE TABLE IF NOT EXISTS favorites (symbol TEXT PRIMARY KEY)")
    cursor.execute("SELECT symbol FROM favorites WHERE symbol = ?", (ticker,))
    if cursor.fetchone(): cursor.execute("DELETE FROM favorites WHERE symbol = ?", (ticker,))
    else: cursor.execute("INSERT INTO favorites (symbol) VALUES (?)", (ticker,))
    conn.commit()
    conn.close()

def get_favorites():
    if not os.path.exists('ibd_system.db'): return []
    conn = sqlite3.connect('ibd_system.db')
    try: return pd.read_sql("SELECT symbol FROM favorites", conn)['symbol'].tolist()
    except: return []
    finally: conn.close()

def get_data():
    if not os.path.exists('ibd_system.db'): return pd.DataFrame()
    conn = sqlite3.connect('ibd_system.db')
    df = pd.read_sql("SELECT * FROM repo_results", conn)
    conn.close()
    return df

@st.cache_data(ttl=3600)
def get_detailed_info(ticker):
    try:
        # FMP JSON 파싱 강화
        p_res = requests.get(f"https://financialmodelingprep.com/api/v3/profile/{ticker}?apikey={FMP_API_KEY}").json()
        i_res = requests.get(f"https://financialmodelingprep.com/api/v3/income-statement/{ticker}?period=quarter&limit=8&apikey={FMP_API_KEY}").json()
        h_res = requests.get(f"https://financialmodelingprep.com/api/v3/historical-price-full/{ticker}?serietype=line&apikey={FMP_API_KEY}").json()
        return pd.DataFrame(i_res), p_res[0] if p_res else {}, pd.DataFrame(h_res.get('historical', []))
    except: return pd.DataFrame(), {}, pd.DataFrame()

# --- 페이지 설정 및 밝은 텍스트 스타일 ---
st.set_page_config(layout="wide", page_title="Market Leaders Terminal")
st.markdown("""
<style>
    .stApp { background-color: #161C27 !important; color: #E0E6ED !important; }
    /* 탭 및 일반 텍스트 밝게 조정 */
    [data-baseweb="tab"], p, span, label, .stMarkdown { color: #FFFFFF !important; font-weight: 500; }
    /* 사이드바 버튼 글씨 크기 1단계 낮춤 */
    .stButton > button { width: 100%; height: 32px; font-size: 11px !important; border-radius: 4px; }
    .overview-panel { background: #252b3b; padding: 1.5rem; border-radius: 12px; border: 1px solid #4a5161; color: #FFFFFF !important; }
    [data-testid="stTable"] td { color: #FFFFFF !important; }
</style>
""", unsafe_allow_html=True)

df = get_data()

if not df.empty:
    with st.sidebar:
        st.header("🎛️ Terminal Control")
        min_p = st.number_input("최소 주가 ($)", value=10.0)
        # 4. 거래대금 항목 복구
        min_adv = st.number_input("최소 거래대금 ($M)", value=2.0)
        rs_m = st.slider("최소 RS 점수", 1, 99, 80)
        ind_rs_m = st.slider("최소 산업군 RS", 1, 99, 50)

        # 5. 버튼 필터 디자인 수정
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
        
        # 1. 산업군 필터 복구
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
            
            # 6. 즐겨찾기 버튼 복구
            c1, c2 = st.columns([4, 1])
            with c1: st.markdown(f"## {ticker}")
            with c2: 
                favs = get_favorites()
                if st.button("⭐ 삭제" if ticker in favs else "☆ 저장"):
                    toggle_favorite(ticker)
                    st.rerun()
            
            st.write(f"**Industry:** {target['industry']} | **Ind RS:** {target['industry_rs_score']}")

            # 7. 데이터 긁어오기 (FMP 연동 확인)
            q_inc, info, p_hist = get_detailed_info(ticker)
            
            # 8. 밝은 텍스트 적용된 탭
            t_chart, t_fin, t_check, t_biz = st.tabs(["📊 차트", "🧾 재무제표", "🛡️ 체크리스트", "🏢 개요"])
            
            with t_chart:
                components.html(f'<script src="https://s3.tradingview.com/tv.js"></script><div id="tv_chart" style="height: 500px;"></div><script>new TradingView.widget({{"autosize":true,"symbol":"{ticker}","theme":"dark","container_id":"tv_chart"}});</script>', height=500)

            with t_fin:
                if not q_inc.empty:
                    st.markdown("### 최근 분기 실적")
                    st.table(q_inc[['date','revenue','netIncome','eps']].head(4).rename(columns={'revenue':'매출', 'netIncome':'순이익'}))
                else: st.info("데이터 로드 중이거나 재무 정보가 없습니다.")

            with t_check:
                if not p_hist.empty:
                    p = p_hist.sort_values('date')
                    curr = p['close'].iloc[-1]
                    high52 = p['close'].max()
                    st.markdown(f"**현재가:** ${curr:.2f} / **52주 고가:** ${high52:.2f}")
                    st.checkbox("RS 80점 이상", value=target['rs_score'] >= 80, disabled=True)
                    st.checkbox("52주 고가 근접 (90% 이상)", value=curr >= high52 * 0.9, disabled=True)
                    st.checkbox("SMR A 또는 B", value=target['smr_grade'] in ['A', 'B'], disabled=True)
                else: st.info("가격 데이터를 불러오는 중입니다.")

            with t_biz:
                st.markdown(f'<div class="overview-panel">{info.get("description", "회원님의 API 한도 초과이거나 설명이 없는 종목입니다.")}</div>', unsafe_allow_html=True)
        else:
            st.info("👈 리스트에서 종목을 선택해 주세요.")
else:
    st.warning("데이터베이스가 비어있습니다. 'update_data.py'를 먼저 실행하세요.")
