# -*- coding: utf-8 -*-
import streamlit as st
import pandas as pd
import sqlite3
import requests
import streamlit.components.v1 as components
import os

FMP_API_KEY = "1kJBflGjsp5fCgbancejhI5bN5iavEJF"

def init_db():
    conn = sqlite3.connect('ibd_system.db')
    conn.execute("CREATE TABLE IF NOT EXISTS favorites (symbol TEXT PRIMARY KEY)")
    # 기업 정보 마스터 테이블 (산업군, 개요 저장용)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS company_profiles (
            symbol TEXT PRIMARY KEY, 
            industry TEXT, 
            description TEXT
        )
    """)
    conn.close()

def get_data():
    if not os.path.exists('ibd_system.db'): return pd.DataFrame()
    conn = sqlite3.connect('ibd_system.db')
    df = pd.read_sql("SELECT * FROM repo_results", conn)
    conn.close()
    return df

@st.cache_data(ttl=3600)
def get_detailed_info(ticker):
    try:
        # 재무/차트는 API 호출
        i_res = requests.get(f"https://financialmodelingprep.com/api/v3/income-statement/{ticker}?period=quarter&limit=8&apikey={FMP_API_KEY}").json()
        h_res = requests.get(f"https://financialmodelingprep.com/api/v3/historical-price-full/{ticker}?serietype=line&apikey={FMP_API_KEY}").json()
        
        # 개요는 DB 마스터 테이블 우선 확인
        conn = sqlite3.connect('ibd_system.db')
        p_df = pd.read_sql(f"SELECT * FROM company_profiles WHERE symbol='{ticker}'", conn)
        
        if not p_df.empty:
            info = {"description": p_df.iloc[0]['description'], "industry": p_df.iloc[0]['industry']}
        else:
            p_res = requests.get(f"https://financialmodelingprep.com/api/v3/profile/{ticker}?apikey={FMP_API_KEY}").json()
            info = p_res[0] if p_res and not isinstance(p_res, dict) else {}
            if info: # 새로 가져온 건 DB에 저장
                conn.execute("INSERT OR REPLACE INTO company_profiles (symbol, industry, description) VALUES (?, ?, ?)",
                             (ticker, info.get('industry'), info.get('description')))
                conn.commit()
        conn.close()
        return pd.DataFrame(i_res), info, pd.DataFrame(h_res.get('historical', []))
    except: return pd.DataFrame(), {}, pd.DataFrame()

# --- CSS 설정 ---
st.set_page_config(layout="wide", page_title="Market Leaders Terminal")
st.markdown("""
<style>
    .stApp { background-color: #161C27 !important; }
    /* 메인 텍스트 밝게 */
    .block-container p, .block-container span, .block-container h1, .block-container h2, 
    .block-container h3, .block-container h4, .block-container label { color: #FFFFFF !important; }
    
    /* 사이드바 스타일 및 '전체' 글씨 크기 */
    [data-testid="stSidebar"] { background-color: #F8F9FA !important; }
    [data-testid="stSidebar"] p, [data-testid="stSidebar"] span, [data-testid="stSidebar"] label { 
        color: #1E293B !important; font-size: 13px;
    }
    [data-testid="stSidebar"] button div:contains("전체") { font-size: 11px !important; }

    /* 4. 저장 버튼 및 체크박스 명도 대비 수정 */
    .stButton > button { 
        background-color: #FFFFFF !important; color: #1E293B !important; 
        border: 1px solid #CBD5E1 !important; font-weight: bold; 
    }
    /* 데이터프레임 툴바(돋보기 등) 배경 밝게, 아이콘 어둡게 */
    [data-testid="stElementToolbar"] { background-color: #FFFFFF !important; border: 1px solid #DDD; }
    [data-testid="stElementToolbar"] svg { fill: #1E293B !important; }
    
    .overview-panel { background: #2A3143; padding: 1.2rem; border-radius: 8px; color: #FFFFFF !important; }
</style>
""", unsafe_allow_html=True)

init_db()
df = get_data()

if not df.empty:
    with st.sidebar:
        st.header("🎛️ Terminal Control")
        min_p = st.number_input("최소 주가 ($)", value=10.0)
        rs_m = st.slider("최소 RS 점수", 1, 99, 80)
        
        # 3. 산업군 필터 (버튼 + 동그라미 표시)
        with st.expander("🏭 산업군 필터"):
            all_inds = sorted(df['industry'].unique().tolist())
            if 'ind_sel' not in st.session_state: st.session_state.ind_sel = all_inds
            
            c_all = st.columns(1)
            is_all = len(st.session_state.ind_sel) == len(all_inds)
            if st.button(f"{'●' if is_all else '○'} 전체 선택/해제", key="all_ind_btn"):
                st.session_state.ind_sel = [] if is_all else all_inds
                st.rerun()
            
            # 산업군 목록을 2열로 배치
            cols = st.columns(2)
            for idx, ind in enumerate(all_inds):
                with cols[idx % 2]:
                    is_sel = ind in st.session_state.ind_sel
                    if st.button(f"{'●' if is_sel else '○'} {ind[:12]}..", key=f"ind_{ind}", use_container_width=True):
                        if is_sel: st.session_state.ind_sel.remove(ind)
                        else: st.session_state.ind_sel.append(ind)
                        st.rerun()

        # SMR / AD 등급 필터
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

        mask = (df['price'] >= min_p) & (df['rs_score'] >= rs_m) & \
               (df['smr_grade'].isin(smr_sel)) & (df['ad_grade'].isin(ad_sel)) & \
               (df['industry'].isin(st.session_state.ind_sel))
        f_df = df[mask].sort_values('rs_score', ascending=False).copy()

    col_l, col_r = st.columns([4, 3])
    with col_l:
        st.subheader(f"Leaders List ({len(f_df)})")
        sel_row = st.dataframe(f_df[['symbol', 'price', 'rs_score', 'smr_grade', 'ad_grade', 'industry']],
                                hide_index=True, on_select="rerun", selection_mode="single-row", height=800, use_container_width=True)

    with col_r:
        if len(sel_row.selection.rows) > 0:
            target = f_df.iloc[sel_row.selection.rows[0]]
            ticker = target['symbol']
            
            st.markdown(f"## {ticker}")
            if st.button("☆ 관심종목 저장"): pass # 기존 저장 로직 연결
            
            q_inc, info, p_hist = get_detailed_info(ticker)
            t_chart, t_fin, t_check, t_biz = st.tabs(["📊 차트", "🧾 재무", "🛡️ 체크", "🏢 개요"])
            
            with t_fin:
                if not q_inc.empty: st.table(q_inc[['date','revenue','eps']].head(4))
                else: st.error("재무 정보를 불러올 수 없습니다. (API 한도 확인)")
            with t_biz:
                st.markdown(f'<div class="overview-panel">{info.get("description", "정보 없음")}</div>', unsafe_allow_html=True)
        else: st.info("👈 리스트에서 종목을 선택해 주세요.")
