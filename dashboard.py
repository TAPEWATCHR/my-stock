# -*- coding: utf-8 -*-
import streamlit as st
import pandas as pd
import sqlite3
import requests
import streamlit.components.v1 as components
import os

FMP_API_KEY = "1kJBflGjsp5fCgbancejhI5bN5iavEJF"

# --- 데이터베이스 관련 함수 ---
def init_db():
    conn = sqlite3.connect('ibd_system.db')
    # 관심종목 테이블
    conn.execute("CREATE TABLE IF NOT EXISTS favorites (symbol TEXT PRIMARY KEY)")
    # [중요] 기업 프로필(산업군, 개요 등) 캐싱 테이블 추가
    conn.execute("""
        CREATE TABLE IF NOT EXISTS company_profiles (
            symbol TEXT PRIMARY KEY, 
            industry TEXT, 
            description TEXT,
            last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    conn.close()

def get_data():
    if not os.path.exists('ibd_system.db'): return pd.DataFrame()
    conn = sqlite3.connect('ibd_system.db')
    # 기존 repo_results 데이터 가져오기
    df = pd.read_sql("SELECT * FROM repo_results", conn)
    conn.close()
    return df

@st.cache_data(ttl=3600)
def get_detailed_info(ticker):
    """API 호출 및 캐싱 로직"""
    try:
        # 1. 재무제표 및 히스토리컬 데이터 (변동성이 크므로 API 호출)
        i_res = requests.get(f"https://financialmodelingprep.com/api/v3/income-statement/{ticker}?period=quarter&limit=8&apikey={FMP_API_KEY}").json()
        h_res = requests.get(f"https://financialmodelingprep.com/api/v3/historical-price-full/{ticker}?serietype=line&apikey={FMP_API_KEY}").json()
        
        # 2. 기업 개요 (변동성이 적으므로 DB 확인 후 없으면 API 호출)
        conn = sqlite3.connect('ibd_system.db')
        profile_df = pd.read_sql(f"SELECT * FROM company_profiles WHERE symbol='{ticker}'", conn)
        
        if not profile_df.empty:
            info = {"description": profile_df.iloc[0]['description'], "industry": profile_df.iloc[0]['industry']}
        else:
            p_res = requests.get(f"https://financialmodelingprep.com/api/v3/profile/{ticker}?apikey={FMP_API_KEY}").json()
            info = p_res[0] if p_res and not isinstance(p_res, dict) else {}
            if info:
                # DB에 캐싱 저장
                conn.execute("INSERT OR REPLACE INTO company_profiles (symbol, industry, description) VALUES (?, ?, ?)",
                             (ticker, info.get('industry'), info.get('description')))
                conn.commit()
        conn.close()
        
        return pd.DataFrame(i_res), info, pd.DataFrame(h_res.get('historical', []))
    except: 
        return pd.DataFrame(), {}, pd.DataFrame()

# --- 페이지 설정 및 디자인 CSS ---
st.set_page_config(layout="wide", page_title="Market Leaders Terminal")
st.markdown("""
<style>
    .stApp { background-color: #161C27 !important; }
    .block-container p, .block-container span, .block-container h1, .block-container h2, 
    .block-container h3, .block-container h4, .block-container label, .block-container .stMarkdown { 
        color: #FFFFFF !important; 
    }
    
    /* 사이드바 스타일 */
    [data-testid="stSidebar"] { background-color: #F8F9FA !important; min-width: 350px !important; }
    [data-testid="stSidebar"] p, [data-testid="stSidebar"] span, [data-testid="stSidebar"] label { 
        color: #1E293B !important; font-weight: 600; 
    }
    
    /* 1. 사이드바 '전체' 글씨 작게 조정 */
    [data-testid="stSidebar"] button div:contains("전체") {
        font-size: 11px !important;
        transform: scale(0.9);
    }

    /* 3. 명도 대비 조정: 툴바와 버튼 */
    [data-testid="stElementToolbar"] { background-color: #F8F9FA !important; border-radius: 5px; }
    [data-testid="stElementToolbar"] svg { fill: #1E293B !important; }
    
    .main .stButton > button { 
        background-color: #FFFFFF !important; color: #1E293B !important; 
        border: 1px solid #CBD5E1 !important; font-weight: bold;
    }

    .overview-panel { background: #2A3143; padding: 1.5rem; border-radius: 8px; border: 1px solid #5C657A; color: #FFFFFF !important; }
</style>
""", unsafe_allow_html=True)

init_db()
df = get_data()

if not df.empty:
    with st.sidebar:
        st.header("🎛️ Terminal Control")
        min_p = st.number_input("최소 주가 ($)", value=10.0)
        min_adv = st.number_input("최소 거래대금 ($M)", value=2.0)
        rs_m = st.slider("최소 RS 점수", 1, 99, 80)
        
        # 2. 산업군 필터 아코디언 처리
        with st.expander("🏭 산업군 필터 설정"):
            all_inds = sorted(df['industry'].unique().tolist())
            select_all_ind = st.checkbox("전체 산업군 선택", value=True)
            if select_all_ind:
                ind_sel = all_inds
                st.multiselect("산업군 선택 (전체 선택됨)", options=all_inds, default=all_inds, disabled=True)
            else:
                ind_sel = st.multiselect("산업군 선택", options=all_inds, default=[])

        def btn_filter(label, key):
            if key not in st.session_state: st.session_state[key] = ["A", "B", "C"]
            st.caption(label)
            cols = st.columns(3)
            grades = ["A", "B", "C", "D", "E", "전체"]
            for i, g in enumerate(grades):
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
               (df['adv_50'] >= min_adv * 1000000) & \
               (df['smr_grade'].isin(smr_sel)) & (df['ad_grade'].isin(ad_sel)) & \
               (df['industry'].isin(ind_sel))
        
        f_df = df[mask].sort_values('rs_score', ascending=False).copy()
        f_df['ADV($M)'] = (f_df['adv_50'] / 1000000).round(1)

    col_l, col_r = st.columns([4, 3])
    with col_l:
        st.subheader(f"Leaders List ({len(f_df)})")
        sel_row = st.dataframe(f_df[['symbol', 'price', 'ADV($M)', 'rs_score', 'smr_grade', 'ad_grade', 'industry']],
                                hide_index=True, on_select="rerun", selection_mode="single-row", height=800, use_container_width=True)

    with col_r:
        if len(sel_row.selection.rows) > 0:
            target = f_df.iloc[sel_row.selection.rows[0]]
            ticker = target['symbol']
            
            c1, c2 = st.columns([4, 1])
            with c1: st.markdown(f"## {ticker}")
            with c2: 
                # (생략: 저장 기능은 기존과 동일)
                st.button("☆ 저장")
            
            st.write(f"**Industry:** {target['industry']}")
            q_inc, info, p_hist = get_detailed_info(ticker)
            
            t_chart, t_fin, t_check, t_biz = st.tabs(["📊 차트", "🧾 재무제표", "🛡️ 체크리스트", "🏢 개요"])
            
            with t_chart:
                components.html(f'<script src="https://s3.tradingview.com/tv.js"></script><div id="tv_chart" style="height: 500px;"></div><script>new TradingView.widget({{"autosize":true,"symbol":"{ticker}","theme":"dark","container_id":"tv_chart"}});</script>', height=500)

            with t_fin:
                if not q_inc.empty:
                    st.table(q_inc[['date','revenue','netIncome','eps']].head(4))
                else: st.error("⚠️ 데이터를 가져올 수 없습니다. API 한도를 확인하세요.")

            with t_check:
                if not p_hist.empty:
                    st.checkbox("RS 점수 80점 이상", value=target['rs_score'] >= 80, disabled=True)
                    # 추가 체크리스트 로직...
            
            with t_biz:
                desc = info.get("description", "설명 데이터가 없습니다.")
                st.markdown(f'<div class="overview-panel">{desc}</div>', unsafe_allow_html=True)
        else:
            st.info("👈 리스트에서 종목을 선택해 주세요.")
else:
    st.warning("데이터가 없습니다. update_data.py를 실행하세요.")
