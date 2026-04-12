# -*- coding: utf-8 -*-
import streamlit as st
import pandas as pd
import sqlite3
import requests
import streamlit.components.v1 as components
import os
import altair as alt

# --- [설정] FMP API 키 ---
FMP_API_KEY = "1kJBflGjsp5fCgbancejhI5bN5iavEJF"

# --- 데이터 유틸리티 ---
def get_data():
    if not os.path.exists('ibd_system.db'): return pd.DataFrame()
    conn = sqlite3.connect('ibd_system.db')
    df = pd.read_sql("SELECT * FROM repo_results", conn)
    conn.close()
    return df

def get_rs_history(ticker):
    conn = sqlite3.connect('ibd_system.db')
    df = pd.read_sql(f"SELECT * FROM rs_history WHERE symbol='{ticker}'", conn)
    conn.close()
    return df

@st.cache_data(ttl=3600)
def get_detailed_info(ticker):
    """FMP API를 통해 상세 정보를 로드합니다."""
    try:
        profile = requests.get(f"https://financialmodelingprep.com/api/v3/profile/{ticker}?apikey={FMP_API_KEY}").json()
        income = requests.get(f"https://financialmodelingprep.com/api/v3/income-statement/{ticker}?period=quarter&limit=8&apikey={FMP_API_KEY}").json()
        prices = requests.get(f"https://financialmodelingprep.com/api/v3/historical-price-full/{ticker}?serietype=line&apikey={FMP_API_KEY}").json()
        
        info = profile[0] if profile else {}
        df_inc = pd.DataFrame(income)
        df_prices = pd.DataFrame(prices.get('historical', []))
        return df_inc, info, df_prices
    except:
        return pd.DataFrame(), {}, pd.DataFrame()

# --- 페이지 설정 및 스타일 ---
st.set_page_config(layout="wide", page_title="Institutional Stock Terminal")
st.markdown("""
<style>
    .stApp { background-color: #161C27 !important; color: #ccd6f6; }
    .stButton > button { width: 100%; height: 32px; font-size: 12px; margin-bottom: 2px; }
    .overview-panel { background: #252b3b; padding: 1.5rem; border-radius: 12px; border: 1px solid #4a5161; line-height: 1.6; }
</style>
""", unsafe_allow_html=True)

df = get_data()

if not df.empty:
    # --- 사이드바 UI (버전 1 스타일 복구) ---
    with st.sidebar:
        st.header("🎛️ Terminal Control")
        
        min_price = st.number_input("최소 주가 ($)", value=10.0)
        rs_min = st.slider("최소 RS 점수", 1, 99, 80)
        ind_rs_min = st.slider("최소 산업군 RS 점수", 1, 99, 50) # 산업군 RS 슬라이더 복구

        def grade_btn_filter(label, session_key):
            if session_key not in st.session_state: st.session_state[session_key] = ["A", "B", "C"]
            st.caption(label)
            cols = st.columns(3)
            grades = ["A", "B", "C", "D", "E", "전체"]
            for i, g in enumerate(grades):
                with cols[i % 3]:
                    is_sel = g in st.session_state[session_key] if g != "전체" else len(st.session_state[session_key]) == 5
                    if st.button(f"{'●' if is_sel else '○'} {g}", key=f"{session_key}_{g}"):
                        if g == "전체": 
                            st.session_state[session_key] = ["A", "B", "C", "D", "E"] if not is_sel else []
                        else:
                            if g in st.session_state[session_key]: st.session_state[session_key].remove(g)
                            else: st.session_state[session_key].append(g)
                        st.rerun()
            return st.session_state[session_key]

        sel_smr = grade_btn_filter("SMR 등급", "smr_sel")
        sel_ad = grade_btn_filter("AD 수급 등급", "ad_sel")

        # 산업군 필터
        all_inds = sorted(df['industry'].unique().tolist())
        sel_ind = st.multiselect("산업군 선택", options=all_inds, default=all_inds)

        # 필터링
        mask = (df['price'] >= min_price) & \
               (df['rs_score'] >= rs_min) & \
               (df['industry_rs_score'] >= ind_rs_min) & \
               (df['smr_grade'].isin(sel_smr)) & \
               (df['ad_grade'].isin(sel_ad)) & \
               (df['industry'].isin(sel_ind))
        f_df = df[mask].sort_values('rs_score', ascending=False)

    # --- 메인 화면 ---
    col_l, col_r = st.columns([4, 3])
    
    with col_l:
        st.subheader(f"Leaders List ({len(f_df)})")
        sel_row = st.dataframe(
            f_df[['symbol', 'price', 'rs_score', 'smr_grade', 'ad_grade', 'industry_rs_score', 'industry']],
            hide_index=True, on_select="rerun", selection_mode="single-row", height=800, use_container_width=True
        )

    with col_r:
        if len(sel_row.selection.rows) > 0:
            target = f_df.iloc[sel_row.selection.rows[0]]
            ticker = target['symbol']
            
            st.markdown(f"## {ticker}")
            st.write(f"**Industry:** {target['industry']} | **Ind RS:** {target['industry_rs_score']}")

            q_inc, info, p_hist = get_detailed_info(ticker)
            t_chart, t_fin, t_check, t_biz = st.tabs(["📊 차트", "🧾 재무제표", "🛡️ 체크리스트", "🏢 개요"])

            with t_chart:
                components.html(f"""
                <script type="text/javascript" src="https://s3.tradingview.com/tv.js"></script>
                <div id="tv_chart" style="height: 500px;"></div>
                <script type="text/javascript">
                new TradingView.widget({{"autosize":true,"symbol":"{ticker}","interval":"D","theme":"dark","style":"1","locale":"kr","container_id":"tv_chart"}});
                </script>
                """, height=500)
                
                # RS 트렌드
                h_df = get_rs_history(ticker)
                if not h_df.empty:
                    c = alt.Chart(h_df).mark_line(color='#64ffda').encode(x='date:T', y='rs_score:Q').properties(height=200)
                    st.altair_chart(c, use_container_width=True)

            with t_fin:
                if not q_inc.empty:
                    st.write("#### 최근 분기 실적 (FMP)")
                    f_disp = q_inc[['date', 'revenue', 'netIncome', 'eps']].head(4)
                    f_disp.columns = ['날짜', '매출액', '순이익', 'EPS']
                    st.table(f_disp.set_index('날짜'))
                else: st.warning("재무 데이터가 없습니다.")

            with t_check:
                if not p_hist.empty:
                    st.subheader("🛡️ 기술적 체크리스트")
                    p_sorted = p_hist.sort_values('date')
                    curr = p_sorted['close'].iloc[-1]
                    ma50 = p_sorted['close'].rolling(50).mean().iloc[-1]
                    ma200 = p_sorted['close'].rolling(200).mean().iloc[-1]
                    high52 = p_sorted['close'].max()
                    
                    st.checkbox("주가 > 50일 이평선", value=curr > ma50)
                    st.checkbox("50일 이평선 > 200일 이평선", value=ma50 > ma200)
                    st.checkbox("52주 신고가 대비 -10% 이내", value=curr >= high52 * 0.9)
                    st.checkbox("RS 점수 80 이상", value=target['rs_score'] >= 80)
                else: st.error("체크리스트용 데이터를 불러오지 못했습니다.")

            with t_biz:
                st.write("#### 기업 개요")
                st.markdown(f'<div class="overview-panel">{info.get("description", "제공된 정보가 없습니다.")}</div>', unsafe_allow_html=True)
        else:
            st.info("👈 리스트에서 종목을 선택해 주세요.")
else:
    st.warning("데이터베이스를 먼저 업데이트해 주세요.")
