# -*- coding: utf-8 -*-
# TAPEWATCHR/my-stock 대시보드 개선판
# 1) 사이드바 필터 UX  2) 테이블/재무 스타일  3) 개요 가독성  4) RS 차트 Y축 0~100 고정
# + 추가 개선: 공백 추가, 선택 시 빨간색 강조, 버튼 크기 축소, 반응형 넓이 적용

import streamlit as st
import pandas as pd
import sqlite3
import yfinance as yf
import streamlit.components.v1 as components
import os
import altair as alt

@st.cache_data(ttl=86400)
def translate_to_korean(text):
    """영문 개요를 한국어로 번역 (deep_translator 사용)"""
    if not text or text == "N/A" or len(text.strip()) < 10:
        return text
    try:
        from deep_translator import GoogleTranslator
        chunk_size = 4500
        out = []
        for i in range(0, len(text), chunk_size):
            chunk = text[i:i + chunk_size]
            out.append(GoogleTranslator(source="auto", target="ko").translate(chunk))
        return " ".join(out)
    except Exception:
        return text

# --- 0. 페이지 설정 ---
st.set_page_config(layout="wide", page_title="Institutional Stock Terminal")

BG_COLOR = "#161C27"
TABLE_BG_COLOR = "#363C4C"
OVERVIEW_BG = "#252b3b"
OVERVIEW_TEXT = "#e6eaf0"

st.markdown(f"""
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;600;800&family=JetBrains+Mono:wght@400;500&display=swap');
.stApp {{ background-color: {BG_COLOR} !important; font-family: 'Inter', sans-serif; }}
h1, h2, h3, h4, h5, h6, p, label, span, .stCheckbox {{ color: #ccd6f6 !important; }}

[data-testid="stDataFrame"] {{ background-color: {TABLE_BG_COLOR} !important; }}
.metric-card {{ background-color: {TABLE_BG_COLOR}; border-radius: 12px; padding: 22px; border: 1px solid #4a5161; text-align: center; }}
.metric-label {{ color: #aeb9cc !important; font-size: 0.75rem; font-weight: 600; text-transform: uppercase; }}
.metric-value {{ font-size: 1.8rem; font-weight: 800; color: #64ffda !important; }}

[data-testid="stDataFrame"] {{ border-radius: 10px; overflow: hidden; border: 1px solid #4a5161; font-size: 0.9rem; width: 100% !important; }}
[data-testid="stDataFrame"] th {{ background: #2d3340 !important; color: #64ffda !important; font-weight: 600; padding: 10px 12px !important; }}
[data-testid="stDataFrame"] td {{ padding: 8px 12px !important; color: #ccd6f6; }}
[data-testid="stDataFrame"] tr:hover td {{ background: #3d4354 !important; }}

.overview-panel {{ background: {OVERVIEW_BG}; color: {OVERVIEW_TEXT}; padding: 1.5rem 1.75rem; border-radius: 12px; border: 1px solid #4a5161; line-height: 1.7; font-size: 0.95rem; }}
.overview-panel h2 {{ color: #64ffda !important; margin-bottom: 1rem; }}
.overview-panel p {{ color: {OVERVIEW_TEXT} !important; }}

/* 3. 사이드바 선택칸: 크기 1/3 수준으로 대폭 축소 */
[data-testid="stSidebar"] .stButton > button {{
  width: auto !important; /* 가로 길이를 글자 크기에 맞춤 */
  min-width: 0 !important;
  padding: 0.1rem 0.5rem !important; /* 패딩 최소화 */
  font-size: 0.75rem !important;
  line-height: 1.2 !important;
  min-height: 24px !important; /* 세로 높이 축소 */
  white-space: nowrap;
}}

/* 2. 선택 시 글자색 빨간색 활성화 (Primary 버튼 CSS 커스텀) */
[data-testid="stSidebar"] button[kind="primary"] {{
    color: #ff4b4b !important; /* 빨간색 텍스트 */
    border-color: #ff4b4b !important; /* 테두리 빨간색 */
    background-color: transparent !important; /* 배경 투명 */
}}
[data-testid="stSidebar"] button[kind="primary"]:hover {{
    color: #ff7676 !important;
    border-color: #ff7676 !important;
}}

/* 4. 사이드바 폭 유연하게 조정 (강제 너비 삭제하여 숨김 시 원활하게 확장됨) */
[data-testid="stSidebar"] [data-testid="stVerticalBlock"] > div {{ width: 100% !important; min-width: 0 !important; }}

</style>
""", unsafe_allow_html=True)

# --- 2. 유틸리티 함수 ---
FIN_MAP = {
    'Total Revenue': '매출액', 'Operating Income': '영업이익', 'Net Income': '당기순이익',
    'EBITDA': 'EBITDA', 'Basic EPS': 'EPS', 'Total Assets': '총 자산',
    'Total Liabilities Net Minority Interest': '총 부채', 'Stockholders Equity': '총 자본'
}

def get_data():
    if not os.path.exists('ibd_system.db'):
        return pd.DataFrame()
    conn = sqlite3.connect('ibd_system.db')
    df = pd.read_sql("SELECT * FROM repo_results", conn)
    conn.close()
    if 'adv_50' not in df.columns:
        df['adv_50'] = 0.0
    return df

def get_rs_history(ticker):
    if not os.path.exists('ibd_system.db'):
        return pd.DataFrame()
    conn = sqlite3.connect('ibd_system.db')
    try:
        query = f"SELECT date, rs_score FROM rs_history WHERE symbol = '{ticker}' ORDER BY date ASC"
        hist_df = pd.read_sql(query, conn)
    except Exception:
        hist_df = pd.DataFrame()
    conn.close()
    return hist_df

def format_date_idx(idx, type='Q'):
    if type == 'Q':
        return [f"{i.year} Q{(i.month-1)//3 + 1}" if hasattr(i, 'year') else str(i) for i in idx]
    return [str(i.year) if hasattr(i, 'year') else str(i) for i in idx]

def calc_growth(series, periods):
    if series is None or series.empty:
        return pd.Series(dtype=float)
    s = series.sort_index(ascending=True)
    growth = ((s - s.shift(periods)) / s.shift(periods).abs()) * 100
    return growth.sort_index(ascending=False)

@st.cache_data(ttl=3600)
def get_detailed_info(ticker):
    s = yf.Ticker(ticker)
    return s.quarterly_income_stmt, s.income_stmt, s.quarterly_balance_sheet, s.balance_sheet, s.info

# --- 3. 메인 화면 ---
df = get_data()
if not df.empty:
    with st.sidebar:
        st.header("🎛️ Terminal Control")

        with st.expander("🔍 필터 설정", expanded=True):
            min_price = st.number_input("최소 주가 ($)", min_value=0.0, value=10.0, step=1.0)
            min_adv_m = st.number_input("최소 50일 평균 거래대금 ($M)", min_value=0.0, value=2.0, step=0.5)
            rs_min = st.slider("최소 RS 점수", 1, 99, 80)
            ind_rs_min = st.slider("최소 산업군 RS", 1, 99, 50)

            # SMR 등급: 1. 공백 추가, 2. 선택 시 type="primary"로 빨간색 적용
            if "smr_sel" not in st.session_state:
                st.session_state.smr_sel = ["A", "B"]
            st.caption("SMR 등급")
            smr_cols1 = st.columns(3)
            for i, g in enumerate(["A", "B", "C"]):
                with smr_cols1[i]:
                    sel = g in st.session_state.smr_sel
                    lbl = f"● {g}" if sel else f"○ {g}"  # 공백 추가
                    btn_type = "primary" if sel else "secondary" # 선택 시 primary 클래스 부여
                    if st.button(lbl, key=f"smr_{g}", type=btn_type):
                        if g in st.session_state.smr_sel:
                            st.session_state.smr_sel = [x for x in st.session_state.smr_sel if x != g]
                        else:
                            st.session_state.smr_sel = sorted(st.session_state.smr_sel + [g])
                        st.rerun()
            smr_cols2 = st.columns(3)
            for i, g in enumerate(["D", "E", "전체"]):
                with smr_cols2[i]:
                    sel = g in st.session_state.smr_sel if g != "전체" else set(st.session_state.smr_sel) == {"A","B","C","D","E"}
                    lbl = f"● {g}" if sel else f"○ {g}" # 공백 추가
                    btn_type = "primary" if sel else "secondary"
                    if st.button(lbl, key=f"smr_{g}", type=btn_type):
                        if g == "전체":
                            st.session_state.smr_sel = ["A","B","C","D","E"] if len(st.session_state.smr_sel) < 5 else []
                        else:
                            if g in st.session_state.smr_sel:
                                st.session_state.smr_sel = [x for x in st.session_state.smr_sel if x != g]
                            else:
                                st.session_state.smr_sel = sorted(st.session_state.smr_sel + [g])
                        st.rerun()
            smr_f = st.session_state.smr_sel

            st.divider()

            # 수급(AD) 등급: 1. 공백 추가, 2. 선택 시 type="primary" 적용
            if "ad_sel" not in st.session_state:
                st.session_state.ad_sel = ["A", "B", "C"]
            st.caption("수급(AD) 등급")
            ad_cols1 = st.columns(3)
            for i, g in enumerate(["A", "B", "C"]):
                with ad_cols1[i]:
                    sel = g in st.session_state.ad_sel
                    lbl = f"● {g}" if sel else f"○ {g}" # 공백 추가
                    btn_type = "primary" if sel else "secondary"
                    if st.button(lbl, key=f"ad_{g}", type=btn_type):
                        if g in st.session_state.ad_sel:
                            st.session_state.ad_sel = [x for x in st.session_state.ad_sel if x != g]
                        else:
                            st.session_state.ad_sel = sorted(st.session_state.ad_sel + [g])
                        st.rerun()
            ad_cols2 = st.columns(3)
            for i, g in enumerate(["D", "E", "전체"]):
                with ad_cols2[i]:
                    sel = g in st.session_state.ad_sel if g != "전체" else set(st.session_state.ad_sel) == {"A","B","C","D","E"}
                    lbl = f"● {g}" if sel else f"○ {g}" # 공백 추가
                    btn_type = "primary" if sel else "secondary"
                    if st.button(lbl, key=f"ad_{g}", type=btn_type):
                        if g == "전체":
                            st.session_state.ad_sel = ["A","B","C","D","E"] if len(st.session_state.ad_sel) < 5 else []
                        else:
                            if g in st.session_state.ad_sel:
                                st.session_state.ad_sel = [x for x in st.session_state.ad_sel if x != g]
                            else:
                                st.session_state.ad_sel = sorted(st.session_state.ad_sel + [g])
                        st.rerun()
            ad_f = st.session_state.ad_sel

        st.divider()
        with st.expander("🏢 산업군 필터"):
            all_sec = sorted(df['sector'].unique())
            if "sector_sel" not in st.session_state:
                st.session_state.sector_sel = [s for s in all_sec if s != 'Unknown']
            all_sel = set(st.session_state.sector_sel) == set(all_sec)
            lbl_sec_all = "● 전체" if all_sel else "○ 전체" # 공백 추가
            btn_type_all = "primary" if all_sel else "secondary"
            if st.button(lbl_sec_all, key="sec_all", type=btn_type_all):
                st.session_state.sector_sel = list(all_sec) if not all_sel else []
                st.rerun()
            st.caption("산업군 (클릭하여 선택/해제)")
            n_col = 2
            for j in range(0, len(all_sec), n_col):
                cols = st.columns(n_col)
                for k in range(n_col):
                    idx = j + k
                    if idx < len(all_sec):
                        s = all_sec[idx]
                        with cols[k]:
                            sel = s in st.session_state.sector_sel
                            lbl = f"● {s}" if sel else f"○ {s}" # 공백 추가
                            btn_type = "primary" if sel else "secondary"
                            if st.button(lbl, key=f"sec_{s}", type=btn_type):
                                if s in st.session_state.sector_sel:
                                    st.session_state.sector_sel = [x for x in st.session_state.sector_sel if x != s]
                                else:
                                    st.session_state.sector_sel = sorted(st.session_state.sector_sel + [s])
                                st.rerun()
            sel_sec = st.session_state.sector_sel

        mask = (df['price'] >= min_price) & \
               (df['adv_50'] >= min_adv_m * 1_000_000) & \
               (df['rs_score'] >= rs_min) & \
               (df['industry_rs_score'] >= ind_rs_min) & \
               (df['smr_grade'].isin(smr_f)) & (df['ad_rating'].isin(ad_f)) & (df['sector'].isin(sel_sec))
        f_df = df[mask].sort_values('rs_score', ascending=False)

    col_l, col_r = st.columns([4, 3])
    with col_l:
        st.subheader(f"Leaders ({len(f_df)})")
        display_list = f_df.copy()
        display_list['ADV($M)'] = (display
