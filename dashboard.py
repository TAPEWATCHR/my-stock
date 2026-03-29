# -*- coding: utf-8 -*-
import streamlit as st
import pandas as pd
import sqlite3
import yfinance as yf
import streamlit.components.v1 as components
import os
import altair as alt
import time

# --- [추가] 즐겨찾기 관련 DB 함수 ---
def init_fav_db():
    """즐겨찾기 테이블 초기화"""
    conn = sqlite3.connect('ibd_system.db')
    cursor = conn.cursor()
    cursor.execute("CREATE TABLE IF NOT EXISTS favorites (symbol TEXT PRIMARY KEY)")
    conn.commit()
    conn.close()

def toggle_favorite(ticker):
    """즐겨찾기 추가/삭제 토글"""
    conn = sqlite3.connect('ibd_system.db')
    cursor = conn.cursor()
    cursor.execute("SELECT symbol FROM favorites WHERE symbol = ?", (ticker,))
    if cursor.fetchone():
        cursor.execute("DELETE FROM favorites WHERE symbol = ?", (ticker,))
    else:
        cursor.execute("INSERT INTO favorites (symbol) VALUES (?)", (ticker,))
    conn.commit()
    conn.close()

def get_favorites():
    """즐겨찾기 목록 가져오기"""
    if not os.path.exists('ibd_system.db'):
        return []
    conn = sqlite3.connect('ibd_system.db')
    df_fav = pd.read_sql("SELECT symbol FROM favorites", conn)
    conn.close()
    return df_fav['symbol'].tolist()

# DB 초기화 실행
init_fav_db()

class YFDataFetchError(Exception):
    pass

@st.cache_data(ttl=86400)
def translate_to_korean(text):
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
.overview-panel {{ background: {OVERVIEW_BG}; color: {OVERVIEW_TEXT}; padding: 1.5rem 1.75rem; border-radius: 12px; border: 1px solid #4a5161; line-height: 1.7; font-size: 0.95rem; }}
.overview-panel h2 {{ color: #64ffda !important; margin-bottom: 1rem; }}
[data-testid="stSidebar"] .stButton > button {{ width: auto !important; min-width: 0 !important; padding: 0.1rem 0.5rem !important; font-size: 0.75rem !important; line-height: 1.2 !important; min-height: 24px !important; white-space: nowrap; }}
[data-testid="stSidebar"] button[kind="primary"] {{ color: #ff4b4b !important; border-color: #ff4b4b !important; background-color: transparent !important; }}
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
        query = f"SELECT * FROM rs_history WHERE symbol = '{ticker}' ORDER BY date ASC"
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
    max_retries = 3
    for attempt in range(max_retries):
        try:
            s = yf.Ticker(ticker)
            info = s.info
            q_inc = s.quarterly_income_stmt
            a_inc = s.income_stmt
            q_bal = s.quarterly_balance_sheet
            a_bal = s.balance_sheet
            return q_inc, a_inc, q_bal, a_bal, info
        except Exception as e:
            if attempt < max_retries - 1:
                time.sleep(2)
            else:
                raise YFDataFetchError(f"Rate Limit or Fetch Error: {str(e)}")

# --- 3. 메인 화면 ---
df = get_data()
if not df.empty:
    with st.sidebar:
        st.header("🎛️ Terminal Control")

        with st.expander("🔍 필터 설정", expanded=True):
            # [추가] 즐겨찾기 전용 필터
            show_only_favs = st.checkbox("⭐ 즐겨찾기만 보기", value=False)
            
            min_price = st.number_input("최소 주가 ($)", min_value=0.0, value=10.0, step=1.0)
            min_adv_m = st.number_input("최소 50일 평균 거래대금 ($M)", min_value=0.0, value=2.0, step=0.5)
            rs_min = st.slider("최소 RS 점수", 1, 99, 80)
            ind_rs_min = st.slider("최소 산업군 RS", 1, 99, 50)

            # SMR 등급 로직 (기존 유지)
            if "smr_sel" not in st.session_state: st.session_state.smr_sel = ["A", "B"]
            st.caption("SMR 등급")
            smr_cols1 = st.columns(3)
            for i, g in enumerate(["A", "B", "C"]):
                with smr_cols1[i]:
                    sel_b = g in st.session_state.smr_sel
                    if st.button(f"{'●' if sel_b else '○'} {g}", key=f"smr_{g}", type="primary" if sel_b else "secondary"):
                        st.session_state.smr_sel = [x for x in st.session_state.smr_sel if x != g] if sel_b else sorted(st.session_state.smr_sel + [g])
                        st.rerun()
            # (중략된 UI 로직들은 기존과 동일)
            smr_f = st.session_state.smr_sel

            st.divider()
            # AD 등급 로직 (기존 유지)
            if "ad_sel" not in st.session_state: st.session_state.ad_sel = ["A", "B", "C"]
            ad_f = st.session_state.ad_sel

        with st.expander("🏢 산업군 필터"):
            all_ind = sorted(df['industry'].unique())
            if "industry_sel" not in st.session_state: st.session_state.industry_sel = [s for s in all_ind if s != 'Unknown']
            sel_ind = st.session_state.industry_sel

        # 필터 마스크 적용
        mask = (df['price'] >= min_price) & \
               (df['adv_50'] >= min_adv_m * 1_000_000) & \
               (df['rs_score'] >= rs_min) & \
               (df['industry_rs_score'] >= ind_rs_min) & \
               (df['smr_grade'].isin(smr_f)) & (df['ad_grade'].isin(ad_f)) & (df['industry'].isin(sel_ind))
        
        # [추가] 즐겨찾기 필터 마스크 추가
        if show_only_favs:
            fav_list = get_favorites()
            mask = mask & (df['symbol'].isin(fav_list))
        
        f_df = df[mask].sort_values('rs_score', ascending=False)

    col_l, col_r = st.columns([4, 3])
    with col_l:
        st.subheader(f"Leaders ({len(f_df)})")
        display_list = f_df.copy()
        display_list['ADV($M)'] = (display_list['adv_50'] / 1_000_000).round(1)
        # Ticker 열 앞에 즐겨찾기 표시를 붙여줌
        fav_list = get_favorites()
        display_list['Ticker'] = display_list['symbol'].apply(lambda x: f"⭐ {x}" if x in fav_list else x)
        
        sel = st.dataframe(
            display_list[['Ticker', 'price', 'ADV($M)', 'rs_score', 'smr_grade', 'ad_grade', 'industry_rs_score', 'industry']],
            hide_index=True, on_select="rerun", selection_mode="single-row", height=850,
            use_container_width=True
        )

    with col_r:
        if len(sel.selection.rows) > 0:
            row = f_df.iloc[sel.selection.rows[0]]
            ticker = row['symbol']

            # --- [추가] 즐겨찾기 버튼 영역 ---
            fav_list = get_favorites()
            is_fav = ticker in fav_list
            
            c_header, c_fav = st.columns([4, 1])
            with c_header:
                st.markdown(f"### {ticker} 상세 정보")
            with c_fav:
                fav_btn_label = "⭐ 제거" if is_fav else "☆ 추가"
                if st.button(fav_btn_label, use_container_width=True):
                    toggle_favorite(ticker)
                    st.rerun()

            st.markdown(f"**RS** {row['rs_score']} · **SMR** {row['smr_grade']} · **AD** {row['ad_grade']} · {row['industry']}")

            try:
                with st.spinner(f"'{ticker}' 로드 중..."):
                    q_inc, a_inc, q_bal, a_bal, info = get_detailed_info(ticker)
                
                t_chart, t_fin, t_check, t_biz = st.tabs(["📊 차트", "🧾 재무제표", "🛡️ 체크리스트", "🏢 개요"])
                # ... (기존 탭 내부 로직 동일) ...
                with t_biz:
                    st.subheader("🏢 개요")
                    summary_ko = translate_to_korean(info.get('longBusinessSummary', 'N/A'))
                    st.markdown(f'<div class="overview-panel"><h2>{info.get("longName", ticker)}</h2><p>{summary_ko}</p></div>', unsafe_allow_html=True)

            except YFDataFetchError:
                st.error("🚨 야후 파이낸스 서버 차단됨.")
        else:
            st.info("👈 왼쪽 리스트에서 종목을 선택해 주세요.")
