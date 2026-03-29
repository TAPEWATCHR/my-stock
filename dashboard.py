# -*- coding: utf-8 -*-
import streamlit as st
import pandas as pd
import sqlite3
import yfinance as yf
import streamlit.components.v1 as components
import os
import altair as alt
import time

# --- [1] 즐겨찾기 데이터베이스 유틸리티 ---
def init_fav_db():
    """즐겨찾기 전용 테이블 생성"""
    conn = sqlite3.connect('ibd_system.db')
    cursor = conn.cursor()
    cursor.execute("CREATE TABLE IF NOT EXISTS favorites (symbol TEXT PRIMARY KEY)")
    conn.commit()
    conn.close()

def toggle_favorite(ticker):
    """즐겨찾기 추가/삭제 토글 기능"""
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
    """저장된 즐겨찾기 목록 리스트 반환"""
    if not os.path.exists('ibd_system.db'):
        return []
    conn = sqlite3.connect('ibd_system.db')
    df_fav = pd.read_sql("SELECT symbol FROM favorites", conn)
    conn.close()
    return df_fav['symbol'].tolist()

# DB 초기화
init_fav_db()

# 데이터 페치 실패 시 커스텀 에러
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

# --- [2] 페이지 설정 및 스타일 ---
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

# --- [3] 유틸리티 함수 ---
FIN_MAP = {
    'Total Revenue': '매출액', 'Operating Income': '영업이익', 'Net Income': '당기순이익',
    'EBITDA': 'EBITDA', 'Basic EPS': 'EPS', 'Total Assets': '총 자산',
    'Total Liabilities Net Minority Interest': '총 부채', 'Stockholders Equity': '총 자본'
}

def get_data():
    if not os.path.exists('ibd_system.db'): return pd.DataFrame()
    conn = sqlite3.connect('ibd_system.db')
    df = pd.read_sql("SELECT * FROM repo_results", conn)
    conn.close()
    if 'adv_50' not in df.columns: df['adv_50'] = 0.0
    return df

def get_rs_history(ticker):
    conn = sqlite3.connect('ibd_system.db')
    try:
        hist_df = pd.read_sql(f"SELECT * FROM rs_history WHERE symbol = '{ticker}' ORDER BY date ASC", conn)
    except: hist_df = pd.DataFrame()
    conn.close()
    return hist_df

def format_date_idx(idx, type='Q'):
    if type == 'Q': return [f"{i.year} Q{(i.month-1)//3 + 1}" if hasattr(i, 'year') else str(i) for i in idx]
    return [str(i.year) if hasattr(i, 'year') else str(i) for i in idx]

def calc_growth(series, periods):
    if series is None or series.empty: return pd.Series(dtype=float)
    s = series.sort_index(ascending=True)
    return (((s - s.shift(periods)) / s.shift(periods).abs()) * 100).sort_index(ascending=False)

@st.cache_data(ttl=3600)
def get_detailed_info(ticker):
    max_retries = 3
    for attempt in range(max_retries):
        try:
            s = yf.Ticker(ticker)
            return s.quarterly_income_stmt, s.income_stmt, s.quarterly_balance_sheet, s.balance_sheet, s.info
        except:
            if attempt < max_retries - 1: time.sleep(2)
            else: raise YFDataFetchError("Fetch Error")

# --- [4] 메인 화면 로직 ---
df = get_data()
if not df.empty:
    with st.sidebar:
        st.header("🎛️ Terminal Control")

        with st.expander("🔍 필터 설정", expanded=True):
            # 즐겨찾기 필터
            show_only_favs = st.checkbox("⭐ 즐겨찾기 종목만 보기", value=False)
            
            min_price = st.number_input("최소 주가 ($)", min_value=0.0, value=10.0, step=1.0)
            min_adv_m = st.number_input("최소 거래대금 ($M)", min_value=0.0, value=2.0, step=0.5)
            rs_min = st.slider("최소 RS 점수", 1, 99, 80)
            ind_rs_min = st.slider("최소 산업군 RS", 1, 99, 50)

            # SMR 등급 (복구됨)
            if "smr_sel" not in st.session_state: st.session_state.smr_sel = ["A", "B"]
            st.caption("SMR 등급")
            for row_grades in [["A", "B", "C"], ["D", "E", "전체"]]:
                cols = st.columns(3)
                for i, g in enumerate(row_grades):
                    with cols[i]:
                        if g == "전체":
                            is_all = set(st.session_state.smr_sel) == {"A","B","C","D","E"}
                            if st.button(f"{'●' if is_all else '○'} 전체", key="smr_all", type="primary" if is_all else "secondary"):
                                st.session_state.smr_sel = ["A","B","C","D","E"] if not is_all else []
                                st.rerun()
                        else:
                            sel = g in st.session_state.smr_sel
                            if st.button(f"{'●' if sel else '○'} {g}", key=f"smr_{g}", type="primary" if sel else "secondary"):
                                if sel: st.session_state.smr_sel.remove(g)
                                else: st.session_state.smr_sel = sorted(st.session_state.smr_sel + [g])
                                st.rerun()
            
            st.divider()

            # AD 등급 (복구됨)
            if "ad_sel" not in st.session_state: st.session_state.ad_sel = ["A", "B", "C"]
            st.caption("수급(AD) 등급")
            for row_grades in [["A", "B", "C"], ["D", "E", "전체"]]:
                cols = st.columns(3)
                for i, g in enumerate(row_grades):
                    with cols[i]:
                        if g == "전체":
                            is_all = set(st.session_state.ad_sel) == {"A","B","C","D","E"}
                            if st.button(f"{'●' if is_all else '○'} 전체", key="ad_all", type="primary" if is_all else "secondary"):
                                st.session_state.ad_sel = ["A","B","C","D","E"] if not is_all else []
                                st.rerun()
                        else:
                            sel = g in st.session_state.ad_sel
                            if st.button(f"{'●' if sel else '○'} {g}", key=f"ad_{g}", type="primary" if sel else "secondary"):
                                if sel: st.session_state.ad_sel.remove(g)
                                else: st.session_state.ad_sel = sorted(st.session_state.ad_sel + [g])
                                st.rerun()

        with st.expander("🏢 산업군 필터"):
            all_ind = sorted(df['industry'].unique())
            if "industry_sel" not in st.session_state: st.session_state.industry_sel = [s for s in all_ind if s != 'Unknown']
            if st.button("전체 선택/해제", key="ind_all_btn"):
                st.session_state.industry_sel = [] if len(st.session_state.industry_sel) == len(all_ind) else list(all_ind)
                st.rerun()
            
            sel_ind = st.session_state.industry_sel

        # 필터링 적용
        mask = (df['price'] >= min_price) & \
               (df['adv_50'] >= min_adv_m * 1_000_000) & \
               (df['rs_score'] >= rs_min) & \
               (df['industry_rs_score'] >= ind_rs_min) & \
               (df['smr_grade'].isin(st.session_state.smr_sel)) & \
               (df['ad_grade'].isin(st.session_state.ad_sel)) & \
               (df['industry'].isin(sel_ind))
        
        if show_only_favs:
            mask = mask & (df['symbol'].isin(get_favorites()))
        
        f_df = df[mask].sort_values('rs_score', ascending=False)

    # --- [5] 화면 분할 및 데이터 표시 ---
    col_l, col_r = st.columns([4, 3])
    with col_l:
        st.subheader(f"Leaders ({len(f_df)})")
        display_list = f_df.copy()
        display_list['ADV($M)'] = (display_list['adv_50'] / 1_000_000).round(1)
        
        # 리스트에 즐겨찾기 별표 표시
        fav_list = get_favorites()
        display_list['Ticker'] = display_list['symbol'].apply(lambda x: f"⭐ {x}" if x in fav_list else x)
        
        sel = st.dataframe(
            display_list[['Ticker', 'price', 'ADV($M)', 'rs_score', 'smr_grade', 'ad_grade', 'industry_rs_score', 'industry']],
            hide_index=True, on_select="rerun", selection_mode="single-row", height=850, use_container_width=True
        )

    with col_r:
        if len(sel.selection.rows) > 0:
            row = f_df.iloc[sel.selection.rows[0]]
            ticker = row['symbol']

            # 즐겨찾기 버튼 (메인 상단)
            fav_list = get_favorites()
            is_fav = ticker in fav_list
            c1, c2 = st.columns([4, 1])
            with c1: st.markdown(f"### {ticker} 상세 정보")
            with c2: 
                if st.button("⭐ 삭제" if is_fav else "☆ 추가", use_container_width=True):
                    toggle_favorite(ticker)
                    st.rerun()

            st.markdown(f"**RS** {row['rs_score']} · **SMR** {row['smr_grade']} · **AD** {row['ad_grade']} · {row['industry']}")

            try:
                with st.spinner("불러오는 중..."):
                    q_inc, a_inc, q_bal, a_bal, info = get_detailed_info(ticker)
                
                t_chart, t_fin, t_check, t_biz = st.tabs(["📊 차트", "🧾 재무제표", "🛡️ 체크리스트", "🏢 개요"])
                
                with t_chart:
                    # 기존 차트 로직 유지
                    components.html(f"""
                    <script type="text/javascript" src="https://s3.tradingview.com/tv.js"></script>
                    <div id="tv_chart" style="height: 500px;"></div>
                    <script type="text/javascript">
                    new TradingView.widget({{"autosize":true,"symbol":"{ticker}","interval":"D","theme":"dark","style":"1","locale":"kr","container_id":"tv_chart"}});
                    </script>
                    """, height=510)

                with t_fin:
                    st.write("재무 데이터 요약")
                    st.dataframe(q_inc.iloc[:5], use_container_width=True) # 예시 표시

                with t_check:
                    st.checkbox("RS 점수 80 이상", value=row['rs_score'] >= 80)
                    st.checkbox("SMR 등급 A/B", value=row['smr_grade'] in ['A', 'B'])

                with t_biz:
                    summary_ko = translate_to_korean(info.get('longBusinessSummary', 'N/A'))
                    st.markdown(f'<div class="overview-panel"><h2>{info.get("longName", ticker)}</h2><p>{summary_ko}</p></div>', unsafe_allow_html=True)

            except:
                st.error("데이터를 가져오는 중 오류가 발생했습니다.")
        else:
            st.info("👈 왼쪽 리스트에서 종목을 선택해 주세요.")
