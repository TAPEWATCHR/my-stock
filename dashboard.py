# -*- coding: utf-8 -*-
import streamlit as st
import pandas as pd
import sqlite3
import yfinance as yf
import streamlit.components.v1 as components
import os
import altair as alt
import time

# --- [1] 즐겨찾기 DB 유틸리티 (추가됨) ---
def init_fav_db():
    conn = sqlite3.connect('ibd_system.db')
    cursor = conn.cursor()
    cursor.execute("CREATE TABLE IF NOT EXISTS favorites (symbol TEXT PRIMARY KEY)")
    conn.commit()
    conn.close()

def toggle_favorite(ticker):
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
    if not os.path.exists('ibd_system.db'): return []
    conn = sqlite3.connect('ibd_system.db')
    df_fav = pd.read_sql("SELECT symbol FROM favorites", conn)
    conn.close()
    return df_fav['symbol'].tolist()

init_fav_db()

# --- [2] 기존 유틸리티 및 에러 핸들링 ---
class YFDataFetchError(Exception):
    pass

@st.cache_data(ttl=86400)
def translate_to_korean(text):
    if not text or text == "N/A" or len(text.strip()) < 10: return text
    try:
        from deep_translator import GoogleTranslator
        chunk_size = 4500
        out = []
        for i in range(0, len(text), chunk_size):
            chunk = text[i:i + chunk_size]
            out.append(GoogleTranslator(source="auto", target="ko").translate(chunk))
        return " ".join(out)
    except: return text

# --- [3] 페이지 설정 및 스타일 ---
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
.overview-panel {{ background: {OVERVIEW_BG}; color: {OVERVIEW_TEXT}; padding: 1.5rem 1.75rem; border-radius: 12px; border: 1px solid #4a5161; line-height: 1.7; font-size: 0.95rem; }}
.overview-panel h2 {{ color: #64ffda !important; margin-bottom: 1rem; }}
[data-testid="stSidebar"] .stButton > button {{ width: auto !important; min-width: 0 !important; padding: 0.1rem 0.5rem !important; font-size: 0.75rem !important; line-height: 1.2 !important; min-height: 24px !important; white-space: nowrap; }}
[data-testid="stSidebar"] button[kind="primary"] {{ color: #ff4b4b !important; border-color: #ff4b4b !important; background-color: transparent !important; }}
</style>
""", unsafe_allow_html=True)

# --- [4] 데이터 처리 함수들 (복구됨) ---
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
    if not os.path.exists('ibd_system.db'): return pd.DataFrame()
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

# --- [5] 사이드바 컨트롤 (복구됨) ---
df = get_data()
if not df.empty:
    with st.sidebar:
        st.header("🎛️ Terminal Control")
        
        with st.expander("🔍 필터 설정", expanded=True):
            show_only_favs = st.checkbox("⭐ 즐겨찾기만 보기", value=False)
            min_price = st.number_input("최소 주가 ($)", min_value=0.0, value=10.0, step=1.0)
            min_adv_m = st.number_input("최소 거래대금 ($M)", min_value=0.0, value=2.0, step=0.5)
            rs_min = st.slider("최소 RS 점수", 1, 99, 80)
            ind_rs_min = st.slider("최소 산업군 RS", 1, 99, 50)

            # SMR 등급 멀티 선택
            if "smr_sel" not in st.session_state: st.session_state.smr_sel = ["A", "B"]
            st.caption("SMR 등급")
            for gs in [["A", "B", "C"], ["D", "E", "전체"]]:
                cols = st.columns(3)
                for i, g in enumerate(gs):
                    with cols[i]:
                        is_all = (g == "전체" and set(st.session_state.smr_sel) == {"A","B","C","D","E"})
                        sel = g in st.session_state.smr_sel or is_all
                        if st.button(f"{'●' if sel else '○'} {g}", key=f"smr_{g}", type="primary" if sel else "secondary"):
                            if g == "전체": st.session_state.smr_sel = ["A","B","C","D","E"] if not is_all else []
                            else:
                                if g in st.session_state.smr_sel: st.session_state.smr_sel.remove(g)
                                else: st.session_state.smr_sel = sorted(st.session_state.smr_sel + [g])
                            st.rerun()

            st.divider()

            # AD 등급 멀티 선택
            if "ad_sel" not in st.session_state: st.session_state.ad_sel = ["A", "B", "C"]
            st.caption("수급(AD) 등급")
            for gs in [["A", "B", "C"], ["D", "E", "전체"]]:
                cols = st.columns(3)
                for i, g in enumerate(gs):
                    with cols[i]:
                        is_all = (g == "전체" and set(st.session_state.ad_sel) == {"A","B","C","D","E"})
                        sel = g in st.session_state.ad_sel or is_all
                        if st.button(f"{'●' if sel else '○'} {g}", key=f"ad_{g}", type="primary" if sel else "secondary"):
                            if g == "전체": st.session_state.ad_sel = ["A","B","C","D","E"] if not is_all else []
                            else:
                                if g in st.session_state.ad_sel: st.session_state.ad_sel.remove(g)
                                else: st.session_state.ad_sel = sorted(st.session_state.ad_sel + [g])
                            st.rerun()

        with st.expander("🏢 산업군 필터", expanded=False):
            all_ind = sorted(df['industry'].unique())
            if "industry_sel" not in st.session_state: st.session_state.industry_sel = [s for s in all_ind if s != 'Unknown']
            
            if st.button("● 전체 선택/해제", key="ind_all"):
                st.session_state.industry_sel = [] if len(st.session_state.industry_sel) == len(all_ind) else list(all_ind)
                st.rerun()

            for s in all_ind:
                sel = s in st.session_state.industry_sel
                if st.button(f"{'●' if sel else '○'} {s}", key=f"ind_{s}", type="primary" if sel else "secondary", use_container_width=True):
                    if sel: st.session_state.industry_sel.remove(s)
                    else: st.session_state.industry_sel.append(s)
                    st.rerun()

        # 데이터 필터링 마스크
        mask = (df['price'] >= min_price) & \
               (df['adv_50'] >= min_adv_m * 1_000_000) & \
               (df['rs_score'] >= rs_min) & \
               (df['industry_rs_score'] >= ind_rs_min) & \
               (df['smr_grade'].isin(st.session_state.smr_sel)) & \
               (df['ad_grade'].isin(st.session_state.ad_sel)) & \
               (df['industry'].isin(st.session_state.industry_sel))
        
        if show_only_favs: mask = mask & (df['symbol'].isin(get_favorites()))
        f_df = df[mask].sort_values('rs_score', ascending=False)

    # --- [6] 메인 화면 레이아웃 ---
    col_l, col_r = st.columns([4, 3])
    with col_l:
        st.subheader(f"Leaders ({len(f_df)})")
        display_list = f_df.copy()
        display_list['ADV($M)'] = (display_list['adv_50'] / 1_000_000).round(1)
        
        favs = get_favorites()
        display_list['Ticker'] = display_list['symbol'].apply(lambda x: f"⭐ {x}" if x in favs else x)

        sel = st.dataframe(
            display_list[['Ticker', 'price', 'ADV($M)', 'rs_score', 'smr_grade', 'ad_grade', 'industry_rs_score', 'industry']],
            hide_index=True, on_select="rerun", selection_mode="single-row", height=850, use_container_width=True
        )

    with col_r:
        if len(sel.selection.rows) > 0:
            row = f_df.iloc[sel.selection.rows[0]]
            ticker = row['symbol']
            
            # 즐겨찾기 버튼 상단 배치
            favs = get_favorites()
            is_fav = ticker in favs
            c_head, c_fav = st.columns([4, 1])
            with c_head: st.markdown(f"### {ticker}")
            with c_fav:
                if st.button("⭐ 삭제" if is_fav else "☆ 저장", use_container_width=True):
                    toggle_favorite(ticker)
                    st.rerun()

            st.write(f"**RS** {row['rs_score']} · **SMR** {row['smr_grade']} · **AD** {row['ad_grade']} · {row['industry']}")

            try:
                with st.spinner("재무 분석 중..."):
                    q_inc, a_inc, q_bal, a_bal, info = get_detailed_info(ticker)
                
                t_chart, t_fin, t_check, t_biz = st.tabs(["📊 차트", "🧾 재무제표", "🛡️ 체크리스트", "🏢 개요"])

                with t_chart:
                    # TradingView 임베드 로직 복구
                    if "tv_embed_url" not in st.session_state: st.session_state.tv_embed_url = ""
                    with st.expander("📌 차트 설정", expanded=False):
                        tv_url = st.text_input("TradingView URL", value=st.session_state.tv_embed_url)
                        if tv_url != st.session_state.tv_embed_url:
                            st.session_state.tv_embed_url = tv_url
                            st.rerun()
                    
                    if st.session_state.tv_embed_url:
                        components.html(f'<iframe src="{st.session_state.tv_embed_url.replace("SYMBOL", ticker)}" height="500" width="100%"></iframe>', height=510)
                    else:
                        components.html(f"""
                        <script type="text/javascript" src="https://s3.tradingview.com/tv.js"></script>
                        <div id="tv_chart" style="height: 500px;"></div>
                        <script type="text/javascript">
                        new TradingView.widget({{"autosize":true,"symbol":"{ticker}","interval":"D","theme":"dark","container_id":"tv_chart"}});
                        </script>
                        """, height=510)
                    
                    # RS 히스토리 차트 복구
                    st.markdown("#### 📈 RS 추세 (Stock & Industry)")
                    rs_h = get_rs_history(ticker)
                    if not rs_h.empty:
                        rs_h['date'] = pd.to_datetime(rs_h['date'])
                        plot_data = rs_h.melt(id_vars='date', value_vars=['rs_score', 'industry_rs_score'])
                        c = alt.Chart(plot_data).mark_line().encode(
                            x='date:T', y=alt.Y('value:Q', scale=alt.Scale(domain=[0, 100])), color='variable:N'
                        ).properties(height=300)
                        st.altair_chart(c, use_container_width=True)

                with t_fin:
                    # 성장률 표 복구
                    q_rev = q_inc.loc['Total Revenue'] if 'Total Revenue' in q_inc.index else pd.Series()
                    st.write("**분기 매출 성장률 (QoQ %)**")
                    st.dataframe(pd.DataFrame({'성장률': calc_growth(q_rev, 1)}).head(4).T)
                    
                    # 상세 표 (format_fin_df 로직 함축)
                    st.write("**연간 상세 재무**")
                    target_a = pd.concat([a_inc, a_bal]).reindex(list(FIN_MAP.keys())).dropna()
                    target_a.index = [FIN_MAP.get(i, i) for i in target_a.index]
                    st.dataframe(target_a, use_container_width=True)

                with t_check:
                    # CANSLIM / 미너비니 체크리스트 복구
                    st.subheader("🛡️ 주도주 판별")
                    c1, c2 = st.columns(2)
                    with c1:
                        st.markdown("### CANSLIM")
                        st.checkbox("C: 분기 이익 25%↑", value=True)
                        st.checkbox("L: 주도주 (RS 80↑)", value=row['rs_score']>=80)
                    with c2:
                        st.markdown("### 미너비니")
                        st.checkbox("1. 주가 > 150/200MA", value=True)
                        st.checkbox("7. RS 80 이상", value=row['rs_score']>=80)

                with t_biz:
                    summary_ko = translate_to_korean(info.get('longBusinessSummary', 'N/A'))
                    st.markdown(f'<div class="overview-panel"><h2>{info.get("longName", ticker)}</h2><p>{summary_ko}</p></div>', unsafe_allow_html=True)

            except Exception as e:
                st.error(f"데이터 로드 에러: {e}")
        else:
            st.info("👈 왼쪽 리스트에서 종목을 선택해 주세요.")
