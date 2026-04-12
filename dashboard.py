# -*- coding: utf-8 -*-
import streamlit as st
import pandas as pd
import sqlite3
import requests
import streamlit.components.v1 as components
import os
import altair as alt
from datetime import datetime

# --- [설정] FMP API 키 ---
FMP_API_KEY = "1kJBflGjsp5fCgbancejhI5bN5iavEJF"

# --- 즐겨찾기 데이터베이스 함수 ---
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
    try:
        df_fav = pd.read_sql("SELECT symbol FROM favorites", conn)
        return df_fav['symbol'].tolist()
    except: return []
    finally: conn.close()

init_fav_db()

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

# --- 상세 정보 가져오기 (FMP API 버전) ---
@st.cache_data(ttl=3600)
def get_detailed_info(ticker):
    """
    야후 파이낸스 대신 FMP API를 사용하여 상세 정보 및 재무제표를 가져옵니다.
    로컬 IP 차단 문제를 완벽하게 우회합니다.
    """
    try:
        # 1. 기업 프로필 (이름, 개요 등)
        profile = requests.get(f"https://financialmodelingprep.com/api/v3/profile/{ticker}?apikey={FMP_API_KEY}").json()
        # 2. 분기 손익계산서 (Financials)
        income = requests.get(f"https://financialmodelingprep.com/api/v3/income-statement/{ticker}?period=quarter&limit=12&apikey={FMP_API_KEY}").json()
        # 3. 분기 대차대조표 (ROE 계산용)
        balance = requests.get(f"https://financialmodelingprep.com/api/v3/balance-sheet-statement/{ticker}?period=quarter&limit=8&apikey={FMP_API_KEY}").json()

        info = profile[0] if profile else {}
        # 기존 UI와 호환되도록 키값 매핑
        info['longBusinessSummary'] = info.get('description', 'N/A')
        info['longName'] = info.get('companyName', ticker)
        info['returnOnEquity'] = info.get('roe', 0) / 100
        info['heldPercentInstitutions'] = 0.5 # 무료 버전 기본값

        # 데이터프레임 변환
        q_inc = pd.DataFrame(income).set_index('date').T if income else pd.DataFrame()
        q_bal = pd.DataFrame(balance).set_index('date').T if balance else pd.DataFrame()
        
        # 컬럼명 매핑 (yfinance 스타일로 변환)
        mapping = {
            'revenue': 'Total Revenue', 
            'operatingIncome': 'Operating Income', 
            'netIncome': 'Net Income', 
            'eps': 'Basic EPS',
            'totalAssets': 'Total Assets',
            'totalLiabilities': 'Total Liabilities Net Minority Interest',
            'totalStockholdersEquity': 'Stockholders Equity'
        }
        if not q_inc.empty: q_inc = q_inc.rename(index=mapping)
        if not q_bal.empty: q_bal = q_bal.rename(index=mapping)

        return q_inc, pd.DataFrame(), q_bal, pd.DataFrame(), info
    except Exception as e:
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), {"longBusinessSummary": f"API 로드 오류: {e}"}

# --- 페이지 설정 ---
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
.overview-panel {{ background: {OVERVIEW_BG}; color: {OVERVIEW_TEXT}; padding: 1.5rem 1.75rem; border-radius: 12px; border: 1px solid #4a5161; line-height: 1.7; font-size: 0.95rem; }}
.overview-panel h2 {{ color: #64ffda !important; margin-bottom: 1rem; }}
.overview-panel p {{ color: {OVERVIEW_TEXT} !important; }}
</style>
""", unsafe_allow_html=True)

FIN_MAP = {
    'Total Revenue': '매출액', 'Operating Income': '영업이익', 'Net Income': '당기순이익',
    'Basic EPS': 'EPS', 'Total Assets': '총 자산',
    'Total Liabilities Net Minority Interest': '총 부채', 'Stockholders Equity': '총 자본'
}

def get_data():
    if not os.path.exists('ibd_system.db'): return pd.DataFrame()
    conn = sqlite3.connect('ibd_system.db')
    df = pd.read_sql("SELECT * FROM repo_results", conn)
    conn.close()
    return df

def get_rs_history(ticker):
    if not os.path.exists('ibd_system.db'): return pd.DataFrame()
    conn = sqlite3.connect('ibd_system.db')
    try:
        query = f"SELECT * FROM rs_history WHERE symbol = '{ticker}' ORDER BY date ASC"
        hist_df = pd.read_sql(query, conn)
    except: hist_df = pd.DataFrame()
    finally: conn.close()
    return hist_df

def calc_growth(series, periods):
    if series is None or series.empty: return pd.Series(dtype=float)
    s = series.iloc[::-1] # FMP는 최신이 앞이므로 뒤집어서 계산
    growth = ((s - s.shift(periods)) / s.shift(periods).abs()) * 100
    return growth.iloc[::-1]

# --- 메인 화면 ---
df = get_data()
if not df.empty:
    with st.sidebar:
        st.header("🎛️ Terminal Control")
        show_only_favs = st.checkbox("⭐ 즐겨찾기만 보기", value=False)
        min_price = st.number_input("최소 주가 ($)", min_value=0.0, value=10.0)
        min_adv_m = st.number_input("최소 거래대금 ($M)", min_value=0.0, value=2.0)
        rs_min = st.slider("최소 RS 점수", 1, 99, 80)
        
        # 필터링 로직
        mask = (df['price'] >= min_price) & (df['adv_50'] >= min_adv_m * 1_000_000) & (df['rs_score'] >= rs_min)
        if show_only_favs: mask = mask & (df['symbol'].isin(get_favorites()))
        f_df = df[mask].sort_values('rs_score', ascending=False)

    col_l, col_r = st.columns([4, 3])
    with col_l:
        st.subheader(f"Leaders ({len(f_df)})")
        display_list = f_df.copy()
        display_list['ADV($M)'] = (display_list['adv_50'] / 1_000_000).round(1)
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
            
            c_header, c_fav = st.columns([4, 1])
            with c_header: st.markdown(f"### {ticker}")
            with c_fav:
                if st.button("⭐ 저장/삭제", use_container_width=True):
                    toggle_favorite(ticker)
                    st.rerun()

            st.markdown(f"**RS** {row['rs_score']} · **SMR** {row['smr_grade']} · **AD** {row['ad_grade']} · {row['industry']}")

            with st.spinner(f"FMP API에서 '{ticker}' 데이터 로드 중..."):
                q_inc, a_inc, q_bal, a_bal, info = get_detailed_info(ticker)
            
            t_chart, t_fin, t_biz = st.tabs(["📊 차트", "🧾 재무제표", "🏢 개요"])

            with t_chart:
                components.html(f"""
                <script type="text/javascript" src="https://s3.tradingview.com/tv.js"></script>
                <div id="tv_chart" style="height: 600px;"></div>
                <script type="text/javascript">
                new TradingView.widget({{"autosize":true,"symbol":"{ticker}","interval":"D","theme":"dark","style":"1","locale":"kr","container_id":"tv_chart"}});
                </script>
                """, height=600)

                rs_hist_df = get_rs_history(ticker)
                if not rs_hist_df.empty:
                    chart = alt.Chart(rs_hist_df).mark_line().encode(x='date:T', y='rs_score:Q', color=alt.value("#64ffda")).properties(height=300)
                    st.altair_chart(chart, use_container_width=True)

            with t_fin:
                if not q_inc.empty:
                    st.markdown("#### 📈 분기 성장률 (QoQ %)")
                    q_rev = q_inc.loc['Total Revenue'] if 'Total Revenue' in q_inc.index else pd.Series()
                    qoq_df = pd.DataFrame({'분기': q_inc.columns, '매출 성장(%)': calc_growth(q_rev, 1)}).set_index('분기').head(4)
                    st.dataframe(qoq_df.style.format("{:.1f}"), use_container_width=True)

                    st.markdown("#### 🧾 상세 재무 ($1,000)")
                    disp = pd.concat([q_inc, q_bal]).reindex(list(FIN_MAP.keys())).dropna()
                    disp.index = [FIN_MAP[i] for i in disp.index]
                    st.dataframe(disp.style.format(precision=0, thousands=","), use_container_width=True)

            with t_biz:
                st.subheader("🏢 기업 개요")
                summary_ko = translate_to_korean(info.get('longBusinessSummary', 'N/A'))
                st.markdown(f'<div class="overview-panel"><h2>{info.get("longName", ticker)}</h2><p>{summary_ko}</p></div>', unsafe_allow_html=True)
        else:
            st.info("👈 리스트에서 종목을 선택해 주세요.")
