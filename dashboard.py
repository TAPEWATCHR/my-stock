# -*- coding: utf-8 -*-
import streamlit as st
import pandas as pd
import sqlite3
import requests
import streamlit.components.v1 as components
import os
import altair as alt
from deep_translator import GoogleTranslator

# 환경 변수에서 FMP API 키 로드
FMP_API_KEY = os.environ.get("FMP_API_KEY", "").strip()

def init_db():
    conn = sqlite3.connect('ibd_system.db')
    conn.execute("CREATE TABLE IF NOT EXISTS favorites (symbol TEXT PRIMARY KEY)")
    conn.close()

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
        hist = pd.read_sql("SELECT * FROM rs_history WHERE symbol = ? ORDER BY date ASC", conn, params=(ticker,))
    except: hist = pd.DataFrame()
    conn.close()
    return hist

def toggle_favorite(symbol):
    conn = sqlite3.connect('ibd_system.db')
    c = conn.cursor()
    c.execute("SELECT symbol FROM favorites WHERE symbol=?", (symbol,))
    if c.fetchone(): c.execute("DELETE FROM favorites WHERE symbol=?", (symbol,))
    else: c.execute("INSERT INTO favorites (symbol) VALUES (?)", (symbol,))
    conn.commit()
    conn.close()

def get_favorites():
    conn = sqlite3.connect('ibd_system.db')
    try:
        favs = pd.read_sql("SELECT symbol FROM favorites", conn)['symbol'].tolist()
    except:
        favs = []
    conn.close()
    return favs

@st.cache_data(ttl=3600)
def get_fin_data(ticker):
    if not FMP_API_KEY: return [], [], [], {}
    try:
        url_is_ann = f"https://financialmodelingprep.com/stable/income-statement?symbol={ticker}&period=annual&limit=5&apikey={FMP_API_KEY}"
        is_ann = requests.get(url_is_ann).json()
        
        url_bs_ann = f"https://financialmodelingprep.com/stable/balance-sheet-statement?symbol={ticker}&period=annual&limit=5&apikey={FMP_API_KEY}"
        bs_ann = requests.get(url_bs_ann).json()
        
        url_is_qtr = f"https://financialmodelingprep.com/stable/income-statement?symbol={ticker}&period=quarter&limit=12&apikey={FMP_API_KEY}"
        is_qtr = requests.get(url_is_qtr).json()
        
        url_prof = f"https://financialmodelingprep.com/stable/profile?symbol={ticker}&apikey={FMP_API_KEY}"
        p_res = requests.get(url_prof).json()
        info = p_res[0] if p_res and isinstance(p_res, list) else {}
        
        return is_ann, bs_ann, is_qtr, info
    except: return [], [], [], {}

def format_currency(val):
    try:
        val = float(val)
        if pd.isna(val) or val == 0: return "0"
        return f"{int(val / 1000):,}"
    except: return "0"

def calc_growth(current, previous):
    try:
        current, previous = float(current), float(previous)
        if pd.isna(current) or pd.isna(previous) or previous == 0: return None
        return ((current - previous) / abs(previous)) * 100
    except: return None

def format_growth(val):
    if pd.isna(val) or val is None: return "-"
    return f"{val:.1f}%"

def format_adv(val):
    try:
        val = float(val)
        if val >= 1e9: return f"${val/1e9:.2f}B"
        elif val >= 1e6: return f"${val/1e6:.2f}M"
        return f"${val:,.0f}"
    except: return "$0"

# ================= UI 디자인 =================
st.set_page_config(layout="wide", page_title="Market Leaders Terminal")
st.markdown("""
<style>
    .stApp { background-color: #161C27 !important; }
    
    /* 메인 화면 기본 글씨는 하얗게 */
    .block-container p, .block-container span, .block-container h1, .block-container h2, 
    .block-container h3, .block-container h4, .block-container label { color: #FFFFFF !important; }
    
    /* 사이드바 글씨는 어둡게 */
    [data-testid="stSidebar"] { background-color: #F8F9FA !important; }
    [data-testid="stSidebar"] p, [data-testid="stSidebar"] span, [data-testid="stSidebar"] label { color: #1E293B !important; font-size: 13px; }
    
    /* 버튼 배경 및 테두리 */
    .stButton > button { 
        background-color: #FFFFFF !important; 
        border: 1px solid #CBD5E1 !important; 
    }
    
    /* 버튼 안의 글씨를 어두운 남색으로 강제 지정 */
    .stButton > button p, .stButton > button span, .stButton > button div {
        color: #1E293B !important; 
        font-weight: bold !important;
    }

    .overview-panel { background: #2A3143; padding: 1.2rem; border-radius: 8px; color: #FFFFFF !important; line-height: 1.6;}
    .check-box { padding: 10px; margin-bottom: 5px; border-radius: 5px; background-color: #1E293B; border-left: 5px solid #3b82f6; color: #D1D5DB !important; }
    .check-pass { border-left-color: #10b981; }
    .check-fail { border-left-color: #ef4444; }
</style>
""", unsafe_allow_html=True)

if not FMP_API_KEY:
    st.error("🚨 FMP_API_KEY가 설정되지 않아 대시보드를 불러올 수 없습니다.")

init_db()
df = get_data()
fav_list = get_favorites()

if not df.empty:
    if 'adv_50' not in df.columns: df['adv_50'] = 0.0
    if 'industry_rs_score' not in df.columns: df['industry_rs_score'] = 0
    if 'ad_grade' not in df.columns: df['ad_grade'] = 'C'
    if 'smr_grade' not in df.columns: df['smr_grade'] = 'C'
    if 'industry' not in df.columns: df['industry'] = 'Unknown'

    with st.sidebar:
        st.header("🎛️ Terminal Control")
        min_p = st.number_input("최소 주가 ($)", value=10.0)
        min_adv_m = st.number_input("최소 거래대금 ($Million)", value=10.0)
        rs_m = st.slider("최소 RS 점수", 1, 99, 80)
        ind_rs_m = st.slider("최소 산업군 RS 점수", 1, 99, 70)
        
        with st.expander("🏭 산업군 필터"):
            all_inds = sorted(df['industry'].unique().tolist())
            if 'ind_sel' not in st.session_state: st.session_state.ind_sel = all_inds
            
            c_all = st.columns(1)
            is_all = len(st.session_state.ind_sel) == len(all_inds)
            if st.button(f"{'●' if is_all else '○'} 전체 선택/해제", key="all_ind_btn"):
                st.session_state.ind_sel = [] if is_all else all_inds
                st.rerun()
            
            cols = st.columns(2)
            for idx, ind in enumerate(all_inds):
                with cols[idx % 2]:
                    is_sel = ind in st.session_state.ind_sel
                    if st.button(f"{'●' if is_sel else '○'} {str(ind)[:12]}..", key=f"ind_{ind}", use_container_width=True):
                        if is_sel: st.session_state.ind_sel.remove(ind)
                        else: st.session_state.ind_sel.append(ind)
                        st.rerun()

        def btn_filter(label, key):
            if key not in st.session_state: st.session_state[key] = ["A", "B"]
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
        show_fav_only = st.checkbox("⭐ 관심종목만 보기", value=False)

    mask = (df['price'] >= min_p) & (df['rs_score'] >= rs_m) & \
           (df['adv_50'] >= min_adv_m * 1000000) & (df['industry_rs_score'] >= ind_rs_m) & \
           (df['smr_grade'].isin(smr_sel)) & (df['ad_grade'].isin(ad_sel)) & \
           (df['industry'].isin(st.session_state.ind_sel))
    
    f_df = df[mask].sort_values('rs_score', ascending=False).copy()
    if show_fav_only: f_df = f_df[f_df['symbol'].isin(fav_list)]

    display_df = f_df[['symbol', 'price', 'rs_score', 'industry_rs_score', 'smr_grade', 'ad_grade', 'adv_50', 'industry']].copy()
    display_df['adv_50'] = display_df['adv_50'].apply(format_adv)

    # 💡 [핵심 수정] 표에 표시될 컬럼명을 한글로 예쁘게 변경
    display_df.rename(columns={
        'symbol': '종목', 
        'price': '가격', 
        'adv_50': '50일 평균 거래대금', 
        'rs_score': 'RS점수', 
        'industry_rs_score': '산업군RS점수', 
        'smr_grade': 'SMR등급', 
        'ad_grade': 'AD등급', 
        'industry': '산업군명'
    }, inplace=True)

    col_l, col_r = st.columns([4, 5])
    with col_l:
        st.subheader(f"Leaders List ({len(display_df)})")
        sel_row = st.dataframe(display_df, hide_index=True, on_select="rerun", selection_mode="single-row", height=800, use_container_width=True)

    with col_r:
        if len(sel_row.selection.rows) > 0:
            target = f_df.iloc[sel_row.selection.rows[0]]
            if isinstance(target, pd.DataFrame): target = target.iloc[0] 
            ticker = target.get('symbol', 'UNKNOWN')
            
            c1, c2 = st.columns([4, 1])
            with c1: st.markdown(f"## {ticker} <span style='font-size:18px; color:#9CA3AF;'>{target.get('industry', 'Unknown')}</span>", unsafe_allow_html=True)
            with c2:
                is_fav = ticker in fav_list
                if st.button("★ 관심해제" if is_fav else "☆ 관심저장", use_container_width=True):
                    toggle_favorite(ticker)
                    st.rerun()
            
            is_ann_raw, bs_ann_raw, is_qtr_raw, info = get_fin_data(ticker)
            t_chart, t_check, t_fin, t_biz = st.tabs(["📊 차트", "🛡️ 체크리스트", "🧾 재무제표", "🏢 기업 개요"])
            
            with t_chart:
                tv_widget = f"""
                <div class="tradingview-widget-container" style="height: 500px; width: 100%;">
                  <div id="tradingview_{ticker}" style="height: calc(100% - 32px); width: 100%;"></div>
                  <script type="text/javascript" src="https://s3.tradingview.com/tv.js"></script>
                  <script type="text/javascript">
                  new TradingView.widget({{"autosize": true, "symbol": "{ticker}", "interval": "D", "timezone": "Etc/UTC", "theme": "dark", "style": "1", "locale": "kr", "enable_publishing": false, "backgroundColor": "#161C27", "gridColor": "#2A3143", "hide_top_toolbar": false, "save_image": false, "container_id": "tradingview_{ticker}"}});
                  </script>
                </div>
                """
                components.html(tv_widget, height=500)

                rs_hist_df = get_rs_history(ticker)
                if not rs_hist_df.empty and len(rs_hist_df) > 1:
                    rs_hist_df['date'] = pd.to_datetime(rs_hist_df['date'])
                    
                    if 'industry_rs_score' in rs_hist_df.columns:
                        rs_hist_df['industry_rs_score'] = rs_hist_df['industry_rs_score'].replace(0, pd.NA)
                        melted_df = rs_hist_df.melt('date', value_vars=['rs_score', 'industry_rs_score'], var_name='Type', value_name='Score')
                        melted_df = melted_df.dropna(subset=['Score'])
                        melted_df['Type'] = melted_df['Type'].map({'rs_score': '개별 RS 점수', 'industry_rs_score': '산업군 RS 점수'})
                        
                        rs_chart = alt.Chart(melted_df).mark_line(strokeWidth=2).encode(
                            x=alt.X('date:T', title='날짜'), 
                            y=alt.Y('Score:Q', title='점수', scale=alt.Scale(domain=[1, 100])),
                            color=alt.Color('Type:N', title='지표', scale=alt.Scale(domain=['개별 RS 점수', '산업군 RS 점수'], range=['#64ffda', '#f59e0b']))
                        ).properties(height=240)
                    else:
                        rs_chart = alt.Chart(rs_hist_df).mark_line(color="#64ffda", strokeWidth=2).encode(
                            x=alt.X('date:T', title='날짜'), y=alt.Y('rs_score:Q', title='RS 점수', scale=alt.Scale(domain=[1, 100]))
                        ).properties(height=240)
                        
                    st.altair_chart(rs_chart, use_container_width=True)

            with t_check:
                price_val = float(target.get('price', 0))
                rs_val = int(target.get('rs_score', 0))
                smr_val = str(target.get('smr_grade', 'C'))
                ad_val = str(target.get('ad_grade', 'C'))
                adv_val = float(target.get('adv_50', 0))
                ind_rs_val = int(target.get('industry_rs_score', 0))

                st.markdown("#### 캔슬림 (CAN SLIM) 전략")
                canslim = [
                    {"name": "C (현재 실적): SMR 등급 A 또는 B", "pass": smr_val in ['A', 'B']},
                    {"name": "A (연간 실적): SMR 등급 A 또는 B", "pass": smr_val in ['A', 'B']},
                    {"name": "N (신제품/신고가): RS 점수 80 이상", "pass": rs_val >= 80},
                    {"name": "S (수요와 공급): 거래대금 $20M 이상", "pass": adv_val >= 20000000},
                    {"name": "L (주도주): 산업군 RS 점수 70 이상", "pass": ind_rs_val >= 70},
                    {"name": "I (기관 수급): AD 수급 등급 A 또는 B", "pass": ad_val in ['A', 'B']},
                ]
                for c in canslim:
                    st.markdown(f'<div class="check-box {"check-pass" if c["pass"] else "check-fail"}">{"✅" if c["pass"] else "❌"} {c["name"]}</div>', unsafe_allow_html=True)

                st.markdown("#### 마크 미너비니 (Minervini VCP) 전략")
                minervini = [
                    {"name": "최소 주가: $15 이상 (기관 진입 가능)", "pass": price_val >= 15},
                    {"name": "주도주 모멘텀: RS 점수 70 이상", "pass": rs_val >= 70},
                    {"name": "펀더멘탈: SMR 등급 A 또는 B", "pass": smr_val in ['A', 'B']},
                    {"name": "매집 흔적: AD 수급 등급 A, B, C", "pass": ad_val in ['A', 'B', 'C']},
                    {"name": "유동성: 거래대금 $10M 이상", "pass": adv_val >= 10000000}
                ]
                for c in minervini:
                    st.markdown(f'<div class="check-box {"check-pass" if c["pass"] else "check-fail"}">{"✅" if c["pass"] else "❌"} {c["name"]}</div>', unsafe_allow_html=True)

            with t_fin:
                st.caption("단위: 천불 ($1,000) / 성장률: %")
                
                def safe_parse(data_list, keys, required_key):
                    if not isinstance(data_list, list) or len(data_list) == 0: return pd.DataFrame()
                    if required_key not in data_list[0]: return pd.DataFrame()
                    parsed = [{k: item.get(k) if item.get(k) is not None else 0 for k in keys} for item in data_list]
                    return pd.DataFrame(parsed)

                req_is_ann = ['calendarYear', 'revenue', 'operatingIncome', 'netIncome', 'ebitda']
                req_bs_ann = ['calendarYear', 'totalAssets', 'totalLiabilities', 'totalStockholdersEquity']
                
                is_ann_df = safe_parse(is_ann_raw, req_is_ann, 'calendarYear')
                bs_ann_df = safe_parse(bs_ann_raw, req_bs_ann, 'calendarYear')

                if not is_ann_df.empty and not bs_ann_df.empty:
                    st.markdown("#### 📅 연간 재무 및 성장률 (최근 5년)")
                    ann_df = is_ann_df.merge(bs_ann_df, on='calendarYear', how='left')
                    
                    for col, growth_col in zip(['revenue', 'operatingIncome', 'netIncome'], ['매출성장률', '영업이익성장률', '순이익성장률']):
                        ann_df[growth_col] = ann_df[col].shift(-1)
                        ann_df[growth_col] = ann_df.apply(lambda row: calc_growth(row[col], row[growth_col]), axis=1)
                    
                    ko_cols = {'calendarYear':'연도', 'revenue':'매출액', 'operatingIncome':'영업이익', 'netIncome':'순이익', 'ebitda':'EBITDA', 'totalAssets':'총자산', 'totalLiabilities':'총부채', 'totalStockholdersEquity':'자본'}
                    ann_df = ann_df.rename(columns=ko_cols)
                    
                    for col in ['매출액', '영업이익', '순이익', 'EBITDA', '총자산', '총부채', '자본']:
                        ann_df[col] = ann_df[col].apply(format_currency)
                    for col in ['매출성장률', '영업이익성장률', '순이익성장률']:
                        ann_df[col] = ann_df[col].apply(format_growth)
                        
                    st.dataframe(ann_df[['연도', '매출액', '매출성장률', '영업이익', '영업이익성장률', '순이익', '순이익성장률', 'EBITDA', '총자산', '총부채', '자본']].head(5), hide_index=True, use_container_width=True)
                else:
                    st.info("해당 기업의 연간 상세 재무제표가 공시되지 않았거나, 제공되지 않습니다.")

                req_is_qtr = ['date', 'period', 'revenue', 'operatingIncome', 'netIncome', 'eps']
                qtr_df = safe_parse(is_qtr_raw, req_is_qtr, 'date')

                if not qtr_df.empty:
                    st.markdown("#### 📊 분기별 재무 및 성장률 (최근 3년)")
                    for col, growth_col in zip(['revenue', 'operatingIncome', 'netIncome'], ['매출성장률(YoY)', '영업이익성장률(YoY)', '순이익성장률(YoY)']):
                        qtr_df[growth_col] = qtr_df[col].shift(-4)
                        qtr_df[growth_col] = qtr_df.apply(lambda row: calc_growth(row[col], row[growth_col]), axis=1)
                    
                    ko_qtr = {'date':'발표일', 'period':'분기', 'revenue':'매출액', 'operatingIncome':'영업이익', 'netIncome':'순이익', 'eps':'EPS'}
                    qtr_df = qtr_df.rename(columns=ko_qtr)
                    
                    for col in ['매출액', '영업이익', '순이익']:
                        qtr_df[col] = qtr_df[col].apply(format_currency)
                    for col in ['매출성장률(YoY)', '영업이익성장률(YoY)', '순이익성장률(YoY)']:
                        qtr_df[col] = qtr_df[col].apply(format_growth)
                        
                    st.dataframe(qtr_df[['발표일', '분기', '매출액', '매출성장률(YoY)', '영업이익', '영업이익성장률(YoY)', '순이익', '순이익성장률(YoY)', 'EPS']].head(12), hide_index=True, use_container_width=True)
                else:
                    st.info("해당 기업의 분기 상세 재무제표가 공시되지 않았거나, 제공되지 않습니다.")

            with t_biz:
                desc_en = info.get("description", "")
                if desc_en:
                    st.markdown(f'<div class="overview-panel" style="margin-bottom: 20px;"><strong>[영문 원문]</strong><br><br>{desc_en}</div>', unsafe_allow_html=True)
                    try:
                        with st.spinner("AI가 회사 개요를 번역 중입니다..."):
                            desc_ko = GoogleTranslator(source='en', target='ko').translate(desc_en)
                        st.markdown(f'<div class="overview-panel"><strong>[🇰🇷 한글 번역]</strong><br><br>{desc_ko}</div>', unsafe_allow_html=True)
                    except:
                        st.error("번역 서버에 일시적으로 연결할 수 없습니다.")
                else:
                    st.info("해당 기업의 개요 정보가 제공되지 않습니다.")
                
        else: st.info("👈 왼쪽 리스트에서 종목을 선택해 주세요.")
else:
    st.warning("데이터베이스가 비어있습니다. 먼저 `update_data.py`를 실행해주세요.")
