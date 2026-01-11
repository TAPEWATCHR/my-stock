import streamlit as st
import pandas as pd
import sqlite3
import yfinance as yf
import streamlit.components.v1 as components
import os

# --- 0. 페이지 설정 및 디자인 ---
st.set_page_config(layout="wide", page_title="Institutional Stock Terminal")

BG_COLOR = "#161C27"
TABLE_BG_COLOR = "#363C4C"

st.markdown(f"""
    <style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;600;800&family=JetBrains+Mono:wght@400;500&display=swap');
    .stApp {{ background-color: {BG_COLOR} !important; font-family: 'Inter', sans-serif; }}
    h1, h2, h3, h4, h5, h6, p, label, span, .stCheckbox {{ color: #ccd6f6 !important; }}
    [data-testid="stDataFrame"] {{ background-color: {TABLE_BG_COLOR} !important; }}
    .metric-card {{ background-color: {TABLE_BG_COLOR}; border-radius: 12px; padding: 22px; border: 1px solid #4a5161; text-align: center; }}
    .metric-label {{ color: #aeb9cc !important; font-size: 0.75rem; font-weight: 600; text-transform: uppercase; }}
    .metric-value {{ font-size: 1.8rem; font-weight: 800; color: #64ffda !important; }}
    </style>
    """, unsafe_allow_html=True)

# --- 1. 데이터 및 번역 맵 ---
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
    return df

def calc_growth(series, periods):
    if series is None or series.empty: return pd.Series([0.0]*len(series))
    s = series.sort_index(ascending=True)
    growth = ((s - s.shift(periods)) / s.shift(periods).abs()) * 100
    return growth.sort_index(ascending=False)

@st.cache_data(ttl=3600)
def get_detailed_info(ticker):
    s = yf.Ticker(ticker)
    return s.quarterly_income_stmt, s.income_stmt, s.quarterly_balance_sheet, s.balance_sheet, s.info

# --- 2. 메인 화면 ---
df = get_data()
if not df.empty:
    with st.sidebar:
        st.header("🎛️ Terminal Control")
        with st.expander("🔍 필터 설정", expanded=True):
            min_price = st.number_input("최소 주가 ($)", min_value=0.0, value=10.0, step=1.0)
            rs_min = st.slider("최소 RS 점수", 1, 99, 80)
            smr_f = st.multiselect("SMR 등급", ["A", "B", "C", "D", "E"], default=["A", "B"])
            ad_f = st.multiselect("수급(AD) 등급", ["A", "B", "C", "D", "E"], default=["A", "B", "C"])
        with st.expander("🏢 산업군 필터"):
            all_sec = sorted(df['sector'].unique())
            sel_sec = [s for s in all_sec if st.checkbox(s, value=(s != 'Unknown'))]

    mask = (df['price'] >= min_price) & (df['rs_score'] >= rs_min) & \
           (df['smr_grade'].isin(smr_f)) & (df['ad_rating'].isin(ad_f)) & (df['sector'].isin(sel_sec))
    f_df = df[mask].sort_values('rs_score', ascending=False)

    col_l, col_r = st.columns([2.5, 4])
    with col_l:
        st.subheader(f"Leaders ({len(f_df)})")
        display_list = f_df.rename(columns={'symbol': 'Ticker', 'price': 'Price', 'rs_score': 'RS', 'smr_grade': 'SMR', 'ad_rating': 'AD', 'sector': 'Sector'})
        sel = st.dataframe(display_list[['Ticker', 'Price', 'RS', 'SMR', 'AD', 'Sector']], 
                           use_container_width=True, hide_index=True, on_select="rerun", selection_mode="single-row", height=850)

    with col_r:
        if len(sel.selection.rows) > 0:
            row = f_df.iloc[sel.selection.rows[0]]
            ticker = row['symbol']
            
            # 상단 지표 카드
            st.markdown(f"""<div style="display:flex; gap:15px; margin-bottom:25px;">
                <div class="metric-card" style="flex:1;"><div class="metric-label">Stock RS</div><div class="metric-value">{row['rs_score']}</div></div>
                <div class="metric-card" style="flex:1;"><div class="metric-label">SMR Grade</div><div class="metric-value">{row['smr_grade']}</div></div>
                <div class="metric-card" style="flex:1;"><div class="metric-label">AD Rating</div><div class="metric-value">{row['ad_rating']}</div></div>
                <div class="metric-card" style="flex:1.5;"><div class="metric-label">Sector</div><div class="metric-value" style="color:#fff!important; font-size:1.2rem;">{row['sector']}</div></div>
            </div>""", unsafe_allow_html=True)

            q_inc, a_inc, q_bal, a_bal, info = get_detailed_info(ticker)
            t_chart, t_fin, t_check, t_biz = st.tabs(["📊 차트", "🧾 재무제표", "🛡️ 체크리스트", "🏢 개요"])

            with t_chart:
                components.html(f"""
                    <div id="tv_chart" style="height:700px; border-radius:12px; border:1px solid #4a5161; overflow:hidden;"></div>
                    <script src="https://s3.tradingview.com/tv.js"></script>
                    <script>new TradingView.widget({{"autosize":true,"symbol":"{ticker}","interval":"D","theme":"dark","style":"1","locale":"kr","toolbar_bg":"#f1f3f6","enable_publishing":false,"withdateranges":true,"hide_side_toolbar":false,"allow_symbol_change":true,"studies":["MAExp@tv-basicstudies","MAExp@tv-basicstudies","RSI@tv-basicstudies"],"container_id":"tv_chart"}});</script>
                """, height=710)

            with t_fin:
                # 1. 성장률 요약표
                st.markdown("#### 📈 성장률 요약 (Growth Summary)")
                q_rev = q_inc.loc['Total Revenue'] if 'Total Revenue' in q_inc.index else pd.Series()
                q_eps = q_inc.loc['Basic EPS'] if 'Basic EPS' in q_inc.index else pd.Series()
                a_rev = a_inc.loc['Total Revenue'] if 'Total Revenue' in a_inc.index else pd.Series()
                
                sum_col1, sum_col2 = st.columns(2)
                with sum_col1:
                    st.write("**분기 성장률 (QoQ %)**")
                    qoq_df = pd.DataFrame({'매출(%)': calc_growth(q_rev, 1), 'EPS(%)': calc_growth(q_eps, 1)}).head(4)
                    st.dataframe(qoq_df.style.format("{:.1f}"), use_container_width=True)
                with sum_col2:
                    st.write("**연간 성장률 (YoY %)**")
                    yoy_df = pd.DataFrame({'매출(%)': calc_growth(a_rev, 1)}).head(4)
                    st.dataframe(yoy_df.style.format("{:.1f}"), use_container_width=True)

                # 2. 상세 재무제표 (한글화 + 1000단위 포맷팅)
                def format_fin_df(df_in):
                    target = df_in.reindex(list(FIN_MAP.keys())).dropna(how='all')
                    target.index = [FIN_MAP.get(i, i) for i in target.index]
                    disp = target.copy()
                    for idx in disp.index:
                        if "EPS" not in str(idx): disp.loc[idx] = disp.loc[idx] / 1000
                    eps_rows = [i for i in disp.index if "EPS" in str(i)]
                    return disp.style.format(precision=0, thousands=",").format(precision=2, subset=pd.IndexSlice[eps_rows, :])

                st.markdown("#### 📅 연간 상세 (Annual - $1,000)")
                st.dataframe(format_fin_df(pd.concat([a_inc, a_bal])), use_container_width=True)
                st.markdown("#### ⏱️ 분기 상세 (Quarterly - $1,000)")
                st.dataframe(format_fin_df(pd.concat([q_inc, q_bal])), use_container_width=True)

            with t_check:
                # 체크리스트 데이터 계산
                cur_eps_growth = calc_growth(q_eps, 4).iloc[0] if len(q_eps) >= 5 else 0
                
                st.subheader("🛡️ 주도주 판별 시스템")
                c1, c2 = st.columns(2)
                with c1:
                    st.markdown("### 🟢 CANSLIM (윌리엄 오닐)")
                    st.checkbox(f"**C**: 현재 분기 EPS 25%↑ (현재: {cur_eps_growth:.1f}%)", value=cur_eps_growth >= 25)
                    st.checkbox("**A**: 연간 이익 증가 (ROE 17%↑)", value=True)
                    st.checkbox("**N**: 새로운 제품, 경영진, 신고가", value=True)
                    st.checkbox(f"**S**: 주식 수급 (AD Rating: {row['ad_rating']})", value=row['ad_rating'] in ['A','B'])
                    st.checkbox(f"**L**: 주도주 여부 (RS 점수: {row['rs_score']})", value=row['rs_score'] >= 80)
                    st.checkbox(f"**I**: 기관의 관심 (SMR: {row['smr_grade']})", value=row['smr_grade'] in ['A','B'])
                    st.checkbox("**M**: 시장의 방향성 (상승장 확인)", value=True)
                with c2:
                    st.markdown("### 🔵 트렌드 템플릿 (마크 미너비니)")
                    st.checkbox("1. 현재 주가 > 150일 & 200일 이평선", value=True)
                    st.checkbox("2. 150일 이평선 > 200일 이평선", value=True)
                    st.checkbox("3. 200일 이평선 우상향 (최소 1개월)", value=True)
                    st.checkbox("4. 50일 이평선 > 150일 & 200일 이평선", value=True)
                    st.checkbox("5. 현재 주가 > 52주 최저가 대비 30% 위", value=True)
                    st.checkbox("6. 현재 주가 < 52주 최고가 대비 25% 이내", value=True)
                    st.checkbox(f"7. RS 점수 80 이상 (현재: {row['rs_score']})", value=row['rs_score'] >= 80)
                    st.checkbox("8. 현재 주가가 50일 이평선 위에서 유지", value=True)

            with t_biz:
                st.subheader(info.get('longName', ticker))
                st.markdown(f"<div style='background-color:{TABLE_BG_COLOR}; padding:25px; border-radius:12px;'>{info.get('longBusinessSummary', 'N/A')}</div>", unsafe_allow_html=True)
        else:
            st.info("👈 왼쪽 리스트에서 종목을 선택해 주세요.")
