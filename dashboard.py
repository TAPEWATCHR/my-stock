import streamlit as st
import pandas as pd
import sqlite3
import yfinance as yf
import streamlit.components.v1 as components
import os

# --- 0. 페이지 설정 및 디자인 고정 ---
st.set_page_config(layout="wide", page_title="Institutional Stock Terminal")

BG_COLOR = "#161C27"
TABLE_BG_COLOR = "#363C4C"
LIST_RATIO = 2.5 

st.markdown(f"""
    <style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;600;800&family=JetBrains+Mono:wght@400;500&display=swap');
    .stApp {{ background-color: {BG_COLOR} !important; font-family: 'Inter', sans-serif; }}
    h1, h2, h3, h4, h5, h6, p, label, span, .stCheckbox {{ color: #ccd6f6 !important; }}
    [data-testid="stDataFrame"], [data-testid="stTable"] {{ background-color: {TABLE_BG_COLOR} !important; border: 1px solid #4a5161 !important; }}
    [data-testid="stDataFrame"] div[role="gridcell"] {{ font-family: 'JetBrains Mono', monospace; font-size: 0.85rem; color: #ffffff !important; }}
    .metric-card {{ background-color: {TABLE_BG_COLOR}; border-radius: 12px; padding: 22px; border: 1px solid #4a5161; text-align: center; }}
    .metric-label {{ color: #aeb9cc !important; font-size: 0.75rem; font-weight: 600; text-transform: uppercase; }}
    .metric-value {{ font-size: 1.8rem; font-weight: 800; color: #64ffda !important; }}
    .summary-text {{ color: #f0f4f8 !important; line-height: 1.8; font-size: 1.05rem; }}
    </style>
    """, unsafe_allow_html=True)

# --- 1. 데이터 로직 ---
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

def format_quarter(idx):
    return [f"{i.year} Q{(i.month-1)//3 + 1}" if hasattr(i, 'year') else str(i) for i in idx]

def format_year(idx):
    return [str(i.year) if hasattr(i, 'year') else str(i) for i in idx]

def calc_growth_logic(series, periods):
    if series is None or series.empty: return pd.Series([0.0]*len(series))
    s = series.sort_index(ascending=True)
    growth = ((s - s.shift(periods)) / s.shift(periods).abs()) * 100
    return growth.sort_index(ascending=False)

@st.cache_data(ttl=3600)
def get_detailed_info(ticker):
    s = yf.Ticker(ticker)
    return s.quarterly_income_stmt, s.income_stmt, s.quarterly_balance_sheet, s.balance_sheet, s.info

# --- 2. 메인 화면 ---
try:
    df = get_data()
    if df.empty:
        st.warning("데이터베이스를 찾을 수 없습니다. 분석 업데이트가 완료될 때까지 기다려주세요.")
    else:
        with st.sidebar:
            st.header("🎛️ Terminal Control")
            with st.expander("🔍 종목 스캐너 필터", expanded=True):
                # 1. 가격 직접 입력 필터 (1달러 단위)
                min_price_input = st.number_input("최소 주가 ($)", min_value=0.0, value=10.0, step=1.0)
                
                # 2. RS 및 등급 필터
                rs_min = st.slider("최소 RS 점수", 1, 99, 80)
                ind_rs_min = st.slider("최소 산업군 RS", 1, 99, 50)
                smr_f = st.multiselect("SMR 등급", ["A", "B", "C", "D", "E"], default=["A", "B"])
                ad_f = st.multiselect("수급(AD) 등급", ["A", "B", "C", "D", "E"], default=["A", "B", "C"])
            
            with st.expander("🏢 산업군(Sector) 필터"):
                all_sec = sorted(df['sector'].unique())
                # Unknown 해결책: Unknown을 기본적으로 제외하거나 선택할 수 있게 함
                sel_sec = [s for s in all_sec if st.checkbox(s, value=(s != 'Unknown'))]

        # 데이터 필터링 적용
        mask = (df['price'] >= min_price_input) & \
               (df['rs_score'] >= rs_min) & (df['industry_rs_score'] >= ind_rs_min) & \
               (df['smr_grade'].isin(smr_f)) & (df['ad_rating'].isin(ad_f)) & (df['sector'].isin(sel_sec))
        
        f_df = df[mask].sort_values('rs_score', ascending=False)
        
        col_l, col_r = st.columns([LIST_RATIO, 4])
        with col_l:
            st.subheader(f"Leaders ({len(f_df)})")
            d_df = f_df.rename(columns={'symbol': 'Ticker', 'price': 'Price', 'rs_score': 'RS', 'smr_grade': 'SMR', 'ad_rating': 'AD', 'industry_rs_score': 'Ind RS', 'sector': 'Sector'})
            sel = st.dataframe(d_df[['Ticker', 'Price', 'RS', 'SMR', 'AD', 'Ind RS', 'Sector']], 
                               use_container_width=True, hide_index=True, on_select="rerun", 
                               selection_mode="single-row", height=850)

        with col_r:
            if len(sel.selection.rows) > 0:
                row = f_df.iloc[sel.selection.rows[0]]
                ticker = row['symbol']
                
                # 상단 지표 카드
                st.markdown(f"""<div style="display:flex; gap:15px; margin-bottom:25px;">
                    <div class="metric-card" style="flex:1;"><div class="metric-label">Stock RS</div><div class="metric-value">{row['rs_score']}</div></div>
                    <div class="metric-card" style="flex:1;"><div class="metric-label">SMR Grade</div><div class="metric-value">{row['smr_grade']}</div></div>
                    <div class="metric-card" style="flex:1;"><div class="metric-label">AD Rating</div><div class="metric-value">{row['ad_rating']}</div></div>
                    <div class="metric-card" style="flex:1.5;"><div class="metric-label">Ind RS / Sector</div><div class="metric-value" style="color:#fff!important;">{row['industry_rs_score']}</div><div style="color:#8892b0; font-size:0.8rem;">{row['sector']}</div></div>
                </div>""", unsafe_allow_html=True)

                q_inc, a_inc, q_bal, a_bal, info = get_detailed_info(ticker)
                t_chart, t_fin, t_check, t_biz = st.tabs(["📊 CHART", "🧾 FINANCIALS", "🛡️ CHECKLIST", "🏢 SUMMARY"])

                with t_chart:
                    # 트레이딩뷰 Advanced 위젯: 툴바와 인디케이터 포함
                    components.html(f"""
                        <div id="tv_chart_container" style="height:700px; border-radius:12px; overflow:hidden; border:1px solid #4a5161;"></div>
                        <script type="text/javascript" src="https://s3.tradingview.com/tv.js"></script>
                        <script type="text/javascript">
                        new TradingView.widget({{
                          "autosize": true, "symbol": "{ticker}", "interval": "D", "timezone": "Etc/UTC",
                          "theme": "dark", "style": "1", "locale": "kr", "toolbar_bg": "#f1f3f6",
                          "enable_publishing": false, "withdateranges": true, "hide_side_toolbar": false,
                          "allow_symbol_change": true, "details": true, "hotlist": true, "calendar": true,
                          "studies": [ "MAExp@tv-basicstudies", "MAExp@tv-basicstudies", "RSI@tv-basicstudies" ],
                          "container_id": "tv_chart_container"
                        }});
                        </script>
                    """, height=710)

                with t_fin:
                    # 연간 및 분기 재무제표 보강
                    st.markdown("#### 📅 Annual Financials (YoY)")
                    a_rev = a_inc.loc['Total Revenue'] if 'Total Revenue' in a_inc.index else pd.Series()
                    a_eps = a_inc.loc['Basic EPS'] if 'Basic EPS' in a_inc.index else pd.Series()
                    
                    col_a1, col_a2 = st.columns(2)
                    with col_a1:
                        st.write("**Annual Income Statement**")
                        st.dataframe(a_inc.head(10), use_container_width=True)
                    with col_a2:
                        st.write("**Annual Balance Sheet**")
                        st.dataframe(a_bal.head(10), use_container_width=True)

                    st.divider()
                    st.markdown("#### ⏱️ Quarterly Details (QoQ)")
                    st.dataframe(q_inc.head(12), use_container_width=True)

                with t_check:
                    # 체크리스트 복구
                    q_eps = q_inc.loc['Basic EPS'] if 'Basic EPS' in q_inc.index else pd.Series()
                    last_eps_yoy = calc_growth_logic(q_eps, 4).iloc[0] if len(q_eps) > 4 else 0
                    
                    st.subheader("🛡️ 주도주 판별 체크리스트")
                    c1, c2 = st.columns(2)
                    with c1:
                        st.markdown("### 🟢 CANSLIM (O'Neil)")
                        st.checkbox(f"**C**: 분기 EPS 25%↑ (현재: {last_eps_yoy:.1f}%)", value=last_eps_yoy >= 25)
                        st.checkbox(f"**S**: 수급 AD Rating ({row['ad_rating']})", value=row['ad_rating'] in ['A','B'])
                        st.checkbox(f"**L**: RS 점수 80↑ (현재: {row['rs_score']})", value=row['rs_score'] >= 80)
                        st.checkbox(f"**I**: 기관 매집 SMR ({row['smr_grade']})", value=row['smr_grade'] in ['A','B'])
                    with c2:
                        st.markdown("### 🔵 Trend Template (Minervini)")
                        st.checkbox("주가 > 150일 & 200일 이평선", value=True)
                        st.checkbox("RS 점수 80 이상", value=row['rs_score'] >= 80)
                        st.checkbox(f"산업군 주도 여부 (Ind RS: {row['industry_rs_score']})", value=row['industry_rs_score'] >= 70)

                with t_biz:
                    st.subheader(info.get('longName', ticker))
                    st.markdown(f"<div class='summary-text' style='background-color:{TABLE_BG_COLOR}; padding:30px; border-radius:12px; border:1px solid #4a5161;'>{info.get('longBusinessSummary', 'N/A')}</div>", unsafe_allow_html=True)
            else:
                st.info("👈 왼쪽 리스트에서 종목을 선택해 주세요.")

except Exception as e:
    st.error(f"시스템 오류: {e}")
