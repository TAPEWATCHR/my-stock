import streamlit as st
import pandas as pd
import sqlite3
import yfinance as yf
import streamlit.components.v1 as components

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
    conn = sqlite3.connect('ibd_system.db')
    df = pd.read_sql("SELECT m.symbol, r.* FROM security_master m JOIN repo_results r ON m.security_id = r.security_id", conn)
    conn.close()
    return df

# 분기 표기 수정: 2024 Q4 형식
def format_quarter(idx):
    return [f"{i.year} Q{(i.month-1)//3 + 1}" if hasattr(i, 'year') else str(i) for i in idx]

# 연도 표기 수정: 2024 형식
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
    return s.quarterly_income_stmt, s.quarterly_balance_sheet, s.income_stmt, s.balance_sheet, s.info

# --- 2. 메인 화면 ---
try:
    df = get_data()
    with st.sidebar:
        st.header("🎛️ Terminal Control")
        with st.expander("🔍 종목 스캐너 필터", expanded=True):
            rs_min = st.slider("최소 RS 점수", 1, 99, 80)
            ind_rs_min = st.slider("최소 산업군 RS", 1, 99, 50)
            smr_f = st.multiselect("SMR 등급", ["A", "B", "C", "D", "E"], default=["A", "B"])
            ad_f = st.multiselect("수급(AD) 등급", ["A", "B", "C", "D", "E"], default=["A", "B", "C"])
        with st.expander("🏢 산업군(Sector) 필터"):
            all_sec = sorted(df['sector'].unique())
            sel_sec = [s for s in all_sec if st.checkbox(s, value=True)]

    mask = (df['rs_score'] >= rs_min) & (df['industry_rs_score'] >= ind_rs_min) & \
           (df['smr_grade'].isin(smr_f)) & (df['ad_rating'].isin(ad_f)) & (df['sector'].isin(sel_sec))
    f_df = df[mask].sort_values('rs_score', ascending=False)
    
    col_l, col_r = st.columns([LIST_RATIO, 4])
    with col_l:
        st.subheader(f"Leaders ({len(f_df)})")
        d_df = f_df.rename(columns={'symbol': 'Ticker', 'rs_score': 'RS', 'smr_grade': 'SMR', 'ad_rating': 'AD', 'industry_rs_score': 'Ind RS', 'sector': 'Sector'})
        sel = st.dataframe(d_df[['Ticker', 'RS', 'SMR', 'AD', 'Ind RS', 'Sector']], use_container_width=True, hide_index=True, on_select="rerun", selection_mode="single-row", height=850)

    with col_r:
        if len(sel.selection.rows) > 0:
            row = f_df.iloc[sel.selection.rows[0]]
            ticker = row['symbol']
            
            st.markdown(f"""<div style="display:flex; gap:15px; margin-bottom:25px;">
                <div class="metric-card" style="flex:1;"><div class="metric-label">Stock RS</div><div class="metric-value">{row['rs_score']}</div></div>
                <div class="metric-card" style="flex:1;"><div class="metric-label">SMR Grade</div><div class="metric-value">{row['smr_grade']}</div></div>
                <div class="metric-card" style="flex:1;"><div class="metric-label">AD Rating</div><div class="metric-value">{row['ad_rating']}</div></div>
                <div class="metric-card" style="flex:1.5;"><div class="metric-label">Ind RS / Sector</div><div class="metric-value" style="color:#fff!important;">{row['industry_rs_score']}</div><div style="color:#8892b0; font-size:0.8rem;">{row['sector']}</div></div>
            </div>""", unsafe_allow_html=True)

            q_inc, q_bal, a_inc, a_bal, info = get_detailed_info(ticker)
            t_chart, t_fin, t_check, t_biz = st.tabs(["CHART", "FINANCIAL", "CHECKLIST", "SUMMARY"])

            with t_chart:
                components.html(f"""<div style="height:700px; border-radius:12px; overflow:hidden; border:1px solid #4a5161;"><div id="tv_chart" style="height:100%;"></div><script src="https://s3.tradingview.com/tv.js"></script><script>new TradingView.widget({{"autosize":true,"symbol":"{ticker}","interval":"D","theme":"dark","style":"1","locale":"kr","container_id":"tv_chart"}});</script></div>""", height=710)

            with t_fin:
                q_rev = q_inc.loc['Total Revenue'] if 'Total Revenue' in q_inc.index else pd.Series()
                q_op = q_inc.loc['Operating Income'] if 'Operating Income' in q_inc.index else pd.Series()
                q_eps = q_inc.loc['Basic EPS'] if 'Basic EPS' in q_inc.index else (q_inc.loc['Diluted EPS'] if 'Diluted EPS' in q_inc.index else pd.Series())
                a_rev = a_inc.loc['Total Revenue'] if 'Total Revenue' in a_inc.index else pd.Series()
                a_op = a_inc.loc['Operating Income'] if 'Operating Income' in a_inc.index else pd.Series()
                a_eps = a_inc.loc['Basic EPS'] if 'Basic EPS' in a_inc.index else (a_inc.loc['Diluted EPS'] if 'Diluted EPS' in a_inc.index else pd.Series())

                st.markdown("#### 📊 Growth Momentum (QoQ)")
                qoq_df = pd.DataFrame({'분기': format_quarter(q_rev.index), '매출 QoQ(%)': calc_growth_logic(q_rev, 1), '영업이익 QoQ(%)': calc_growth_logic(q_op, 1), 'EPS QoQ(%)': calc_growth_logic(q_eps, 1)}).set_index('분기').head(5)
                st.dataframe(qoq_df.style.format("{:.2f}"), use_container_width=True)

                st.markdown("#### 📈 Annual Growth (YoY)")
                yoy_df = pd.DataFrame({'연도': format_year(a_rev.index), '매출 YoY(%)': calc_growth_logic(a_rev, 1), '영업이익 YoY(%)': calc_growth_logic(a_op, 1), 'EPS YoY(%)': calc_growth_logic(a_eps, 1)}).set_index('연도')
                st.dataframe(yoy_df.style.format("{:.2f}"), use_container_width=True)

                # 3. Quarterly Details
                st.markdown("#### 🧾 Quarterly Details ($1,000)")
                q_target = pd.concat([q_inc, q_bal]).reindex(list(FIN_MAP.keys())).dropna(how='all')
                q_target.index = [FIN_MAP.get(i, i) for i in q_target.index]
                q_target.columns = format_quarter(q_target.columns)
                q_disp = q_target.copy()
                for idx in q_disp.index:
                    if "EPS" not in str(idx): q_disp.loc[idx] = q_disp.loc[idx] / 1000
                
                # 에러 해결: 인덱스 슬라이싱을 이용한 정밀 포맷팅
                eps_rows = [i for i in q_disp.index if "EPS" in str(i)]
                st.dataframe(q_disp.style.format(precision=0, thousands=",").format(precision=2, subset=pd.IndexSlice[eps_rows, :]), use_container_width=True)

                # 4. Annual Details
                st.markdown("#### 📅 Annual Details ($1,000)")
                a_target = pd.concat([a_inc, a_bal]).reindex(list(FIN_MAP.keys())).dropna(how='all')
                a_target.index = [FIN_MAP.get(i, i) for i in a_target.index]
                a_target.columns = format_year(a_target.columns)
                a_disp = a_target.copy()
                for idx in a_disp.index:
                    if "EPS" not in str(idx): a_disp.loc[idx] = a_disp.loc[idx] / 1000
                
                eps_rows_a = [i for i in a_disp.index if "EPS" in str(i)]
                st.dataframe(a_disp.style.format(precision=0, thousands=",").format(precision=2, subset=pd.IndexSlice[eps_rows_a, :]), use_container_width=True)

            with t_check:
                last_eps_yoy = calc_growth_logic(q_eps, 4).iloc[0] if len(q_eps) > 4 else 0
                ann_eps_yoy = calc_growth_logic(a_eps, 1).iloc[0] if len(a_eps) > 1 else 0
                
                st.subheader("🛡️ CANSLIM & Minervini Integrated Checklist")
                c1, c2 = st.columns(2)
                with c1:
                    st.markdown("### 🟢 William O'Neil's CANSLIM")
                    st.checkbox(f"**C**: Current Quarterly Earnings (EPS 25%↑, 현재: {last_eps_yoy:.1f}%)", value=last_eps_yoy >= 25)
                    st.checkbox(f"**A**: Annual Earnings Increase (25%↑, 현재: {ann_eps_yoy:.1f}%)", value=ann_eps_yoy >= 25)
                    st.checkbox(f"**N**: New Products, Management, or Highs", value=True)
                    st.checkbox(f"**S**: Supply and Demand (AD Rating: {row['ad_rating']})", value=row['ad_rating'] in ['A','B'])
                    st.checkbox(f"**L**: Leader or Laggard (RS 점수: {row['rs_score']})", value=row['rs_score'] >= 80)
                    st.checkbox(f"**I**: Institutional Sponsorship (SMR: {row['smr_grade']})", value=row['smr_grade'] in ['A','B'])
                    st.checkbox(f"**M**: Market Direction (Trend Following)", value=True)
                with c2:
                    st.markdown("### 🔵 Mark Minervini's Trend Template")
                    st.checkbox("1. 주가 > 150일 & 200일 이평선", value=True)
                    st.checkbox("2. 150일 MA > 200일 MA (정배열)", value=True)
                    st.checkbox("3. 200일 MA 우상향 (최소 1개월)", value=True)
                    st.checkbox("4. 50일 MA > 150일 & 200일 MA", value=True)
                    st.checkbox("5. 주가 > 52주 최저가 대비 30% 위", value=True)
                    st.checkbox("6. 주가 < 52주 최고가 대비 25% 이내", value=True)
                    st.checkbox(f"7. Stock RS 점수 80 이상 (현재: {row['rs_score']})", value=row['rs_score'] >= 80)
                    st.checkbox(f"8. Industry RS 점수 50 이상 (현재: {row['industry_rs_score']})", value=row['industry_rs_score'] >= 50)

            with t_biz:
                st.subheader(info.get('longName', ticker))
                st.markdown(f"<div class='summary-text' style='background-color:{TABLE_BG_COLOR}; padding:30px; border-radius:12px; border:1px solid #4a5161;'>{info.get('longBusinessSummary', 'N/A')}</div>", unsafe_allow_html=True)
        else:
            st.info("👈 왼쪽 리스트에서 종목을 선택해 주세요.")
except Exception as e:
    st.error(f"시스템 오류: {e}")
