import streamlit as st
import pandas as pd
import sqlite3
import yfinance as yf
import streamlit.components.v1 as components
import os

# --- 0. 페이지 설정 ---
st.set_page_config(layout="wide", page_title="Institutional Terminal v2")

BG_COLOR = "#161C27"
TABLE_BG_COLOR = "#363C4C"

st.markdown(f"""
    <style>
    .stApp {{ background-color: {BG_COLOR} !important; color: #ccd6f6; }}
    .metric-card {{ background-color: {TABLE_BG_COLOR}; border-radius: 12px; padding: 20px; border: 1px solid #4a5161; text-align: center; }}
    .metric-value {{ font-size: 1.8rem; font-weight: 800; color: #64ffda !important; }}
    </style>
    """, unsafe_allow_html=True)

# --- 1. 데이터 로드 ---
def get_data():
    if not os.path.exists('ibd_system.db'): return pd.DataFrame()
    conn = sqlite3.connect('ibd_system.db')
    df = pd.read_sql("SELECT * FROM repo_results", conn)
    conn.close()
    return df

@st.cache_data(ttl=3600)
def get_detailed_info(ticker):
    s = yf.Ticker(ticker)
    # 연간/분기 재무제표 모두 가져오기
    return s.quarterly_income_stmt, s.income_stmt, s.quarterly_balance_sheet, s.balance_sheet, s.info

# --- 2. 메인 로직 ---
df = get_data()

if not df.empty:
    with st.sidebar:
        st.header("🎛️ Terminal Control")
        min_price = st.number_input("최소 주가 ($)", min_value=0.0, value=10.0, step=1.0)
        rs_min = st.slider("최소 RS 점수", 1, 99, 80)
        
        # 섹터 필터 (Unknown 제외 옵션 제공)
        all_sec = sorted(df['sector'].unique())
        selected_sec = st.multiselect("섹터 선택", all_sec, default=[s for s in all_sec if s != 'Unknown'])

    mask = (df['price'] >= min_price) & (df['rs_score'] >= rs_min) & (df['sector'].isin(selected_sec))
    f_df = df[mask].sort_values('rs_score', ascending=False)

    col_l, col_r = st.columns([1, 2.5])

    with col_l:
        st.subheader(f"Leaders ({len(f_df)})")
        sel = st.dataframe(
            f_df[['symbol', 'price', 'rs_score', 'smr_grade', 'ad_rating', 'sector']],
            use_container_width=True, hide_index=True, on_select="rerun", selection_mode="single-row", height=800
        )

    with col_r:
        if len(sel.selection.rows) > 0:
            row = f_df.iloc[sel.selection.rows[0]]
            ticker = row['symbol']
            
            # 상단 지표 카드
            st.markdown(f"""<div style="display:flex; gap:10px; margin-bottom:20px;">
                <div class="metric-card" style="flex:1;"><div style="color:#aeb9cc; font-size:0.7rem;">STOCK RS</div><div class="metric-value">{row['rs_score']}</div></div>
                <div class="metric-card" style="flex:1;"><div style="color:#aeb9cc; font-size:0.7rem;">SMR GRADE</div><div class="metric-value">{row['smr_grade']}</div></div>
                <div class="metric-card" style="flex:1;"><div style="color:#aeb9cc; font-size:0.7rem;">AD RATING</div><div class="metric-value">{row['ad_rating']}</div></div>
                <div class="metric-card" style="flex:2;"><div style="color:#aeb9cc; font-size:0.7rem;">SECTOR</div><div style="font-size:1.2rem; font-weight:700;">{row['sector']}</div></div>
            </div>""", unsafe_allow_html=True)

            q_inc, a_inc, q_bal, a_bal, info = get_detailed_info(ticker)
            t_chart, t_fin, t_biz = st.tabs(["📊 ADVANCED CHART", "🧾 FINANCIALS", "🏢 SUMMARY"])

            with t_chart:
                # 트레이딩뷰 Advanced 위젯: 이동평균선 및 보조지표 포함
                tradingview_html = f"""
                <div id="tradingview_adv" style="height:600px;"></div>
                <script type="text/javascript" src="https://s3.tradingview.com/tv.js"></script>
                <script type="text/javascript">
                new TradingView.widget({{
                  "width": "100%", "height": 600, "symbol": "{ticker}",
                  "interval": "D", "timezone": "Etc/UTC", "theme": "dark", "style": "1",
                  "locale": "en", "toolbar_bg": "#f1f3f6", "enable_publishing": false,
                  "hide_side_toolbar": false, "allow_symbol_change": true,
                  "details": true, "hotlist": true, "calendar": true,
                  "studies": [ "MAExp@tv-basicstudies", "MAExp@tv-basicstudies", "RSI@tv-basicstudies" ],
                  "container_id": "tradingview_adv"
                }});
                </script>"""
                components.html(tradingview_html, height=620)

            with t_fin:
                col_fin1, col_fin2 = st.columns(2)
                with col_fin1:
                    st.write("### 📅 Annual Income Statement")
                    st.dataframe(a_inc.head(10), use_container_width=True)
                with col_fin2:
                    st.write("### 📅 Annual Balance Sheet")
                    st.dataframe(a_bal.head(10), use_container_width=True)
                
                st.divider()
                st.write("### ⏱️ Quarterly Income Statement")
                st.dataframe(q_inc.head(10), use_container_width=True)

            with t_biz:
                st.subheader(info.get('longName', ticker))
                st.write(info.get('longBusinessSummary', 'No summary available.'))
        else:
            st.info("좌측 리스트에서 종목을 선택하세요.")
