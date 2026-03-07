# -*- coding: utf-8 -*-
# TAPEWATCHR/my-stock 대시보드 개선판
# 1) 사이드바 필터 UX  2) 테이블/재무 스타일  3) 개요 가독성  4) RS 차트 Y축 0~100 고정

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

[data-testid="stDataFrame"] {{ border-radius: 10px; overflow: hidden; border: 1px solid #4a5161; font-size: 0.9rem; }}
[data-testid="stDataFrame"] th {{ background: #2d3340 !important; color: #64ffda !important; font-weight: 600; padding: 10px 12px !important; }}
[data-testid="stDataFrame"] td {{ padding: 8px 12px !important; color: #ccd6f6; }}
[data-testid="stDataFrame"] tr:hover td {{ background: #3d4354 !important; }}

.overview-panel {{ background: {OVERVIEW_BG}; color: {OVERVIEW_TEXT}; padding: 1.5rem 1.75rem; border-radius: 12px; border: 1px solid #4a5161; line-height: 1.7; font-size: 0.95rem; }}
.overview-panel h2 {{ color: #64ffda !important; margin-bottom: 1rem; }}
.overview-panel p {{ color: {OVERVIEW_TEXT} !important; }}

/* 사이드바 선택칸: O·글자·네모칸 축소 */
[data-testid="stSidebar"] .stButton > button {{
  min-width: 1.6rem !important;
  padding: 0.2rem 0.4rem !important;
  font-size: 0.7rem !important;
  line-height: 1.2 !important;
  white-space: nowrap;
  overflow: visible;
}}

/* 사이드바 폭 */
[data-testid="stSidebar"] {{ min-width: 400px !important; }}
[data-testid="stSidebar"] [data-testid="stVerticalBlock"] > div {{ width: 100% !important; min-width: 0 !important; }}

/* 종목 리스트 테이블 폭 확대 (Sector/Industry 풀네임 전부 보이게) */
[data-testid="stDataFrame"] {{ min-width: 580px !important; width: 100% !important; }}
[data-testid="stDataFrame"] th:nth-child(8), [data-testid="stDataFrame"] td:nth-child(8) {{ min-width: 300px !important; white-space: normal !important; word-break: keep-all; }}
/* 리스트가 들어가는 왼쪽 컬럼 폭 보장 */
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

            # SMR 등급: A,B,C,D,E,전체 토글 (빨강=선택, 흰색=해제)
            if "smr_sel" not in st.session_state:
                st.session_state.smr_sel = ["A", "B"]
            st.caption("SMR 등급")
            smr_cols1 = st.columns(3)
            for i, g in enumerate(["A", "B", "C"]):
                with smr_cols1[i]:
                    sel = g in st.session_state.smr_sel
                    lbl = f"●{g}" if sel else f"○{g}"
                    if st.button(lbl, key=f"smr_{g}"):
                        if g in st.session_state.smr_sel:
                            st.session_state.smr_sel = [x for x in st.session_state.smr_sel if x != g]
                        else:
                            st.session_state.smr_sel = sorted(st.session_state.smr_sel + [g])
                        st.rerun()
            smr_cols2 = st.columns(3)
            for i, g in enumerate(["D", "E", "전체"]):
                with smr_cols2[i]:
                    sel = g in st.session_state.smr_sel if g != "전체" else set(st.session_state.smr_sel) == {"A","B","C","D","E"}
                    lbl = f"●{g}" if sel else f"○{g}"
                    if st.button(lbl, key=f"smr_{g}"):
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

            # 수급(AD) 등급: 상하 2줄·3열 배치
            if "ad_sel" not in st.session_state:
                st.session_state.ad_sel = ["A", "B", "C"]
            st.caption("수급(AD) 등급")
            ad_cols1 = st.columns(3)
            for i, g in enumerate(["A", "B", "C"]):
                with ad_cols1[i]:
                    sel = g in st.session_state.ad_sel
                    lbl = f"●{g}" if sel else f"○{g}"
                    if st.button(lbl, key=f"ad_{g}"):
                        if g in st.session_state.ad_sel:
                            st.session_state.ad_sel = [x for x in st.session_state.ad_sel if x != g]
                        else:
                            st.session_state.ad_sel = sorted(st.session_state.ad_sel + [g])
                        st.rerun()
            ad_cols2 = st.columns(3)
            for i, g in enumerate(["D", "E", "전체"]):
                with ad_cols2[i]:
                    sel = g in st.session_state.ad_sel if g != "전체" else set(st.session_state.ad_sel) == {"A","B","C","D","E"}
                    lbl = f"●{g}" if sel else f"○{g}"
                    if st.button(lbl, key=f"ad_{g}"):
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
            lbl_sec_all = "●전체" if all_sel else "○전체"
            if st.button(lbl_sec_all, key="sec_all"):
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
                            lbl = f"●{s}" if sel else f"○{s}"
                            if st.button(lbl, key=f"sec_{s}"):
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
        display_list['ADV($M)'] = (display_list['adv_50'] / 1_000_000).round(1)
        display_list = display_list.rename(columns={
            'symbol': 'Ticker', 'price': 'Price', 'rs_score': 'RS',
            'smr_grade': 'SMR', 'ad_rating': 'AD', 'industry_rs_score': 'Ind RS', 'sector': 'Sector'
        })
        sel = st.dataframe(
            display_list[['Ticker', 'Price', 'ADV($M)', 'RS', 'SMR', 'AD', 'Ind RS', 'Sector']],
            hide_index=True, on_select="rerun", selection_mode="single-row", height=850,
            column_config={
                "Sector": st.column_config.TextColumn("Sector", width=360),
                "Ticker": st.column_config.TextColumn("Ticker", width="small"),
                "Price": st.column_config.NumberColumn("Price", width="small"),
                "ADV($M)": st.column_config.NumberColumn("ADV($M)", width="small"),
                "RS": st.column_config.NumberColumn("RS", width="small"),
                "SMR": st.column_config.TextColumn("SMR", width="small"),
                "AD": st.column_config.TextColumn("AD", width="small"),
                "Ind RS": st.column_config.NumberColumn("Ind RS", width="small"),
            }
        )

    with col_r:
        if len(sel.selection.rows) > 0:
            row = f_df.iloc[sel.selection.rows[0]]
            ticker = row['symbol']

            st.markdown(f"""
            **Stock RS** {row['rs_score']} · **SMR** {row['smr_grade']} · **AD** {row['ad_rating']} · **Ind RS** {row['industry_rs_score']} · {row['sector']}
            """, unsafe_allow_html=True)

            q_inc, a_inc, q_bal, a_bal, info = get_detailed_info(ticker)
            t_chart, t_fin, t_check, t_biz = st.tabs(["📊 차트", "🧾 재무제표", "🛡️ 체크리스트", "🏢 개요"])

            with t_chart:
                # 트레이딩뷰: 사용자 임베드 URL이 있으면 해당 차트 표시 (계정에서 만든 차트 레이아웃 사용 가능)
                if "tv_embed_url" not in st.session_state:
                    st.session_state.tv_embed_url = ""
                with st.expander("📌 내 트레이딩뷰 차트 사용하기", expanded=False):
                    st.caption("TradingView에서 차트를 꾸민 뒤 [공유] → [차트 임베드]에서 URL을 복사해 붙여넣으면, 해당 차트가 여기서 표시됩니다.")
                    tv_url = st.text_input("TradingView 임베드 URL (선택)", value=st.session_state.tv_embed_url, key="tv_embed_input", placeholder="https://www.tradingview.com/chart/... 또는 임베드 URL")
                    if tv_url and tv_url != st.session_state.tv_embed_url:
                        st.session_state.tv_embed_url = tv_url
                    if st.session_state.tv_embed_url:
                        if st.button("기본 차트로 되돌리기", key="tv_reset"):
                            st.session_state.tv_embed_url = ""
                            st.rerun()
                if st.session_state.tv_embed_url:
                    embed_url = st.session_state.tv_embed_url.strip().replace("SYMBOL", ticker).replace("{{ticker}}", ticker)
                    if "tradingview.com" in embed_url:
                        components.html(f'<iframe src="{embed_url}" height="710" style="width:100%; border:0;"></iframe>', height=715)
                    else:
                        st.warning("TradingView 차트/임베드 URL을 입력해 주세요.")
                        st.session_state.tv_embed_url = ""
                else:
                    components.html(f"""
                    <script type="text/javascript" src="https://s3.tradingview.com/tv.js"></script>
                    <div id="tv_chart" style="height: 710px;"></div>
                    <script type="text/javascript">
                    new TradingView.widget({{"autosize":true,"symbol":"{ticker}","interval":"D","theme":"dark","style":"1","locale":"kr","toolbar_bg":"#f1f3f6","enable_publishing":false,"withdateranges":true,"hide_side_toolbar":false,"allow_symbol_change":true,"studies":["MAExp@tv-basicstudies","MAExp@tv-basicstudies","RSI@tv-basicstudies"],"container_id":"tv_chart"}});
                    </script>
                    """, height=710)

                st.markdown("#### 📈 RS 점수 추세 (1~100 고정)", unsafe_allow_html=True)
                rs_hist_df = get_rs_history(ticker)
                if not rs_hist_df.empty and len(rs_hist_df) > 1:
                    rs_hist_df = rs_hist_df.copy()
                    rs_hist_df['date'] = pd.to_datetime(rs_hist_df['date'])
                    rs_hist_df['rs_score'] = rs_hist_df['rs_score'].clip(0, 100)
                    chart = alt.Chart(rs_hist_df).mark_line(color='#64ffda', strokeWidth=2).encode(
                        x=alt.X('date:T', title='날짜'),
                        y=alt.Y('rs_score:Q', title='RS', scale=alt.Scale(domain=[0, 100]))
                    ).properties(height=320)
                    st.altair_chart(chart, use_container_width=True)
                else:
                    st.info("⏳ 오늘부터 RS 점수가 누적됩니다. 내일 자동 업데이트 이후부터 추세선이 나타납니다.")

            with t_fin:
                q_rev = q_inc.loc['Total Revenue'] if 'Total Revenue' in q_inc.index else pd.Series(dtype=float)
                q_op = q_inc.loc['Operating Income'] if 'Operating Income' in q_inc.index else pd.Series(dtype=float)
                q_eps = q_inc.loc['Basic EPS'] if 'Basic EPS' in q_inc.index else pd.Series(dtype=float)
                a_rev = a_inc.loc['Total Revenue'] if 'Total Revenue' in a_inc.index else pd.Series(dtype=float)
                a_op = a_inc.loc['Operating Income'] if 'Operating Income' in a_inc.index else pd.Series(dtype=float)
                a_eps = a_inc.loc['Basic EPS'] if 'Basic EPS' in a_inc.index else pd.Series(dtype=float)

                st.markdown("#### 📈 분기 성장률 (QoQ %)")
                qoq_df = pd.DataFrame({
                    '분기': format_date_idx(q_rev.index, 'Q'),
                    '매출 성장(%)': calc_growth(q_rev, 1),
                    '영업이익 성장(%)': calc_growth(q_op, 1),
                    'EPS 성장(%)': calc_growth(q_eps, 1)
                }).set_index('분기').head(4)
                st.dataframe(qoq_df.style.format("{:.1f}"), use_container_width=True)

                st.markdown("#### 📅 연간 성장률 (YoY %)")
                yoy_df = pd.DataFrame({
                    '연도': format_date_idx(a_rev.index, 'A'),
                    '매출 성장(%)': calc_growth(a_rev, 1),
                    '영업이익 성장(%)': calc_growth(a_op, 1),
                    'EPS 성장(%)': calc_growth(a_eps, 1)
                }).set_index('연도').head(4)
                st.dataframe(yoy_df.style.format("{:.1f}"), use_container_width=True)

                def format_fin_df(df_in, date_type='Q'):
                    target = df_in.reindex(list(FIN_MAP.keys())).dropna(how='all')
                    target.index = [FIN_MAP.get(i, i) for i in target.index]
                    target.columns = format_date_idx(target.columns, date_type)
                    disp = target.copy()
                    for idx in disp.index:
                        if "EPS" not in str(idx):
                            disp.loc[idx] = disp.loc[idx] / 1000
                    eps_rows = [i for i in disp.index if "EPS" in str(i)]
                    return disp.style.format(precision=0, thousands=",").format(precision=2, subset=pd.IndexSlice[eps_rows, :])

                st.markdown("#### 🧾 상세 재무 데이터 ($1,000)")
                st.write("**연간 상세**")
                st.dataframe(format_fin_df(pd.concat([a_inc, a_bal]), 'A'), use_container_width=True)
                st.write("**분기 상세**")
                st.dataframe(format_fin_df(pd.concat([q_inc, q_bal]), 'Q'), use_container_width=True)

            with t_check:
                cur_eps_growth = calc_growth(q_eps, 4).iloc[0] if len(q_eps) >= 5 else 0
                st.subheader("🛡️ 주도주 판별 시스템")
                c1, c2 = st.columns(2)
                with c1:
                    st.markdown("### 🟢 CANSLIM (오닐)")
                    st.checkbox(f"**C**: 분기 EPS 25%↑ ({cur_eps_growth:.1f}%)", value=cur_eps_growth >= 25)
                    st.checkbox("**A**: 연간 이익 증가 (ROE 17%↑)", value=True)
                    st.checkbox("**N**: 신고가 또는 새로운 재료", value=True)
                    st.checkbox(f"**S**: 공급과 수요 (AD: {row['ad_rating']})", value=row['ad_rating'] in ['A', 'B'])
                    st.checkbox(f"**L**: 시장 주도주 (RS: {row['rs_score']})", value=row['rs_score'] >= 80)
                    st.checkbox(f"**I**: 기관 매집 (SMR: {row['smr_grade']})", value=row['smr_grade'] in ['A', 'B'])
                    st.checkbox("**M**: 시장 대세 상승 확인", value=True)
                with c2:
                    st.markdown("### 🔵 트렌드 템플릿 (미너비니)")
                    st.checkbox("1. 주가 > 150일 & 200일 MA", value=True)
                    st.checkbox("2. 150일 MA > 200일 MA", value=True)
                    st.checkbox("3. 200일 MA 우상향 유지", value=True)
                    st.checkbox("4. 50일 MA > 150일 & 200일 MA", value=True)
                    st.checkbox("5. 현재가 > 52주 저가 대비 30%↑", value=True)
                    st.checkbox("6. 현재가 < 52주 고가 대비 25% 이내", value=True)
                    st.checkbox(f"7. RS 점수 80 이상 (현재: {row['rs_score']})", value=row['rs_score'] >= 80)
                    st.checkbox("8. 주가가 50일 MA 위에서 지지", value=True)

            with t_biz:
                st.subheader("🏢 개요")
                long_name = info.get('longName', ticker)
                summary_en = info.get('longBusinessSummary', 'N/A')
                summary_ko = translate_to_korean(summary_en)
                st.markdown(
                    f'<div class="overview-panel"><h2>{long_name}</h2><p>{summary_ko}</p></div>',
                    unsafe_allow_html=True
                )
        else:
            st.info("👈 왼쪽 리스트에서 종목을 선택해 주세요.")
