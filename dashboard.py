# -*- coding: utf-8 -*-
import streamlit as st
import pandas as pd
import sqlite3
import yfinance as yf
import streamlit.components.v1 as components
import os
import altair as alt
import time

# --- [추가] 즐겨찾기 데이터베이스 함수 ---
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
[data-testid="stDataFrame"] {{ border-radius: 10px; overflow: hidden; border: 1px solid #4a5161; font-size: 0.95rem; width: 100% !important; }}
[data-testid="stDataFrame"] th {{ background: #2d3340 !important; color: #64ffda !important; font-weight: 600; padding: 10px 12px !important; }}
[data-testid="stDataFrame"] td {{ padding: 8px 12px !important; color: #ccd6f6; }}
[data-testid="stDataFrame"] tr:hover td {{ background: #3d4354 !important; }}
.overview-panel {{ background: {OVERVIEW_BG}; color: {OVERVIEW_TEXT}; padding: 1.5rem 1.75rem; border-radius: 12px; border: 1px solid #4a5161; line-height: 1.7; font-size: 0.95rem; }}
.overview-panel h2 {{ color: #64ffda !important; margin-bottom: 1rem; }}
.overview-panel p {{ color: {OVERVIEW_TEXT} !important; }}
[data-testid="stSidebar"] .stButton > button {{ width: auto !important; min-width: 0 !important; padding: 0.1rem 0.5rem !important; font-size: 0.75rem !important; line-height: 1.2 !important; min-height: 24px !important; white-space: nowrap; }}
[data-testid="stSidebar"] button[kind="primary"] {{ color: #ff4b4b !important; border-color: #ff4b4b !important; background-color: transparent !important; }}
[data-testid="stSidebar"] button[kind="primary"]:hover {{ color: #ff7676 !important; border-color: #ff7676 !important; }}
[data-testid="stSidebar"] [data-testid="stVerticalBlock"] > div {{ width: 100% !important; min-width: 0 !important; }}
</style>
""", unsafe_allow_html=True)

# --- 2. 유틸리티 함수 ---
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
        query = f"SELECT * FROM rs_history WHERE symbol = '{ticker}' ORDER BY date ASC"
        hist_df = pd.read_sql(query, conn)
    except Exception: hist_df = pd.DataFrame()
    conn.close()
    return hist_df

def format_date_idx(idx, type='Q'):
    if type == 'Q': return [f"{i.year} Q{(i.month-1)//3 + 1}" if hasattr(i, 'year') else str(i) for i in idx]
    return [str(i.year) if hasattr(i, 'year') else str(i) for i in idx]

def calc_growth(series, periods):
    if series is None or series.empty: return pd.Series(dtype=float)
    s = series.sort_index(ascending=True)
    growth = ((s - s.shift(periods)) / s.shift(periods).abs()) * 100
    return growth.sort_index(ascending=False)

@st.cache_data(ttl=3600)
def get_detailed_info(ticker):
    max_retries = 3
    for attempt in range(max_retries):
        try:
            s = yf.Ticker(ticker)
            return s.quarterly_income_stmt, s.income_stmt, s.quarterly_balance_sheet, s.balance_sheet, s.info
        except Exception as e:
            if attempt < max_retries - 1: time.sleep(2)
            else: raise YFDataFetchError(f"Rate Limit or Fetch Error: {str(e)}")

# --- 3. 메인 화면 ---
df = get_data()
if not df.empty:
    with st.sidebar:
        st.header("🎛️ Terminal Control")
        with st.expander("🔍 필터 설정", expanded=True):
            # [추가] 즐겨찾기 필터
            show_only_favs = st.checkbox("⭐ 즐겨찾기만 보기", value=False)
            
            min_price = st.number_input("최소 주가 ($)", min_value=0.0, value=10.0, step=1.0)
            min_adv_m = st.number_input("최소 50일 평균 거래대금 ($M)", min_value=0.0, value=2.0, step=0.5)
            rs_min = st.slider("최소 RS 점수", 1, 99, 80)
            ind_rs_min = st.slider("최소 산업군 RS", 1, 99, 50)

            # SMR 등급 (사용자 원본 로직)
            if "smr_sel" not in st.session_state: st.session_state.smr_sel = ["A", "B"]
            st.caption("SMR 등급")
            smr_cols1 = st.columns(3)
            for i, g in enumerate(["A", "B", "C"]):
                with smr_cols1[i]:
                    sel = g in st.session_state.smr_sel
                    if st.button(f"{'●' if sel else '○'} {g}", key=f"smr_{g}", type="primary" if sel else "secondary"):
                        if g in st.session_state.smr_sel: st.session_state.smr_sel.remove(g)
                        else: st.session_state.smr_sel = sorted(st.session_state.smr_sel + [g])
                        st.rerun()
            smr_cols2 = st.columns(3)
            for i, g in enumerate(["D", "E", "전체"]):
                with smr_cols2[i]:
                    sel = g in st.session_state.smr_sel if g != "전체" else set(st.session_state.smr_sel) == {"A","B","C","D","E"}
                    if st.button(f"{'●' if sel else '○'} {g}", key=f"smr_{g}", type="primary" if sel else "secondary"):
                        if g == "전체": st.session_state.smr_sel = ["A","B","C","D","E"] if len(st.session_state.smr_sel) < 5 else []
                        else:
                            if g in st.session_state.smr_sel: st.session_state.smr_sel.remove(g)
                            else: st.session_state.smr_sel = sorted(st.session_state.smr_sel + [g])
                        st.rerun()

            st.divider()
            # 수급(AD) 등급 (사용자 원본 로직)
            if "ad_sel" not in st.session_state: st.session_state.ad_sel = ["A", "B", "C"]
            st.caption("수급(AD) 등급")
            ad_cols1 = st.columns(3)
            for i, g in enumerate(["A", "B", "C"]):
                with ad_cols1[i]:
                    sel = g in st.session_state.ad_sel
                    if st.button(f"{'●' if sel else '○'} {g}", key=f"ad_{g}", type="primary" if sel else "secondary"):
                        if g in st.session_state.ad_sel: st.session_state.ad_sel.remove(g)
                        else: st.session_state.ad_sel = sorted(st.session_state.ad_sel + [g])
                        st.rerun()
            ad_cols2 = st.columns(3)
            for i, g in enumerate(["D", "E", "전체"]):
                with ad_cols2[i]:
                    sel = g in st.session_state.ad_sel if g != "전체" else set(st.session_state.ad_sel) == {"A","B","C","D","E"}
                    if st.button(f"{'●' if sel else '○'} {g}", key=f"ad_{g}", type="primary" if sel else "secondary"):
                        if g == "전체": st.session_state.ad_sel = ["A","B","C","D","E"] if len(st.session_state.ad_sel) < 5 else []
                        else:
                            if g in st.session_state.ad_sel: st.session_state.ad_sel.remove(g)
                            else: st.session_state.ad_sel = sorted(st.session_state.ad_sel + [g])
                        st.rerun()

        st.divider()
        with st.expander("🏢 산업군 필터"):
            all_ind = sorted(df['industry'].unique())
            if "industry_sel" not in st.session_state: st.session_state.industry_sel = [s for s in all_ind if s != 'Unknown']
            all_sel = set(st.session_state.industry_sel) == set(all_ind)
            if st.button(f"{'●' if all_sel else '○'} 전체", key="ind_all", type="primary" if all_sel else "secondary"):
                st.session_state.industry_sel = list(all_ind) if not all_sel else []
                st.rerun()
            for s in all_ind:
                sel = s in st.session_state.industry_sel
                if st.button(f"{'●' if sel else '○'} {s}", key=f"ind_{s}", type="primary" if sel else "secondary", use_container_width=True):
                    if sel: st.session_state.industry_sel.remove(s)
                    else: st.session_state.industry_sel.append(s)
                    st.rerun()

        # 필터 마스크 적용
        mask = (df['price'] >= min_price) & \
               (df['adv_50'] >= min_adv_m * 1_000_000) & \
               (df['rs_score'] >= rs_min) & \
               (df['industry_rs_score'] >= ind_rs_min) & \
               (df['smr_grade'].isin(st.session_state.smr_sel)) & \
               (df['ad_grade'].isin(st.session_state.ad_sel)) & \
               (df['industry'].isin(st.session_state.industry_sel))
        
        # [추가] 즐겨찾기 필터 적용
        if show_only_favs:
            mask = mask & (df['symbol'].isin(get_favorites()))
        
        f_df = df[mask].sort_values('rs_score', ascending=False)

    col_l, col_r = st.columns([4, 3])
    with col_l:
        st.subheader(f"Leaders ({len(f_df)})")
        display_list = f_df.copy()
        display_list['ADV($M)'] = (display_list['adv_50'] / 1_000_000).round(1)
        
        # [추가] 목록에 즐겨찾기 별표 표시
        fav_list = get_favorites()
        display_list['Ticker'] = display_list['symbol'].apply(lambda x: f"⭐ {x}" if x in fav_list else x)
        
        sel = st.dataframe(
            display_list[['Ticker', 'price', 'ADV($M)', 'rs_score', 'smr_grade', 'ad_grade', 'industry_rs_score', 'industry']],
            hide_index=True, on_select="rerun", selection_mode="single-row", height=850, use_container_width=True,
            column_config={
                "industry": st.column_config.TextColumn("Industry", width=360),
                "Ticker": st.column_config.TextColumn("Ticker", width="small"),
                "price": st.column_config.NumberColumn("Price", width="small"),
                "ADV($M)": st.column_config.NumberColumn("ADV($M)", width="small"),
                "rs_score": st.column_config.NumberColumn("RS", width="small"),
                "smr_grade": st.column_config.TextColumn("SMR", width="small"),
                "ad_grade": st.column_config.TextColumn("AD", width="small"),
                "industry_rs_score": st.column_config.NumberColumn("Ind RS", width="small"),
            }
        )

    with col_r:
        if len(sel.selection.rows) > 0:
            row = f_df.iloc[sel.selection.rows[0]]
            ticker = row['symbol']

            # [추가] 종목 상세 헤더 및 즐겨찾기 버튼
            fav_list = get_favorites()
            is_fav = ticker in fav_list
            c_header, c_fav = st.columns([4, 1])
            with c_header:
                st.markdown(f"### {ticker}")
            with c_fav:
                if st.button("⭐ 삭제" if is_fav else "☆ 저장", use_container_width=True):
                    toggle_favorite(ticker)
                    st.rerun()

            st.markdown(f"**Stock RS** {row['rs_score']} · **SMR** {row['smr_grade']} · **AD** {row['ad_grade']} · **Ind RS** {row['industry_rs_score']} · {row['industry']}")

            try:
                with st.spinner(f"'{ticker}' 상세 데이터를 불러오는 중..."):
                    q_inc, a_inc, q_bal, a_bal, info = get_detailed_info(ticker)
                
                t_chart, t_fin, t_check, t_biz = st.tabs(["📊 차트", "🧾 재무제표", "🛡️ 체크리스트", "🏢 개요"])

                with t_chart:
                    if "tv_embed_url" not in st.session_state: st.session_state.tv_embed_url = ""
                    with st.expander("📌 내 트레이딩뷰 차트 사용하기", expanded=False):
                        tv_url = st.text_input("TradingView 임베드 URL", value=st.session_state.tv_embed_url)
                        if tv_url != st.session_state.tv_embed_url:
                            st.session_state.tv_embed_url = tv_url
                            st.rerun()
                    
                    if st.session_state.tv_embed_url:
                        embed_url = st.session_state.tv_embed_url.strip().replace("SYMBOL", ticker).replace("{{ticker}}", ticker)
                        components.html(f'<iframe src="{embed_url}" height="710" style="width:100%; border:0;"></iframe>', height=715)
                    else:
                        components.html(f"""
                        <script type="text/javascript" src="https://s3.tradingview.com/tv.js"></script>
                        <div id="tv_chart" style="height: 710px;"></div>
                        <script type="text/javascript">
                        new TradingView.widget({{"autosize":true,"symbol":"{ticker}","interval":"D","theme":"dark","style":"1","locale":"kr","container_id":"tv_chart"}});
                        </script>
                        """, height=710)

                    st.markdown("#### 📈 RS & Industry RS 추세")
                    rs_hist_df = get_rs_history(ticker)
                    if not rs_hist_df.empty and len(rs_hist_df) > 1:
                        rs_hist_df['date'] = pd.to_datetime(rs_hist_df['date'])
                        val_vars = [c for c in ['rs_score', 'industry_rs_score'] if c in rs_hist_df.columns]
                        plot_df = rs_hist_df.melt(id_vars='date', value_vars=val_vars, var_name='Type', value_name='Score')
                        plot_df['Type'] = plot_df['Type'].map({'rs_score': 'Stock RS', 'industry_rs_score': 'Industry RS'})
                        
                        chart = alt.Chart(plot_df).mark_line(strokeWidth=2).encode(
                            x=alt.X('date:T', title='날짜'),
                            y=alt.Y('Score:Q', title='RS 점수', scale=alt.Scale(domain=[0, 100])),
                            color=alt.Color('Type:N', scale=alt.Scale(domain=['Stock RS', 'Industry RS'], range=['#64ffda', '#ff7676']))
                        ).properties(height=320)
                        st.altair_chart(chart, use_container_width=True)

                with t_fin:
                    q_rev = q_inc.loc['Total Revenue'] if 'Total Revenue' in q_inc.index else pd.Series()
                    q_op = q_inc.loc['Operating Income'] if 'Operating Income' in q_inc.index else pd.Series()
                    q_eps = q_inc.loc['Basic EPS'] if 'Basic EPS' in q_inc.index else pd.Series()
                    a_rev = a_inc.loc['Total Revenue'] if 'Total Revenue' in a_inc.index else pd.Series()
                    a_op = a_inc.loc['Operating Income'] if 'Operating Income' in a_inc.index else pd.Series()
                    a_eps = a_inc.loc['Basic EPS'] if 'Basic EPS' in a_inc.index else pd.Series()

                    st.markdown("#### 📈 분기 성장률 (QoQ %)")
                    qoq_df = pd.DataFrame({'분기': format_date_idx(q_rev.index, 'Q'), '매출 성장(%)': calc_growth(q_rev, 1), '영업이익 성장(%)': calc_growth(q_op, 1), 'EPS 성장(%)': calc_growth(q_eps, 1)}).set_index('분기').head(4)
                    st.dataframe(qoq_df.style.format("{:.1f}"), use_container_width=True)

                    st.markdown("#### 📅 연간 성장률 (YoY %)")
                    yoy_df = pd.DataFrame({'연도': format_date_idx(a_rev.index, 'A'), '매출 성장(%)': calc_growth(a_rev, 1), '영업이익 성장(%)': calc_growth(a_op, 1), 'EPS 성장(%)': calc_growth(a_eps, 1)}).set_index('연도').head(4)
                    st.dataframe(yoy_df.style.format("{:.1f}"), use_container_width=True)

                    def format_fin_df(df_in, date_type='Q'):
                        if df_in.empty: return pd.DataFrame()
                        target = df_in.reindex(list(FIN_MAP.keys())).dropna(how='all')
                        target.index = [FIN_MAP.get(i, i) for i in target.index]
                        target.columns = format_date_idx(target.columns, date_type)
                        disp = target.copy()
                        for idx in disp.index:
                            if "EPS" not in str(idx): disp.loc[idx] = disp.loc[idx] / 1000
                        eps_rows = [i for i in disp.index if "EPS" in str(i)]
                        return disp.style.format(precision=0, thousands=",").format(precision=2, subset=pd.IndexSlice[eps_rows, :])

                    st.markdown("#### 🧾 상세 재무 데이터 ($1,000)")
                    st.write("**연간 상세**")
                    st.dataframe(format_fin_df(pd.concat([a_inc, a_bal]) if not a_inc.empty else pd.DataFrame(), 'A'), use_container_width=True)
                    st.write("**분기 상세**")
                    st.dataframe(format_fin_df(pd.concat([q_inc, q_bal]) if not q_inc.empty else pd.DataFrame(), 'Q'), use_container_width=True)

                with t_check:
                    st.subheader("🛡️ 주도주 판별 시스템")
                    
                    # --- [계산] 기술적 지표 (미너비니 템플릿용) ---
                    hist = yf.Ticker(ticker).history(period="1y")
                    if not hist.empty:
                        last_price = hist['Close'].iloc[-1]
                        ma50 = hist['Close'].rolling(50).mean().iloc[-1]
                        ma150 = hist['Close'].rolling(150).mean().iloc[-1]
                        ma200 = hist['Close'].rolling(200).mean().iloc[-1]
                        ma200_prev = hist['Close'].rolling(200).mean().iloc[-20] if len(hist) > 20 else ma200
                        low_52w = hist['Close'].min()
                        high_52w = hist['Close'].max()
                        
                        # 성장률 및 재무 데이터 재계산 (탭 상단에 정의된 변수 활용)
                        cur_q_eps_growth = calc_growth(q_eps, 4).iloc[0] if len(q_eps) >= 5 else 0
                        ann_eps_growth = calc_growth(a_eps, 1).iloc[0] if len(a_eps) >= 2 else 0
                        roe = info.get('returnOnEquity', 0) * 100
                        inst_own = info.get('heldPercentInstitutions', 0) * 100

                        c1, c2 = st.columns(2)
                        
                        with c1:
                            st.markdown("### 🟢 CANSLIM (오닐)")
                            st.checkbox(f"**C**: 분기 EPS 25%↑ ({cur_q_eps_growth:.1f}%)", value=cur_q_eps_growth >= 25)
                            st.checkbox(f"**A**: 연간 EPS 25%↑ 또는 ROE 17%↑ ({ann_eps_growth:.1f}% / {roe:.1f}%)", value=ann_eps_growth >= 25 or roe >= 17)
                            st.checkbox(f"**N**: 52주 고가 근접 (현재가 > 고가 90%)", value=last_price >= (high_52w * 0.9))
                            st.checkbox(f"**S**: 수급 양호 (AD 등급: {row['ad_grade']})", value=row['ad_grade'] in ['A', 'B'])
                            st.checkbox(f"**L**: 시장 주도주 (RS 80↑: {row['rs_score']})", value=row['rs_score'] >= 80)
                            st.checkbox(f"**I**: 기관 관심 (보유 비중: {inst_own:.1f}%)", value=inst_own > 30)
                            st.info("💡 **M(Market)**: 현재 지수의 추세를 확인하세요.")

                        with c2:
                            st.markdown("### 🔵 트렌드 템플릿 (미너비니)")
                            st.checkbox("1. 주가 > 150MA & 200MA", value=last_price > ma150 and last_price > ma200)
                            st.checkbox("2. 150MA > 200MA", value=ma150 > ma200)
                            st.checkbox("3. 200MA 상승세 (1개월 전 대비)", value=ma200 > ma200_prev)
                            st.checkbox("4. 50MA > 150MA & 200MA", value=ma50 > ma150 and ma50 > ma200)
                            st.checkbox("5. 주가 > 50MA", value=last_price > ma50)
                            st.checkbox(f"6. 저가 대비 30%↑ (현재: {((last_price/low_52w)-1)*100:.1f}%)", value=last_price >= low_52w * 1.3)
                            st.checkbox(f"7. 고가 대비 25% 이내 (현재: -{((high_52w/last_price)-1)*100:.1f}%)", value=last_price >= high_52w * 0.75)
                            st.checkbox(f"8. RS 점수 70 이상", value=row['rs_score'] >= 70)
                    else:
                        st.warning("분석을 위한 충분한 가격 데이터가 없습니다.")

                with t_biz:
                    st.subheader("🏢 개요")
                    summary_ko = translate_to_korean(info.get('longBusinessSummary', 'N/A'))
                    st.markdown(f'<div class="overview-panel"><h2>{info.get("longName", ticker)}</h2><p>{summary_ko}</p></div>', unsafe_allow_html=True)

            except YFDataFetchError:
                st.error("🚨 야후 파이낸스 요청이 차단되었습니다. 잠시 후 시도해 주세요.")
        else:
            st.info("👈 왼쪽 리스트에서 종목을 선택해 주세요.")
