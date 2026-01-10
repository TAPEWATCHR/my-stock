def get_master_list():
    """기업 이름이 아닌 '티커 심볼'만 정확히 추출하도록 보정"""
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"}
    
    print("Step 1-1: 주요 종목 섹터 정보 확보 중...")
    sp_data = pd.DataFrame(columns=['symbol', 'sector'])
    try:
        url_sp = 'https://en.wikipedia.org/wiki/List_of_S%26P_500_companies'
        resp = requests.get(url_sp, headers=headers, timeout=10)
        sp500 = pd.read_html(StringIO(resp.text))[0]
        # 첫 번째 컬럼(Symbol)과 네 번째 컬럼(GICS Sector)을 정확히 지정
        sp_data = pd.DataFrame({
            'symbol': sp500.iloc[:, 0].astype(str).str.strip(), 
            'sector': sp500.iloc[:, 3].astype(str).str.strip()
        })
    except Exception as e:
        print(f"S&P 500 섹터 확보 실패: {e}")

    print("Step 1-2: 미국 시장 전체 티커 확보 중...")
    all_tickers = []
    
    # 나스닥 공식 FTP 데이터 시도
    try:
        url_all = "https://tda.nasdaqtrader.com/dynamic/SymDir/nasdaqlisted.txt"
        resp = requests.get(url_all, timeout=15)
        if resp.status_code == 200:
            all_df = pd.read_csv(StringIO(resp.text), sep='|')
            # 'Symbol' 컬럼만 추출 (마지막 줄 제외)
            all_tickers = all_df['Symbol'].dropna().astype(str).tolist()
            # 파일 끝의 파일 정보 텍스트 제거
            all_tickers = [s for s in all_tickers if "File Creation Time" not in s]
    except:
        print("나스닥 서버 연결 실패. 위키피디아 백업 모드 가동...")
        all_tickers = sp_data['symbol'].tolist()
        try:
            url_ndx = 'https://en.wikipedia.org/wiki/Nasdaq-100'
            ndx_resp = requests.get(url_ndx, headers=headers, timeout=10)
            ndx_tables = pd.read_html(StringIO(ndx_resp.text))
            # 보통 3~4번째 테이블에 구성 종목이 있음
            ndx_df = ndx_tables[4] if len(ndx_tables) > 4 else ndx_tables[3]
            # 티커 컬럼명은 'Ticker' 또는 'Symbol'
            col = 'Ticker' if 'Ticker' in ndx_df.columns else ndx_df.columns[1]
            all_tickers.extend(ndx_df[col].astype(str).tolist())
        except: pass

    # 데이터 정제 (특수문자 처리 및 중복 제거)
    clean_tickers = []
    for s in all_tickers:
        s = str(s).strip().upper()
        if not s or s == 'SYMBOL' or ' ' in s: continue # 기업 이름이 들어가는 것 방지
        s = s.replace('.', '-') # BRK.B -> BRK-B
        clean_tickers.append(s)

    all_data = pd.DataFrame({'symbol': list(set(clean_tickers))})
    master = pd.merge(all_data, sp_data, on='symbol', how='left')
    master['sector'] = master['sector'].fillna('US Market')
    
    print(f"최종 확보된 고유 티커 수: {len(master)}")
    return master.drop_duplicates('symbol')
