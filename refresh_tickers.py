import pandas as pd
from io import StringIO
from ftplib import FTP

def get_nasdaq_ftp_data(filename):
    try:
        ftp = FTP('ftp.nasdaqtrader.com')
        ftp.login()
        ftp.cwd('symboldirectory')
        
        lines = []
        ftp.retrlines(f'RETR {filename}', lines.append)
        ftp.quit()
        
        data = "\n".join(lines)
        # 나스닥 파일은 구분자가 '|' 입니다.
        df = pd.read_csv(StringIO(data), sep='|')
        
        # 마지막 줄(파일 생성 정보) 제거 및 유효한 심볼만 추출
        if 'Symbol' in df.columns:
            return df['Symbol'].dropna().astype(str).tolist()
        elif 'ACT Symbol' in df.columns: # otherlisted.txt는 컬럼명이 다를 수 있음
            return df['ACT Symbol'].dropna().astype(str).tolist()
        else:
            return df.iloc[:, 0].dropna().astype(str).tolist()
    except Exception as e:
        print(f"FTP {filename} 수집 실패: {e}")
        return []

def refresh_ticker_list():
    all_symbols = set()

    print("미국 전체 시장 티커 수집 시작 (NASDAQ, NYSE, AMEX 등)...")
    
    # 1. 나스닥 상장 종목
    nasdaq_list = get_nasdaq_ftp_data('nasdaqlisted.txt')
    all_symbols.update(nasdaq_list)
    print(f"- 나스닥 종목 수집 완료: {len(nasdaq_list)}개")

    # 2. 기타 거래소(NYSE 등) 종목
    other_list = get_nasdaq_ftp_data('otherlisted.txt')
    all_symbols.update(other_list)
    print(f"- 기타 거래소(NYSE 등) 종목 수집 완료: {len(other_list)}개")

    # 3. 데이터 정제 (기존 로직 유지)
    clean_tickers = sorted([
        s.strip().upper() for s in all_symbols 
        if s.strip().isalpha() and 1 <= len(s.strip()) <= 5
        and s.strip() not in ['SYMBOL', 'FILE'] # 헤더/푸터 방어 코드
    ])

    if len(clean_tickers) > 1000:
        with open('tickers.txt', 'w') as f:
            for ticker in clean_tickers:
                f.write(f"{ticker}\n")
        print(f"✅ 전체 업데이트 완료: 총 {len(clean_tickers)}개 종목이 'tickers.txt'에 저장되었습니다.")
        return True
    
    return False

if __name__ == "__main__":
    refresh_ticker_list()
