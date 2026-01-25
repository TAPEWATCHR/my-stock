import pandas as pd
from io import StringIO
import os
from ftplib import FTP

def get_nasdaq_ftp_data(filename):
    """나스닥 FTP에서 파일을 다운로드하여 리스트로 반환"""
    try:
        ftp = FTP('ftp.nasdaqtrader.com')
        ftp.login()  # 익명 로그인
        ftp.cwd('symboldirectory')
        
        lines = []
        ftp.retrlines(f'RETR {filename}', lines.append)
        ftp.quit()
        
        # 데이터 정제 (첫 줄은 헤더, 마지막 줄은 파일 정보이므로 제외 가능성 고려)
        data = "\n".join(lines)
        df = pd.read_csv(StringIO(data), sep='|')
        return df.iloc[:, 0].dropna().astype(str).tolist()
    except Exception as e:
        print(f"FTP 접속 실패 ({filename}): {e}")
        return []

def refresh_ticker_list():
    new_tickers = set()

    print("최신 티커 리스트 수집 시작 (FTP 방식)...")
    
    # 1. 나스닥 및 기타 거래소 데이터 가져오기
    new_tickers.update(get_nasdaq_ftp_data('nasdaqlisted.txt'))
    new_tickers.update(get_nasdaq_ftp_data('otherlisted.txt'))

    # 2. 백업 소스 (기존 로직 유지)
    if len(new_tickers) < 3000:
        print("데이터가 부족하여 백업 소스 사용 시도...")
        try:
            headers = {"User-Agent": "Mozilla/5.0"}
            url_backup = "https://raw.githubusercontent.com/rreichel3/US-Stock-Symbols/main/all_tickers.txt"
            import requests
            res = requests.get(url_backup, headers=headers, timeout=10)
            new_tickers.update([s.strip().upper() for s in res.text.split('\n') if s.strip()])
        except Exception as e:
            print(f"백업 소스 접속 실패: {e}")

    # 3. 정제 (1-5자 영문, 불필요한 값 제거)
    # 나스닥 데이터에는 'File Creation Time' 같은 메타데이터가 포함될 수 있어 필터링이 중요합니다.
    clean_tickers = sorted([
        s.strip().upper() for s in new_tickers 
        if s.strip().isalpha() and 1 <= len(s.strip()) <= 5
    ])

    if len(clean_tickers) > 1000:
        with open('tickers.txt', 'w') as f:
            for ticker in clean_tickers:
                f.write(f"{ticker}\n")
        print(f"티커 리스트 업데이트 완료: 총 {len(clean_tickers)}개 종목")
        return True
    
    print("업데이트 실패: 유효한 티커를 충분히 확보하지 못했습니다.")
    return False

if __name__ == "__main__":
    refresh_ticker_list()
