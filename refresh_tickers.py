import requests
import pandas as pd
from io import StringIO
import os

def refresh_ticker_list():
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36"}
    new_tickers = set()

    # 소스 1: 나스닥 공식 리스트 (차단 대비 우회 시도)
    urls = [
        "https://tda.nasdaqtrader.com/dynamic/SymDir/nasdaqlisted.txt",
        "https://tda.nasdaqtrader.com/dynamic/SymDir/otherlisted.txt"
    ]
    
    print("최신 티커 리스트 수집 시작...")
    for url in urls:
        try:
            res = requests.get(url, headers=headers, timeout=15)
            if res.status_code == 200:
                df = pd.read_csv(StringIO(res.text), sep='|')
                symbols = df.iloc[:, 0].dropna().astype(str).tolist()
                new_tickers.update(symbols)
        except:
            print(f"{url} 접속 실패, 다음 소스로 이동.")

    # 소스 2: 오픈 데이터 리포지토리 (백업)
    if len(new_tickers) < 3000:
        try:
            url_backup = "https://raw.githubusercontent.com/rreichel3/US-Stock-Symbols/main/all_tickers.txt"
            res = requests.get(url_backup, headers=headers)
            new_tickers.update([s.strip().upper() for s in res.text.split('\n') if s.strip()])
        except: pass

    # 정제 (1-5자 영문, 특수기호 제외)
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
    return False

if __name__ == "__main__":
    refresh_ticker_list()
