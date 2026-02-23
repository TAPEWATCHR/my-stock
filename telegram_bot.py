import os
import sqlite3
import pandas as pd
import requests

def send_telegram_message(token, chat_id, text):
    """텔레그램 봇을 통해 메시지를 전송합니다."""
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = {"chat_id": chat_id, "text": text, "parse_mode": "Markdown"}
    requests.post(url, json=payload)

def main():
    # 1. GitHub Secrets에서 토큰과 Chat ID 가져오기
    TELEGRAM_TOKEN = os.environ.get('TELEGRAM_TOKEN')
    CHAT_ID = os.environ.get('CHAT_ID')

    if not TELEGRAM_TOKEN or not CHAT_ID:
        print("텔레그램 토큰이나 챗 ID가 설정되지 않았습니다.")
        return

    # 2. DB에서 데이터 불러오기
    if not os.path.exists('ibd_system.db'):
        print("DB 파일이 없습니다.")
        return
        
    conn = sqlite3.connect('ibd_system.db')
    df = pd.read_sql("SELECT * FROM repo_results", conn)
    conn.close()

    # 3. 알림을 보낼 조건 설정 (예: RS 90 이상, SMR A등급)
    mask = (df['rs_score'] >= 90) & (df['smr_grade'] == 'A')
    top_stocks = df[mask].sort_values('rs_score', ascending=False).head(5)

    if top_stocks.empty:
        print("오늘 조건에 맞는 주도주가 없습니다.")
        return

    # 4. 메시지 조립 및 전송
    message = "🚀 **오늘의 SMR 'A' & RS 90 이상 주도주** 🚀\n\n"
    for _, row in top_stocks.iterrows():
        message += f"🔹 **{row['symbol']}** (${row['price']})\n"
        message += f" - RS: {row['rs_score']} | 산업군 RS: {row['industry_rs_score']}\n"
        message += f" - 수급(AD): {row['ad_rating']}\n\n"
    
    message += "자세한 차트는 터미널에서 확인하세요!"
    
    send_telegram_message(TELEGRAM_TOKEN, CHAT_ID, message)
    print("텔레그램 알림 전송 완료!")

if __name__ == "__main__":
    main()
