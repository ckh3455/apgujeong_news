# -*- coding: utf-8 -*-
"""
압구정 뉴스 자동 수집기
시트 구조: [일시, 뉴스제목, 출처(URL)]
"""

import os, re, html
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
from urllib.parse import quote_plus
import feedparser
import gspread
from google.oauth2.service_account import Credentials

SPREADSHEET_ID = os.getenv("SPREADSHEET_ID")
SHEET_NAME     = os.getenv("SHEET_NAME", "압구정_뉴스")
SA_PATH        = "service_account.json"
HEADERS        = ["일시", "뉴스제목", "출처"]
DEDUP_LIMIT    = 2000

KEYWORDS = [
    "압구정","부동산","재건축","부동산 세금","보유세",
    "부동산정책","부동산규제","대출규제","대출정책",
    "가계부채","기준금리","전세대출","주담대","규제지역"
]

def rss_urls():
    urls = []
    gnews = "https://news.google.com/rss/search?q={q}&hl=ko&gl=KR&ceid=KR:ko"
    for k in KEYWORDS:
        urls.append(gnews.format(q=quote_plus(k)))
    urls += [
        gnews.format(q=quote_plus("site:news.naver.com 부동산 OR 재건축 OR 압구정")),
        gnews.format(q=quote_plus("site:news.daum.net 부동산 OR 재건축 OR 압구정")),
        "https://www.mk.co.kr/rss/50300009/",
        "https://www.hankyung.com/feed/realestate",
    ]
    return urls

def auth_sheet():
    creds = Credentials.from_service_account_file(
        SA_PATH, scopes=["https://www.googleapis.com/auth/spreadsheets"]
    )
    gc = gspread.authorize(creds)
    sh = gc.open_by_key(SPREADSHEET_ID)
    try:
        ws = sh.worksheet(SHEET_NAME)
    except gspread.exceptions.WorksheetNotFound:
        ws = sh.add_worksheet(title=SHEET_NAME, rows=2000, cols=3)

    ws.resize(cols=3)
    ws.update("A1:C1", [HEADERS])
    ws.freeze(rows=1)
    ws.format("A:C", {"backgroundColor": {"red": 1, "green": 1, "blue": 1}})
    return ws

def to_kst(entry):
    kst = ZoneInfo("Asia/Seoul")
    try:
        if getattr(entry, "published_parsed", None):
            dt = datetime(*entry.published_parsed[:6], tzinfo=timezone.utc).astimezone(kst)
        else:
            dt = datetime.now(kst)
    except Exception:
        dt = datetime.now(kst)
    return dt.strftime("%Y-%m-%d %H:%M")

def get_existing(ws):
    titles = ws.col_values(2)[1:]
    links  = ws.col_values(3)[1:]
    return set(titles[-DEDUP_LIMIT:]), set(links[-DEDUP_LIMIT:])

def clean(text: str):
    """HTML 엔티티와 여백 제거"""
    return html.unescape(re.sub(r"\s+", " ", text.strip()))

def collect():
    if not SPREADSHEET_ID:
        raise RuntimeError("SPREADSHEET_ID missing")

    ws = auth_sheet()
    exist_titles, exist_links = get_existing(ws)
    rows, seen = [], set()

    for url in rss_urls():
        feed = feedparser.parse(url)
        for e in feed.entries:
            title = clean(getattr(e, "title", ""))
            link  = clean(getattr(e, "link", ""))
            if not title or not link: 
                continue
            if title in exist_titles or link in exist_links or link in seen:
                continue
            rows.append([to_kst(e), title, link])
            seen.add(link)
    if rows:
        ws.append_rows(rows, value_input_option="RAW")
        ws.sort((1, "asc"))
        ws.format("A:C", {"backgroundColor": {"red": 1, "green": 1, "blue": 1}})
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] inserted={len(rows)}")

if __name__ == "__main__":
    collect()
