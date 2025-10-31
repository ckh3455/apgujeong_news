# -*- coding: utf-8 -*-
import os, re
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
from urllib.parse import quote_plus
import feedparser
import gspread
from google.oauth2.service_account import Credentials

SPREADSHEET_ID = os.getenv("SPREADSHEET_ID")            # 필수
SHEET_NAME     = os.getenv("SHEET_NAME", "압구정_뉴스")   # 탭명

# 시트 헤더: 일시 | 뉴스제목 | 요약 | 출처 | 키워드
HEADERS = ["일시", "뉴스제목", "요약", "출처", "키워드"]

# ✅ 사용자 지정 키워드
KEYWORDS = [
    "압구정","부동산","재건축","부동산 세금","보유세",
    "부동산정책","부동산규제","대출규제","대출정책",
    "가계부채","기준금리","전세대출","주담대","규제지역"
]

def rss_urls():
    base = "https://news.google.com/rss/search?q={q}&hl=ko&gl=KR&ceid=KR:ko"
    return [(k, base.format(q=quote_plus(k))) for k in KEYWORDS]

def auth_sheet():
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    creds = Credentials.from_service_account_file("service_account.json", scopes=scopes)
    gc = gspread.authorize(creds)
    sh = gc.open_by_key(SPREADSHEET_ID)
    try:
        ws = sh.worksheet(SHEET_NAME)
    except gspread.exceptions.WorksheetNotFound:
        ws = sh.add_worksheet(title=SHEET_NAME, rows=2000, cols=10)
    # 헤더 보정
    first = ws.get_values("A1:E1")
    if not first or first[0] != HEADERS:
        ws.update("A1:E1", [HEADERS])
    return ws

def get_existing_titles(ws, limit=1000):
    last = ws.last_row
    if last < 2:
        return set()
    start_row = max(2, last - limit + 1)
    vals = ws.get_values(f"B{start_row}:B{last}")  # 2열=뉴스제목
    return set(v[0].strip() for v in vals if v and v[0])

def strip_html(s: str) -> str:
    if not s: return ""
    return re.sub(r"<[^>]+>", "", s).strip()

def to_kst_from_entry(entry) -> str:
    kst = ZoneInfo("Asia/Seoul")
    try:
        if getattr(entry, "published_parsed", None):
            dt = datetime(*entry.published_parsed[:6], tzinfo=timezone.utc).astimezone(kst)
        else:
            dt = datetime.now(kst)
    except Exception:
        dt = datetime.now(kst)
    return dt.strftime("%Y-%m-%d %H:%M")

def collect():
    if not SPREADSHEET_ID:
        raise RuntimeError("Missing SPREADSHEET_ID env")
    ws = auth_sheet()
    existing_titles = get_existing_titles(ws)

    rows = []  # [일시, 뉴스제목, 요약, 출처, 키워드]
    seen_links = set()

    for keyword, url in rss_urls():
        feed = feedparser.parse(url)
        for e in feed.entries:
            title = getattr(e, "title", "").strip()
            link = getattr(e, "link", "").strip()
            if not title or title in existing_titles or link in seen_links:
                continue
            summary = strip_html(getattr(e, "summary", ""))
            ts_kst = to_kst_from_entry(e)

            rows.append([ts_kst, title, summary, link, keyword])
            existing_titles.add(title)
            seen_links.add(link)

    if rows:
        ws.append_rows(rows, value_input_option="RAW")
    print(f"inserted={len(rows)}")

if __name__ == "__main__":
    collect()
