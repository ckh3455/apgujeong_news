# -*- coding: utf-8 -*-
"""
원부동산 매물장 / '압구정_뉴스' 탭에 적재:
[일시, 뉴스제목, 요약, 출처, 키워드]

- 소스:
  * Google News 검색 RSS (키워드별)
  * Google News + site:news.naver.com / site:news.daum.net 필터
  * 매일경제(부동산) 공식 RSS
  * 한국경제(부동산) 공식 RSS
- 중복 방지: 최근 1000개 '뉴스제목' + 링크 기준
- 일시: KST(Asia/Seoul)로 YYYY-MM-DD HH:MM
"""

import os, re
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
from urllib.parse import quote_plus

import feedparser
import gspread
from google.oauth2.service_account import Credentials

SPREADSHEET_ID = os.getenv("SPREADSHEET_ID")            # 스프레드시트 ID
SHEET_NAME     = os.getenv("SHEET_NAME", "압구정_뉴스")   # 탭 이름
SA_PATH        = "service_account.json"

HEADERS = ["일시", "뉴스제목", "요약", "출처", "키워드"]

KEYWORDS = [
    "압구정","부동산","재건축","부동산 세금","보유세",
    "부동산정책","부동산규제","대출규제","대출정책",
    "가계부채","기준금리","전세대출","주담대","규제지역"
]

def rss_urls():
    urls = []

    # ① Google News (키워드별)
    gnews_base = "https://news.google.com/rss/search?q={q}&hl=ko&gl=KR&ceid=KR:ko"
    for k in KEYWORDS:
        urls.append((f"GoogleNews:{k}", gnews_base.format(q=quote_plus(k))))

    # ② Google News (네이버/다음 도메인 제한)
    site_queries = [
        ("NaverNews", "site:news.naver.com (압구정 OR 재건축 OR 부동산 OR 규제 OR 주담대)"),
        ("DaumNews",  "site:news.daum.net  (압구정 OR 재건축 OR 부동산 OR 규제 OR 주담대)"),
    ]
    for name, q in site_queries:
        urls.append((name, gnews_base.format(q=quote_plus(q))))

    # ③ 매일경제(부동산) 공식 RSS
    urls.append(("매일경제 부동산", "https://www.mk.co.kr/rss/50300009/"))

    # ④ 한국경제(부동산) 공식 RSS
    urls.append(("한국경제 부동산", "https://www.hankyung.com/feed/realestate"))

    return urls

def auth_sheet():
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    creds = Credentials.from_service_account_file(SA_PATH, scopes=scopes)
    gc = gspread.authorize(creds)
    sh = gc.open_by_key(SPREADSHEET_ID)
    try:
        ws = sh.worksheet(SHEET_NAME)
    except gspread.exceptions.WorksheetNotFound:
        ws = sh.add_worksheet(title=SHEET_NAME, rows=2000, cols=10)

    # 헤더 보정
    cur = ws.get_values("A1:E1")
    if not cur or cur[0] != HEADERS:
        ws.update("A1:E1", [HEADERS])
    return ws

def get_existing_titles(ws, limit=1000):
    """
    gspread Worksheet에는 last_row가 없습니다.
    2열(뉴스제목) 전체를 읽어 뒤쪽 limit개만 사용해 중복 방지합니다.
    """
    try:
        titles = ws.col_values(2)  # B열 전체 (헤더 포함)
    except Exception:
        return set()
    # 헤더 제거 후 뒤에서 limit개만
    titles = [t.strip() for t in titles[1:] if t]  # 0번은 헤더 '뉴스제목'
    if not titles:
        return set()
    return set(titles[-limit:])

def strip_html(s: str) -> str:
    if not s:
        return ""
    return re.sub(r"<[^>]+>", "", s).strip()

def to_kst(entry) -> str:
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
        raise RuntimeError("SPREADSHEET_ID env missing")

    ws = auth_sheet()
    existing_titles = get_existing_titles(ws)

    rows, seen_links = [], set()  # [일시, 뉴스제목, 요약, 출처, 키워드]

    for tag, url in rss_urls():
        feed = feedparser.parse(url)
        for e in feed.entries:
            title = (getattr(e, "title", "") or "").strip()
            link  = (getattr(e, "link", "")  or "").strip()
            if not title:
                continue
            # 최근 제목/링크 기준 중복 방지
            if title in existing_titles or (link and link in seen_links):
                continue

            summary = strip_html(getattr(e, "summary", ""))
            ts_kst  = to_kst(e)
            rows.append([ts_kst, title, summary, link, tag])

            existing_titles.add(title)
            if link:
                seen_links.add(link)

    if rows:
        ws.append_rows(rows, value_input_option="RAW")
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] inserted={len(rows)}")

if __name__ == "__main__":
    collect()
