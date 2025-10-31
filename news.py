# -*- coding: utf-8 -*-
"""
원부동산 매물장 / '압구정_뉴스' 탭에 적재:
[일시, 뉴스제목, 출처(URL)]

- 소스: Google News(키워드/도메인 필터), 매일경제/한국경제 공식 RSS
- 중복 방지: 최근 N개 제목+링크 기준
- 일시: KST(YYYY-MM-DD HH:MM)
- 정렬: A열(일시) 오름차순
- 서식: 헤더 고정, C열(출처) 배경 흰색, D열 이후 컬럼 삭제
"""

import os
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
from urllib.parse import quote_plus
import re

import feedparser
import gspread
from google.oauth2.service_account import Credentials

SPREADSHEET_ID = os.getenv("SPREADSHEET_ID")
SHEET_NAME     = os.getenv("SHEET_NAME", "압구정_뉴스")
SA_PATH        = "service_account.json"

HEADERS = ["일시", "뉴스제목", "출처"]          # ✅ 3개만
DEDUP_LIMIT = 2000

# 수집 키워드(시트에는 기록하지 않음)
KEYWORDS = [
    "압구정","부동산","재건축","부동산 세금","보유세",
    "부동산정책","부동산규제","대출규제","대출정책",
    "가계부채","기준금리","전세대출","주담대","규제지역"
]

def rss_urls():
    urls = []
    gnews = "https://news.google.com/rss/search?q={q}&hl=ko&gl=KR&ceid=KR:ko"
    # ① 키워드별
    for k in KEYWORDS:
        urls.append(("GoogleNews", gnews.format(q=quote_plus(k))))
    # ② 네이버/다음만
    site_queries = [
        "site:news.naver.com (압구정 OR 재건축 OR 부동산 OR 규제 OR 주담대)",
        "site:news.daum.net  (압구정 OR 재건축 OR 부동산 OR 규제 OR 주담대)",
    ]
    for q in site_queries:
        urls.append(("GoogleNews", gnews.format(q=quote_plus(q))))
    # ③ 매경/한경 부동산 RSS
    urls += [
        ("매일경제 부동산", "https://www.mk.co.kr/rss/50300009/"),
        ("한국경제 부동산", "https://www.hankyung.com/feed/realestate"),
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
        ws = sh.add_worksheet(title=SHEET_NAME, rows=2000, cols=10)
    normalize_sheet(ws)
    return ws

def normalize_sheet(ws):
    """헤더/고정, C열 흰색, D열 이후 삭제(있으면)."""
    # 헤더 강제
    cur = ws.get_values("A1:C1")
    if not cur or (cur and cur[0] != HEADERS):
        ws.update("A1:C1", [HEADERS])
    # 헤더 고정
    try:
        ws.freeze(rows=1)
    except Exception:
        pass
    # C열 배경 흰색 (색상 넣지 말기)
    try:
        ws.format("C:C", {"backgroundColor": {"red": 1, "green": 1, "blue": 1}})
    except Exception:
        pass
    # D열 이후 있으면 삭제
    try:
        if ws.col_count > 3:
            ws.delete_columns(4, ws.col_count - 3)
    except Exception:
        pass

def get_existing_sets(ws, limit=DEDUP_LIMIT):
    """제목/링크 최근 limit개 추출(헤더 제외)."""
    try:
        titles = ws.col_values(2)[1:]
    except Exception:
        titles = []
    try:
        links = ws.col_values(3)[1:]
    except Exception:
        links = []
    titles = [t.strip() for t in titles if t][-limit:]
    links  = [l.strip() for l in links  if l][-limit:]
    return set(titles), set(links)

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

def collect():
    if not SPREADSHEET_ID:
        raise RuntimeError("SPREADSHEET_ID env missing")

    ws = auth_sheet()
    existing_titles, existing_links = get_existing_sets(ws)

    rows, seen_links = [], set()   # [일시, 뉴스제목, 출처(URL)]

    for _, url in rss_urls():
        feed = feedparser.parse(url)
        for e in feed.entries:
            title = (getattr(e, "title", "") or "").strip()
            link  = (getattr(e, "link", "")  or "").strip()
            if not title:
                continue
            if title in existing_titles or (link and (link in existing_links or link in seen_links)):
                continue
            rows.append([to_kst(e), title, link])
            existing_titles.add(title)
            if link:
                existing_links.add(link); seen_links.add(link)

    if rows:
        ws.append_rows(rows, value_input_option="RA_
