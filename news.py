# -*- coding: utf-8 -*-
"""
시트 구조: [일시, 뉴스제목, 출처(URL)]
- 요약 컬럼/값 사용하지 않음
- 출처에는 항상 '순수 URL'만 기록
"""

import os, html, re
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
from urllib.parse import quote_plus
import feedparser, gspread
from google.oauth2.service_account import Credentials

SPREADSHEET_ID = os.getenv("SPREADSHEET_ID")
SHEET_NAME     = os.getenv("SHEET_NAME", "압구정_뉴스")
SA_PATH        = "service_account.json"

HEADERS     = ["일시", "뉴스제목", "출처"]
DEDUP_LIMIT = 2000

KEYWORDS = [
    "압구정","부동산","재건축","부동산 세금","보유세",
    "부동산정책","부동산규제","대출규제","대출정책",
    "가계부채","기준금리","전세대출","주담대","규제지역"
]

def rss_urls():
    base = "https://news.google.com/rss/search?q={q}&hl=ko&gl=KR&ceid=KR:ko"
    urls = [base.format(q=quote_plus(k)) for k in KEYWORDS]
    urls += [
        base.format(q=quote_plus("site:news.naver.com (압구정 OR 재건축 OR 부동산 OR 규제 OR 주담대)")),
        base.format(q=quote_plus("site:news.daum.net  (압구정 OR 재건축 OR 부동산 OR 규제 OR 주담대)")),
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

    # 시트 형태 강제: 헤더, 열 3개, 헤더 고정, A~C 배경을 흰색으로
    ws.resize(cols=3)
    cur = ws.get_values("A1:C1")
    if not cur or cur[0] != HEADERS:
        ws.update("A1:C1", [HEADERS])
    try: ws.freeze(rows=1)
    except Exception: pass
    try: ws.format("A:C", {"backgroundColor": {"red":1,"green":1,"blue":1}})
    except Exception: pass
    return ws

def clean_text(s: str) -> str:
    # HTML 엔티티 제거 + 공백 정리 (제목만에 적용, URL에는 적용하지 않음)
    if not s: return ""
    return html.unescape(re.sub(r"\s+", " ", s.strip()))

def extract_url(entry) -> str:
    """
    링크 추출을 확실히: link → id → links[].href 순으로 첫 번째 http URL
    (요약/본문은 절대 사용하지 않음)
    """
    cands = []
    if hasattr(entry, "link"):  cands.append(entry.link)
    if hasattr(entry, "id"):    cands.append(entry.id)
    if hasattr(entry, "links"):
        for li in entry.links:
            if isinstance(li, dict) and "href" in li: cands.append(li["href"])
    for u in cands:
        if isinstance(u, str) and u.startswith("http"):
            return u.strip()
    return ""

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

def get_existing_sets(ws):
    try: titles = ws.col_values(2)[1:]
    except Exception: titles = []
    try: links  = ws.col_values(3)[1:]
    except Exception: links  = []
    titles = [t.strip() for t in titles if t][-DEDUP_LIMIT:]
    links  = [l.strip() for l in links  if l][-DEDUP_LIMIT:]
    return set(titles), set(links)

def collect():
    if not SPREADSHEET_ID:
        raise RuntimeError("SPREADSHEET_ID missing")

    ws = auth_sheet()
    exist_titles, exist_links = get_existing_sets(ws)
    rows, seen = [], set()

    for url in rss_urls():
        feed = feedparser.parse(url)
        for e in feed.entries:
            title = clean_text(getattr(e, "title", ""))
            link  = extract_url(e)  # ✅ URL만
            if not title or not link:
                continue
            if title in exist_titles or link in exist_links or link in seen:
                continue
            rows.append([to_kst(e), title, link])
            seen.add(link)

    if rows:
        ws.append_rows(rows, value_input_option="RAW")  # 순수 텍스트(URL) 기록
        try: ws.sort((1, "asc"))  # A열(일시) 오름차순
        except Exception: pass
        # 색상 다시 흰색으로 강제 (시트에 남아있는 대체색/조건부서식 영향 방지)
        try: ws.format("A:C", {"backgroundColor": {"red":1,"green":1,"blue":1}})
        except Exception: pass

    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] inserted={len(rows)}")

if __name__ == "__main__":
    collect()
