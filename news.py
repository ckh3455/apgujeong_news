# -*- coding: utf-8 -*-
"""
원부동산 매물장 / '압구정_뉴스' 탭에 적재:
[일시, 뉴스제목, 요약, 출처(=HYPERLINK), 키워드]

- 소스:
  * Google News 검색 RSS (키워드별)
  * Google News + site:news.naver.com / site:news.daum.net 필터
  * 매일경제(부동산) 공식 RSS
  * 한국경제(부동산) 공식 RSS
- 중복 방지: 최근 N개 '뉴스제목' + '링크(URL)' 기준
  (출처가 HYPERLINK 공식이어도 URL을 파싱해 중복 제거)
- 일시: KST(Asia/Seoul)로 YYYY-MM-DD HH:MM
- 정렬: A열(일시) 오름차순
"""

import os, re
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
from urllib.parse import quote_plus, urlparse

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

DEDUP_LIMIT = 2000  # 최근 N개만 중복 검사 (제목/링크)

def rss_urls():
    urls = []
    gnews_base = "https://news.google.com/rss/search?q={q}&hl=ko&gl=KR&ceid=KR:ko"

    # ① Google News (키워드별)
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
    # 헤더 고정
    try:
        ws.freeze(rows=1)
    except Exception:
        pass
    return ws

def parse_hyperlink_formula(cell: str) -> str:
    """
    =HYPERLINK("URL","TEXT") 형태에서 URL만 추출.
    공식이 아닌 경우(cell이 그냥 URL 등)에는 원문 반환.
    """
    if not cell:
        return ""
    m = re.search(r'HYPERLINK\("([^"]+)"', cell, flags=re.IGNORECASE)
    if m:
        return m.group(1).strip()
    return cell.strip()

def get_existing_sets(ws, limit=DEDUP_LIMIT):
    """제목/링크 최근 limit개 추출 (링크는 HYPERLINK 공식일 수도 있어 URL 파싱)"""
    try:
        titles = ws.col_values(2)[1:]  # B열(뉴스제목), 헤더 제외
    except Exception:
        titles = []
    # D열(출처) — 공식 그대로 가져오기 위해 FORMULA 모드로 읽기
    try:
        formulas = ws.get('D2:D', value_render_option='FORMULA')
        # ws.get(...)는 [[cell],[cell],...] 형태
        links = [parse_hyperlink_formula(r[0]) for r in formulas if r and r[0]]
    except Exception:
        # 대안: 일반 값으로 읽기
        try:
            links = ws.col_values(4)[1:]
        except Exception:
            links = []

    titles = [t.strip() for t in titles if t][-limit:]
    links  = [l.strip() for l in links  if l][-limit:]
    return set(titles), set(links)

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

def make_hyperlink(url: str) -> str:
    """=HYPERLINK("url","도메인") 형태 문자열 생성"""
    if not url:
        return ""
    try:
        netloc = urlparse(url).netloc or url
        netloc = netloc.replace("www.", "")
    except Exception:
        netloc = "링크"
    # URL에 "가 들어갈 가능성은 거의 없지만, 방어적으로 이스케이프
    safe_url = url.replace('"', '%22')
    safe_text = netloc.replace('"', "'")
    return f'=HYPERLINK("{safe_url}","{safe_text}")'

def collect():
    if not SPREADSHEET_ID:
        raise RuntimeError("SPREADSHEET_ID env missing")

    ws = auth_sheet()
    existing_titles, existing_links = get_existing_sets(ws)

    rows, seen_links = [], set()  # [일시, 뉴스제목, 요약, 출처(하이퍼링크), 키워드]

    for tag, url in rss_urls():
        feed = feedparser.parse(url)
        for e in feed.entries:
            title = (getattr(e, "title", "") or "").strip()
            link  = (getattr(e, "link", "")  or "").strip()
            if not title:
                continue
            # 제목/링크 기준 중복 제거 (시트에 있거나 이번 실행에서 이미 본 링크)
            if title in existing_titles or (link and (link in existing_links or link in seen_links)):
                continue

            summary = strip_html(getattr(e, "summary", ""))
            ts_kst  = to_kst(e)
            rows.append([ts_kst, title, summary, make_hyperlink(link), tag])

            existing_titles.add(title)
            if link:
                existing_links.add(link)
                seen_links.add(link)

    if rows:
        # 하이퍼링크 공식 평가를 위해 USER_ENTERED 사용
        ws.append_rows(rows, value_input_option="USER_ENTERED")
        # 일자 기준 오름차순 정렬
        try:
            ws.sort((1, 'asc'))
        except Exception:
            pass

    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] inserted={len(rows)}")

if __name__ == "__main__":
    collect()
