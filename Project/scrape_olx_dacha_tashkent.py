import os
import re
import csv
import json
import random
import logging
import hashlib
from datetime import datetime
from time import sleep
from typing import List, Dict, Any, Optional, Tuple

from zoneinfo import ZoneInfo
from tqdm import tqdm
from playwright._impl._errors import TimeoutError as PWTimeoutError

import pandas as pd
from rapidfuzz import fuzz
from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright

import gspread
import unicodedata
from google.oauth2.service_account import Credentials
from dateutil import parser as dtparser

# ==== CONFIG ====
SERVICE_ACCOUNT_JSON = "dacha-data-scraping-bc5665b6482e.json"
SPREADSHEET_NAME = "OLX_Dacha_Tashkent"
WORKSHEET_NAME = "raw_listings"
STATE_FILE = "state.json"
LOCAL_CSV_PATTERN = "olx_dacha_tashkent_raw_{date}.csv"
LOGFILE = "scrape_olx_dacha_tashkent.log"

OLX_START_URL = "https://www.olx.uz/nedvizhimost/posutochno_pochasovo/dachi/tashkent/?currency=UZS"

# pagination
MAX_PAGES = int(os.getenv("OLX_MAX_PAGES", "20"))  # scan up to N list pages
STOP_AFTER_EMPTY = 2                                # stop after consecutive empty pages

UA_LIST = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.2 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36 Edg/120.0.0.0",
    "Mozilla/5.0 (Linux; Android 13; SM-G991B) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Mobile Safari/537.36",
]

# Dacha keywords (stems + UZ/RU/EN variants)
DACHA_KEYWORDS = [
    "дач", "коттед", "загородн", "дом отдых", "вилл",
    "hovli", "dacha", "ijaraga", "villa", "cottej", "dam olish", "dam",
    "ферм", "farm"
]
DACHA_KEYWORDS_NORM = [k.lower() for k in DACHA_KEYWORDS]

AMENITY_PATTERNS = {
    "pool": re.compile(r"\b(бассейн|hovuz|hovz|pool)\b", re.I),
    "billiards": re.compile(r"\b(бильярд|bilyard)\b", re.I),
    "karaoke": re.compile(r"\b(караоке)\b", re.I),
    "table_tennis": re.compile(r"\b(настольн(?:ый)?\s*теннис|stol\s*tennisi|ping\s*pong)\b", re.I),
    "sauna": re.compile(r"\b(сауна|banya|баня)\b", re.I),
    "wifi": re.compile(r"\b(wi[- ]?fi|вай[- ]?фай)\b", re.I),
    "ac": re.compile(r"\b(кондиционер|konditsioner)\b", re.I),
    "terrace": re.compile(r"\b(террас\w*)\b", re.I),
    "garden_bbq": re.compile(r"\b(сад|мангал|barbekyu|barbecue|bbq)\b", re.I),
}
RULE_PATTERNS = {
    "families_only": re.compile(r"(только\s*семей|семьям|oilalarga)", re.I),
    "no_parties": re.compile(r"(без\s*(шум|вечерин)|party.*(запрет|нельзя))", re.I),
    "no_unmarried": re.compile(r"(свидетельство|nikoh|паспорт.*сем)", re.I),
    "kids_ok": re.compile(r"(с\s*детьми|bolalar)", re.I),
    "pets": re.compile(r"(с\s*животн|pets|hayvon)", re.I),
}

UZ_CYR2LAT = {
    "Қ":"Q","қ":"q","Ғ":"G‘","ғ":"g‘","Ў":"O‘","ў":"o‘","Ё":"Yo","ё":"yo",
    "Ю":"Yu","ю":"yu","Я":"Ya","я":"ya","Ш":"Sh","ш":"sh","Ч":"Ch","ч":"ch",
    "Ц":"S","ц":"s","Й":"Y","й":"y","Ъ":"’","ъ":"’","Ь":"","ь":"",
    "Х":"X","х":"x","Э":"E","э":"e","Ҳ":"H","ҳ":"h","Ж":"J","ж":"j"
}

# CI flag (GitHub Actions sets CI=1)
CI = os.getenv("CI", "0") == "1"
TASHKENT = ZoneInfo("Asia/Tashkent")

# ==== LOGGING ====
logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s: %(message)s',
    handlers=[logging.FileHandler(LOGFILE), logging.StreamHandler()]
)

# ==== HELPERS ====
def now_local_str() -> str:
    return datetime.now(TASHKENT).strftime("%Y-%m-%d %H:%M:%S")

def page_url(base: str, page: int) -> str:
    if page <= 1:
        return base
    sep = "&" if "?" in base else "?"
    return f"{base}{sep}page={page}"

def uz_cyr_to_lat(s: str) -> str:
    return "".join(UZ_CYR2LAT.get(ch, ch) for ch in s)

def canon_text(s: str) -> str:
    if not s:
        return ""
    s = re.sub(r"[’`']", "’", s)
    s = unicodedata.normalize("NFKC", s)
    s = s.lower()
    s = s.replace("o'", "o‘").replace("g'", "g‘")
    s = re.sub(r"[^\w\s’]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s

def extract_flags(text: str):
    t = text or ""
    flags = {k: bool(p.search(t)) for k,p in AMENITY_PATTERNS.items()}
    rules = [k for k,p in RULE_PATTERNS.items() if p.search(t)]
    return flags, "|".join(sorted(rules))

def keyword_match(text: str) -> bool:
    norm = canon_text(text or "")
    translit = uz_cyr_to_lat(norm)
    for kw in DACHA_KEYWORDS_NORM:
        if kw in norm or kw in translit:
            return True
        try:
            if fuzz.partial_ratio(kw, norm) >= 80 or fuzz.partial_ratio(kw, translit) >= 80:
                return True
        except Exception:
            continue
    return False

def normalize_phone(phone: str) -> Optional[str]:
    if not phone: return None
    phone = re.sub(r"[^\d]", "", phone)
    if phone.startswith("998"):
        phone = "+" + phone
    elif phone.startswith("8") and len(phone) == 12:
        phone = "+998" + phone[1:]
    elif phone.startswith("0") and len(phone) == 10:
        phone = "+998" + phone[1:]
    return phone if re.match(r"\+998\d{9}$", phone) else None

def sha256_hash(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()

def random_delay():
    sleep(random.uniform(2, 5))

def load_state(state_file:str) -> Dict[str,Any]:
    if not os.path.exists(state_file):
        return {}
    try:
        with open(state_file, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}

def save_state(state_file:str, state:Dict[str,Any]):
    with open(state_file, "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2)

def get_listing_id(url:str) -> str:
    m = re.search(r"/obyavlenie/([a-zA-Z0-9\-]+)", url)
    if m: return m.group(1)
    return url.rstrip("/").split("/")[-1]

def parse_posted_dt_text(dt_text:str) -> Optional[str]:
    """Return 'YYYY-MM-DD HH:MM:SS' in Asia/Tashkent, or None."""
    try:
        dt = dtparser.parse(dt_text, dayfirst=True)
        return dt.astimezone(TASHKENT).strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return None

def extract_ad_id(soup: BeautifulSoup) -> Optional[str]:
    """
    Extract OLX 'ID...' from the ad page.
    We look for a dedicated node, otherwise fall back to a regex scan.
    """
    # direct nodes sometimes exist
    for sel in ['[data-cy="ad_id"]', '[data-testid="ad-id"]', '[data-testid="ad_id"]']:
        el = soup.select_one(sel)
        if el:
            txt = el.get_text(" ", strip=True)
            m = re.search(r"\bID[:\s\-]*([0-9A-Za-z]+)\b", txt, re.I)
            if m:
                return "ID" + m.group(1)

    # broad scan
    txt = soup.get_text(" ", strip=True)[:2000]
    m = re.search(r"\bID[:\s\-]*([0-9A-Za-z]+)\b", txt, re.I)
    if m:
        return "ID" + m.group(1)
    return None

# === Google Sheets helpers (ensure worksheet + header) ===
def get_google_sheet():
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive"
    ]
    creds = Credentials.from_service_account_file(SERVICE_ACCOUNT_JSON, scopes=scopes)
    gc = gspread.authorize(creds)

    try:
        sh = gc.open(SPREADSHEET_NAME)
    except gspread.SpreadsheetNotFound:
        sh = gc.create(SPREADSHEET_NAME)

    try:
        ws = sh.worksheet(WORKSHEET_NAME)
    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet(title=WORKSHEET_NAME, rows=1000, cols=40)

    return ws

# ---- Helpers for Sheets batching/retry ----
def _col_to_a1(n: int) -> str:
    """1 -> A, 26 -> Z, 27 -> AA ..."""
    s = ""
    while n:
        n, r = divmod(n - 1, 26)
        s = chr(65 + r) + s
    return s

def _gsheet_retry(fn, attempts=6, base_delay=1.0):
    """Retry on quota/rate-limit errors with exponential backoff."""
    delay = base_delay
    for i in range(attempts):
        try:
            return fn()
        except Exception as e:
            msg = str(e).lower()
            if any(k in msg for k in ("rate_limit", "quota", "resource_exhausted", "429")) and i < attempts - 1:
                sleep(delay + random.uniform(0, 0.4))
                delay = min(delay * 2, 30)
                continue
            raise

# You can tweak via env if you like; defaults are safe.
_SHEETS_THROTTLE = float(os.getenv("SHEETS_THROTTLE_SEC", "0"))   # e.g., 0.4
_MAX_BATCH = int(os.getenv("SHEETS_MAX_UPDATE_BATCH", "200"))     # requests per batch

def update_google_sheet(rows: List[List[Any]], header: List[str], pk_col: int, old_data: pd.DataFrame):
    """
    Insert new rows and batch-update changed rows to avoid hitting per-minute write quotas.
    """
    sheet = get_google_sheet()
    last_col_letter = _col_to_a1(len(header))

    # Load existing sheet to decide insert vs update
    sheet_data = sheet.get_all_values()
    sheet_df = pd.DataFrame(sheet_data[1:], columns=sheet_data[0]) if sheet_data else pd.DataFrame(columns=header)

    to_insert: List[List[Any]] = []
    to_update: List[tuple[int, List[Any]]] = []  # (1-based row index, row values)

    for row in rows:
        pk = row[pk_col]
        found = sheet_df[sheet_df.iloc[:, pk_col] == pk]
        if found.empty:
            to_insert.append(row)
        else:
            ix = int(found.index[0])
            old_row = found.iloc[0]
            changed = False
            # Only compare a few frequently changing fields
            for field in ["price_uzs", "negotiable", "seller_phone", "views_count"]:
                if field in header:
                    col_idx = header.index(field)
                    if str(old_row.get(field, "")) != str(row[col_idx]):
                        changed = True
                        break
            if changed:
                # +2 => header row + 1-based indexing
                to_update.append((ix + 2, row))

    # 1) Append new rows in chunks
    if to_insert:
        for i in range(0, len(to_insert), 200):
            chunk = to_insert[i:i + 200]
            _gsheet_retry(lambda: sheet.append_rows(chunk, value_input_option="USER_ENTERED"))
            if _SHEETS_THROTTLE:
                sleep(_SHEETS_THROTTLE)

    # 2) Batch update changed rows (single or few API calls)
    if to_update:
        # Build ValueRanges for values.batchUpdate
        data = []
        for rix, vals in to_update:
            rng = f"A{rix}:{last_col_letter}{rix}"
            data.append({"range": rng, "values": [vals]})

        # Worksheet.batch_update() accepts list of {'range','values'}
        for i in range(0, len(data), _MAX_BATCH):
            chunk = data[i:i + _MAX_BATCH]
            _gsheet_retry(lambda: sheet.batch_update(chunk, value_input_option="USER_ENTERED"))
            if _SHEETS_THROTTLE:
                sleep(_SHEETS_THROTTLE)
# ==== SCRAPER ====
def scrape_olx_listings() -> Tuple[List[str], List[str]]:
    """
    Return (all_ad_urls, list_pages_htmls_for_fallback).
    We paginate across the category pages.
    """
    all_urls: List[str] = []
    pages_html: List[str] = []
    empty_streak = 0

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(
            user_agent=("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                        "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"),
            locale="ru-RU",
            timezone_id="Asia/Tashkent",
        )

        def _route(route):
            if route.request.resource_type in {"image", "media", "font"}:
                return route.abort()
            route.continue_()
        context.route("**/*", _route)

        page = context.new_page()
        page.set_default_timeout(60_000)
        page.set_default_navigation_timeout(120_000)

        for pgno in range(1, MAX_PAGES + 1):
            url = page_url(OLX_START_URL, pgno)
            try:
                page.goto(url, wait_until="domcontentloaded", timeout=120_000)
                # Cookie banner (ru/uz/en)
                for sel in ['button:has-text("Принять")',
                            'button:has-text("Qabul qilish")',
                            'button:has-text("Accept")']:
                    try:
                        page.locator(sel).first.click(timeout=1500)
                        break
                    except Exception:
                        pass
                page.wait_for_selector('a[href*="/d/obyavlenie/"]', timeout=30_000)
            except PWTimeoutError:
                # treat as empty and continue; might be tail
                empty_streak += 1
                if empty_streak >= STOP_AFTER_EMPTY:
                    break
            except Exception as e:
                error_msg = str(e)
                # Check for DNS/Network errors
                if "ERR_NAME_NOT_RESOLVED" in error_msg or "Could not resolve host" in error_msg:
                    logging.error("❌ CRITICAL: Domain resolution failed for %s", url)
                    logging.error("The OLX.uz domain appears to be unavailable or no longer operational.")
                    logging.error("Possible reasons:")
                    logging.error("  1. OLX.uz service has been discontinued in Uzbekistan")
                    logging.error("  2. The domain has migrated to a different URL")
                    logging.error("  3. Network/DNS configuration issues")
                    logging.error("")
                    logging.error("ACTION REQUIRED:")
                    logging.error("  - Verify if OLX.uz is still operational")
                    logging.error("  - Check for alternative domains or platforms")
                    logging.error("  - Update OLX_START_URL in the configuration if domain has changed")
                    raise RuntimeError(f"Domain resolution failed for {url}. OLX.uz may no longer be operational.") from e
                else:
                    # Re-raise other exceptions
                    raise
                continue

            html = page.content()
            pages_html.append(html)
            soup = BeautifulSoup(html, "lxml")

            page_urls = []
            for a in soup.select('a[href*="/d/obyavlenie/"]'):
                href = a.get("href")
                if not href:
                    continue
                if href.startswith("/"):
                    href = "https://www.olx.uz" + href
                page_urls.append(href)

            page_urls = list(set(page_urls))
            if not page_urls:
                empty_streak += 1
            else:
                empty_streak = 0
                all_urls.extend(page_urls)

            page.wait_for_timeout(500)  # small backoff
            if empty_streak >= STOP_AFTER_EMPTY:
                break

        context.close()
        browser.close()

    return sorted(set(all_urls)), pages_html

def parse_list_grid(html: str) -> List[Dict[str, Any]]:
    """Fallback: extract minimal rows straight from the listing grid (no per-ad navigation)."""
    rows = []
    soup = BeautifulSoup(html or "", "lxml")
    for card in soup.select('[data-testid="l-card"]'):
        a = card.select_one('a[href*="/d/obyavlenie/"]')
        if not a:
            continue
        href = a.get("href") or ""
        if href.startswith("/"):
            href = "https://www.olx.uz" + href
        lid = get_listing_id(href)

        title_el = card.select_one('[data-cy="ad_title"], h6, h5, h4')
        title = title_el.get_text(strip=True) if title_el else None

        price_el = card.select_one('[data-testid="ad-price"]')
        price_text = price_el.get_text(strip=True) if price_el else None
        price_uzs = int(re.sub(r"[^\d]", "", price_text)) if price_text and re.search(r"\d", price_text) else None
        negotiable = False
        if price_text and ("договорная" in (price_text or "").lower() or "kelishiladi" in (price_text or "").lower()):
            price_uzs = None
            negotiable = True

        loc_el = card.select_one('[data-testid="location-date"]')
        loc_text = loc_el.get_text(" ", strip=True) if loc_el else None
        region = district = None
        if loc_text:
            parts = [p.strip() for p in re.split(r"[·|•]", loc_text)]
            place = parts[0] if parts else None
            if place and "," in place:
                rparts = [p.strip() for p in place.split(",", 1)]
                region = rparts[0] or None
                district = rparts[1] or None
            else:
                region = place

        flags, rules = extract_flags(title or "")
        amenities = "|".join([k for k,v in flags.items() if v])

        rows.append({
            "scrape_ts": now_local_str(),
            "listing_id": lid,
            "ad_id": None,
            "url": href,
            "title": title,
            "price_uzs": price_uzs,
            "negotiable": negotiable,
            "region": region,
            "district": district,
            "posted_dt_local": None,
            "rooms": None,
            "capacity_beds": None,
            "area_m2": None,
            "seller_name": None,
            "seller_type": None,
            "seller_phone": None,
            "seller_phone_hash": None,
            "views_count": None,
            "amenities": amenities,
            "rules": rules,
            "photo_count": None,
            "has_pool": flags.get("pool", False),
            "has_billiards": flags.get("billiards", False),
            "has_karaoke": flags.get("karaoke", False),
            "has_table_tennis": flags.get("table_tennis", False),
            "has_sauna": flags.get("sauna", False),
            "has_wifi": flags.get("wifi", False),
            "has_ac": flags.get("ac", False),
            "has_terrace": flags.get("terrace", False),
            "has_garden": flags.get("garden_bbq", False),
        })
    return rows

def scrape_olx_ad(url:str) -> Optional[Dict[str,Any]]:
    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            context = browser.new_context(user_agent=random.choice(UA_LIST),
                                          timezone_id="Asia/Tashkent",
                                          locale="ru-RU")
            pg = context.new_page()
            pg.set_default_timeout(60_000)
            pg.set_default_navigation_timeout(120_000)

            try:
                pg.goto(url, wait_until="domcontentloaded", timeout=120_000)
                pg.wait_for_selector("h1, [data-cy='ad_description'], section:has-text('Описание')", timeout=30_000)
            except PWTimeoutError:
                try: pg.screenshot(path=f"ad_timeout_{get_listing_id(url)}.png", full_page=True)
                except Exception: pass
                return None

            # detect bot/challenge
            page_text = (pg.content() or "").lower()
            for bad in ["verify", "captcha", "access denied", "пожалуйста, подтвердите", "robot", "are you human"]:
                if bad in page_text:
                    try:
                        lid = get_listing_id(url)
                        pg.screenshot(path=f"ad_challenge_{lid}.png", full_page=True)
                        with open(f"ad_challenge_{lid}.html", "w", encoding="utf-8") as f:
                            f.write(pg.content() or "")
                    except Exception:
                        pass
                    return None

            soup = BeautifulSoup(pg.content(), "lxml")

            listing_id = get_listing_id(url)
            ad_id = extract_ad_id(soup)

            title_el = soup.find("h1")
            title = title_el.text.strip() if title_el else None

            price_el = soup.find("h3", {"data-testid": "ad-price"})
            price_text = price_el.text.strip() if price_el else None
            negotiable = False
            if price_text and ("договорная" in price_text.lower() or "kelishiladi" in price_text.lower()):
                price_uzs = None
                negotiable = True
            else:
                price_uzs = int(re.sub(r"[^\d]", "", price_text)) if (price_text and re.search(r"\d", price_text)) else None

            region = district = None
            bc = soup.select('a[data-testid="location-link"]')
            if bc:
                region = bc[0].text.strip() if len(bc) >= 1 else None
                district = bc[-1].text.strip() if len(bc) >= 2 else None

            rooms = capacity_beds = area_m2 = None
            for li in soup.find_all("li", {"data-testid":"ad-attribute-value"}):
                txt = li.text.strip().lower()
                if "комнат" in txt or "xona" in txt:
                    rooms = int(re.sub(r"[^\d]", "", txt)) if re.search(r"\d", txt) else None
                elif "спальных мест" in txt or "o‘rin" in txt:
                    capacity_beds = int(re.sub(r"[^\d]", "", txt)) if re.search(r"\d", txt) else None
                elif "м²" in txt or "kv" in txt or "м2" in txt:
                    area_m2 = int(re.sub(r"[^\d]", "", txt)) if re.search(r"\d", txt) else None

            posted_dt_local = None
            # typical text like "Опубликовано 18 августа 2025 г., 14:05"
            for span in soup.find_all("span"):
                tx = span.text.strip()
                if "Опубликовано" in tx or "E'lon qilingan" in tx:
                    posted_dt_local = parse_posted_dt_text(tx)
                    break

            seller_name = seller_type = None
            seller_box = soup.find("div", {"data-testid":"seller-profile"})
            if seller_box:
                sname = seller_box.find("h4")
                seller_name = sname.text.strip() if sname else None
                stype = seller_box.find("span")
                seller_type = stype.text.strip() if stype else None

            # Phone (best effort; may be blocked in CI)
            seller_phone = None
            try:
                if pg.locator('[data-testid="phone-reveal-button"]').first.is_visible():
                    pg.click('[data-testid="phone-reveal-button"]')
                    pg.wait_for_timeout(1500)
                    el = pg.query_selector('[data-testid="phone-reveal-phone"]')
                    seller_phone = el.inner_text().strip() if el else None
            except Exception as e:
                logging.warning("Phone scrape failed for %s: %s", url, e)

            seller_phone = normalize_phone(seller_phone)
            seller_phone_hash = sha256_hash(seller_phone) if seller_phone else None

            views_count = None
            views_el = soup.find("span", {"data-testid":"views-count"})
            if views_el:
                vtxt = views_el.text.strip()
                views_count = int(re.sub(r"[^\d]", "", vtxt)) if re.search(r"\d", vtxt) else None

            desc_el = soup.find("div", {"data-testid":"ad-description"})
            description = desc_el.text.strip() if desc_el else ""
            full_text = (title or "") + " " + description
            norm_text = canon_text(full_text)
            flags, rules = extract_flags(full_text + " " + norm_text)
            amenities = "|".join([k for k,v in flags.items() if v])

            photo_count = len(soup.select('img[data-testid="image-gallery-photo"]'))
            has_pool = flags.get("pool",False)
            has_billiards = flags.get("billiards",False)
            has_karaoke = flags.get("karaoke",False)
            has_table_tennis = flags.get("table_tennis",False)
            has_sauna = flags.get("sauna",False)
            has_wifi = flags.get("wifi",False)
            has_ac = flags.get("ac",False)
            has_terrace = flags.get("terrace",False)
            has_garden = flags.get("garden_bbq",False)

            # Only keep matching dacha ads locally; in CI skip this filter
            if not CI:
                if not keyword_match(full_text) and not keyword_match(norm_text):
                    return None
                if "квартира" in norm_text and not keyword_match(norm_text):
                    return None

            browser.close()

            return {
                "scrape_ts": now_local_str(),
                "listing_id": listing_id,
                "ad_id": ad_id,
                "url": url,
                "title": title,
                "price_uzs": price_uzs,
                "negotiable": negotiable,
                "region": region,
                "district": district,
                "posted_dt_local": posted_dt_local,
                "rooms": rooms,
                "capacity_beds": capacity_beds,
                "area_m2": area_m2,
                "seller_name": seller_name,
                "seller_type": seller_type,
                "seller_phone": seller_phone,
                "seller_phone_hash": seller_phone_hash,
                "views_count": views_count,
                "amenities": amenities,
                "rules": rules,
                "photo_count": photo_count,
                "has_pool": has_pool,
                "has_billiards": has_billiards,
                "has_karaoke": has_karaoke,
                "has_table_tennis": has_table_tennis,
                "has_sauna": has_sauna,
                "has_wifi": has_wifi,
                "has_ac": has_ac,
                "has_terrace": has_terrace,
                "has_garden": has_garden,
            }

    except Exception as e:
        logging.warning("Failed to scrape ad %s: %s", url, e)
        return None

def main():
    # Load state and old data
    state = load_state(STATE_FILE)
    scraped_ids = set(state.get("listing_ids", []))

    header = [
        "scrape_ts","listing_id","ad_id","url","title","price_uzs","negotiable",
        "region","district","posted_dt_local","rooms","capacity_beds","area_m2",
        "seller_name","seller_type","seller_phone","seller_phone_hash","views_count",
        "amenities","rules","photo_count","has_pool","has_billiards","has_karaoke",
        "has_table_tennis","has_sauna","has_wifi","has_ac","has_terrace","has_garden"
    ]
    pk_col = 1  # listing_id

    # Scrape listing URLs (+ page HTMLs for fallback)
    ad_urls, pages_html = scrape_olx_listings()
    rows = []
    new_listing_ids = set(scraped_ids)

    parsed_ok = failed_nav = 0

    for url in tqdm(ad_urls, desc="Scraping ads"):
        random_delay()
        ad_data = scrape_olx_ad(url)
        if not ad_data:
            failed_nav += 1
            continue

        listing_id = ad_data["listing_id"]
        new_listing_ids.add(listing_id)
        rows.append([ad_data.get(h) for h in header])
        parsed_ok += 1

    logging.info("SUMMARY: found=%d parsed_ok=%d failed_nav=%d", len(ad_urls), parsed_ok, failed_nav)

    # Fallback across listing pages if nothing parsed
    if parsed_ok == 0 and ad_urls:
        logging.warning("Per-ad scraping failed (parsed_ok=0). Using list-page fallback across pages.")
        added = 0
        for html in pages_html:
            for ad in parse_list_grid(html):
                new_listing_ids.add(ad["listing_id"])
                rows.append([ad.get(h) for h in header])
                added += 1
        logging.info("Fallback added %d rows from listing grid.", added)

    # Read existing sheet data for updates
    try:
        sheet = get_google_sheet()
        sheet_data = sheet.get_all_values()
        old_data = pd.DataFrame(sheet_data[1:], columns=sheet_data[0]) if sheet_data else pd.DataFrame(columns=header)
    except Exception as e:
        logging.warning("Could not load Google Sheet: %s", e)
        old_data = pd.DataFrame([], columns=header)

    # Update Google Sheet (insert/update)
    if rows:
        update_google_sheet(rows, header, pk_col, old_data)

    # Save local CSV
    today = datetime.now(TASHKENT).strftime("%Y%m%d")
    csv_path = LOCAL_CSV_PATTERN.format(date=today)
    with open(csv_path, "w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(header)
        writer.writerows(rows)
    logging.info("Saved local CSV: %s", csv_path)

    # Save state
    state["listing_ids"] = list(new_listing_ids)
    state["last_run_ts"] = now_local_str()
    state["last_scrape_count"] = len(rows)
    save_state(STATE_FILE, state)
    logging.info("Done. Scraped %d ads.", len(rows))

if __name__ == "__main__":
    main()
