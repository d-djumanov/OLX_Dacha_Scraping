import os
import re
import csv
import json
import random
import logging
import hashlib
from datetime import datetime
from time import sleep
from typing import List, Dict, Any, Optional

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

REGION_KEYWORDS = [
    "Чарвак", "Charvak", "Chorvoq", "Чимган", "Chimgan", "Chimyon", "Бельдерсай",
    "Beldersay", "Bo‘stonliq", "Parkent", "Qibray", "Зангиота", "Zangiota"
]

AMENITY_PATTERNS = {
    "pool": re.compile(r"\b(бассейн|hovuz|hovz|pool)\b", re.I),
    "billiards": re.compile(r"\b(бильярд|bilyard)\b", re.I),
    "karaoke": re.compile(r"\b(караоке)\b", re.I),
    "table_tennis": re.compile(r"\b(настольн(?:ый)?\s*теннис|stol\s*tennisi|ping\s*pong)\b", re.I),
    "sauna": re.compile(r"\b(сауна|banya|баня)\b", re.I),
    "wifi": re.compile(r"\b(wi[- ]?fi|вай[- ]?фай)\b", re.I),
    "ac": re.compile(r"\b(кондиционер|konditsioner)\b", re.I),
    "parking": re.compile(r"\b(парковк\w*|автостоянк\w*|parking)\b", re.I),
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

# ==== LOGGING ====
logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s: %(message)s',
    handlers=[logging.FileHandler(LOGFILE), logging.StreamHandler()]
)

# ==== HELPERS ====
def detect_script(s: str) -> str:
    if not s: return "unknown"
    ru = re.search(r"[А-Яа-яЁё]", s)
    uz_cyr = re.search(r"[ҚқҒғЎўҲҳ]", s)
    lat = re.search(r"[A-Za-z]", s)
    buckets = [bool(ru or uz_cyr), bool(lat)]
    if all(buckets): return "mixed"
    if ru or uz_cyr: return "uz_cyr" if uz_cyr and not ru else "ru"
    if lat: return "uz_lat"
    return "unknown"

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

def parse_posted_dt(dt_text:str) -> Optional[str]:
    try:
        dt = dtparser.parse(dt_text, dayfirst=True)
        return dt.astimezone().isoformat()
    except Exception:
        return None

def get_google_sheet():
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive"
    ]
    creds = Credentials.from_service_account_file(SERVICE_ACCOUNT_JSON, scopes=scopes)
    gc = gspread.authorize(creds)
    sh = gc.open(SPREADSHEET_NAME)
    return sh.worksheet(WORKSHEET_NAME)

def _col_label(n: int) -> str:
    # 1 -> A, 26 -> Z, 27 -> AA
    label = ""
    while n:
        n, r = divmod(n - 1, 26)
        label = chr(65 + r) + label
    return label

def update_google_sheet(rows:List[List[Any]], header:List[str], pk_col:int, old_data:pd.DataFrame):
    sheet = get_google_sheet()
    sheet_data = sheet.get_all_values()
    sheet_df = pd.DataFrame(sheet_data[1:], columns=sheet_data[0]) if sheet_data else pd.DataFrame(columns=header)

    need_update, need_insert = [], []
    for row in rows:
        pk = row[pk_col]
        found = sheet_df[sheet_df.iloc[:, pk_col] == pk] if not sheet_df.empty else pd.DataFrame()
        if found.empty:
            need_insert.append(row)
        else:
            ix = found.index[0]
            old_row = found.iloc[0]
            changed = False
            for field in ["price_uzs","negotiable","seller_phone","views_count"]:
                col_idx = header.index(field)
                if str(old_row.get(field, "")) != str(row[col_idx]):
                    changed = True
                    break
            if changed:
                need_update.append((ix+2, row))  # +2 = header + 1-indexing

    if need_insert:
        sheet.append_rows(need_insert, value_input_option="USER_ENTERED")
    for ix, row in need_update:
        last_col = _col_label(len(header))
        sheet.update(f"A{ix}:{last_col}{ix}", [row])

# ==== SCRAPER ====
def scrape_olx_listings() -> List[str]:
    urls: List[str] = []
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

        for attempt in range(2):
            try:
                page.goto(OLX_START_URL, wait_until="domcontentloaded", timeout=120_000)
                for sel in ['button:has-text("Принять")',
                            'button:has-text("Qabul qilish")',
                            'button:has-text("Accept")']:
                    try:
                        page.locator(sel).first.click(timeout=1500)
                        break
                    except Exception:
                        pass
                page.wait_for_selector('a[href*="/d/obyavlenie/"]', timeout=30_000)
                break
            except PWTimeoutError:
                if attempt == 0:
                    page.wait_for_timeout(2000)
                    continue
                else:
                    try: page.screenshot(path="listings_timeout.png", full_page=True)
                    except Exception: pass
                    raise

        soup = BeautifulSoup(page.content(), "lxml")
        for a in soup.select('a[href*="/d/obyavlenie/"]'):
            href = a.get("href")
            if not href: continue
            if href.startswith("/"):
                href = "https://www.olx.uz" + href
            urls.append(href)

        context.close()
        browser.close()

    return sorted(set(urls))

def scrape_olx_ad(url:str) -> Optional[Dict[str,Any]]:
    try:
        scrape_ts = datetime.now().astimezone().isoformat()
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

            # Detect bot/challenge pages
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
            for span in soup.find_all("span"):
                tx = span.text.strip()
                if "Опубликовано" in tx or "E'lon qilingan" in tx:
                    posted_dt_local = parse_posted_dt(tx)
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
            lang_detect = detect_script(full_text)
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
            has_parking = flags.get("parking",False)
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
                "scrape_ts": scrape_ts,
                "listing_id": listing_id,
                "url": url,
                "title": title,
                "price_uzs": price_uzs,
                "negotiable": negotiable,
                "region": region,
                "district": district,
                "rooms": rooms,
                "capacity_beds": capacity_beds,
                "area_m2": area_m2,
                "posted_dt_local": posted_dt_local,
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
                "has_parking": has_parking,
                "has_terrace": has_terrace,
                "has_garden": has_garden,
                "lang_detect": lang_detect
            }

    except Exception as e:
        logging.warning("Failed to scrape ad %s: %s", url, e)
        return None

def main():
    # Load state and old data
    state = load_state(STATE_FILE)
    scraped_ids = set(state.get("listing_ids", []))

    header = [
        "scrape_ts","listing_id","url","title","price_uzs","negotiable","region","district",
        "rooms","capacity_beds","area_m2","posted_dt_local","seller_name","seller_type",
        "seller_phone","seller_phone_hash","views_count","amenities","rules","photo_count",
        "has_pool","has_billiards","has_karaoke","has_table_tennis","has_sauna","has_wifi",
        "has_ac","has_parking","has_terrace","has_garden","lang_detect"
    ]
    pk_col = 1  # listing_id

    # Scrape listing URLs
    ad_urls = scrape_olx_listings()
    rows = []
    new_listing_ids = set(scraped_ids)

    # Counters for diagnostics
    skipped_keyword = 0
    failed_nav = 0
    parsed_ok = 0

    for url in tqdm(ad_urls, desc="Scraping ads"):
        random_delay()
        ad_data = scrape_olx_ad(url)
        if not ad_data:
            failed_nav += 1
            continue

        # Skip keyword filter only when running locally (CI already category-filters)
        if (not CI) and (not keyword_match((ad_data.get("title") or "") + " " + (ad_data.get("amenities") or ""))):
            skipped_keyword += 1
            continue

        listing_id = ad_data["listing_id"]
        new_listing_ids.add(listing_id)
        rows.append([ad_data.get(h) for h in header])
        parsed_ok += 1

    logging.info("SUMMARY: found=%d parsed_ok=%d failed_nav=%d skipped_keyword=%d",
                 len(ad_urls), parsed_ok, failed_nav, skipped_keyword)

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
    today = datetime.now().strftime("%Y%m%d")
    csv_path = LOCAL_CSV_PATTERN.format(date=today)
    with open(csv_path, "w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(header)
        for row in rows:
            writer.writerow(row)
    logging.info("Saved local CSV: %s", csv_path)

    # Save state
    state["listing_ids"] = list(new_listing_ids)
    state["last_run_ts"] = datetime.now().astimezone().isoformat()
    state["last_scrape_count"] = len(rows)
    save_state(STATE_FILE, state)
    logging.info("Done. Scraped %d ads.", len(rows))

if __name__ == "__main__":
    main()
