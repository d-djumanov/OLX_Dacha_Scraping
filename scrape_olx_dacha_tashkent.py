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
    # Add some modern user agents for rotation
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.2 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36 Edg/120.0.0.0",
    "Mozilla/5.0 (Linux; Android 13; SM-G991B) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Mobile Safari/537.36",
]
# Dacha keywords (RU + UZ + fuzzy variants)
#
# The original keyword list contained only full words like "дача" or
# "коттедж".  In practice, listings often use different forms such as
# "дачи", "дачный", "коттеджный" or partial Latin transliterations.
# To improve recall we use stems (e.g. "дач" instead of "дача") so
# that substring checks will match related forms.  We also include
# transliterated Latin keywords and English "farm".  See README for details.
DACHA_KEYWORDS = [
    "дач",          # matches дача, дачи, дачный
    "коттед",       # matches коттедж, коттеджи, коттеджный
    "загородн",     # matches загородный, загородном
    "дом отдых",    # matches дом отдыха, дома отдыха
    "вилл",         # matches вилла, виллы, виллов
    "hovli",
    "dacha",        # latin script dacha/dacha ijaraga
    "ijaraga",
    "villa",
    "cottej",
    "dam olish",
    "dam",
    "ферм",         # matches ферма, фермер
    "farm"          # english transliteration
]
# Precompute lowercase keywords for matching
DACHA_KEYWORDS_NORM = [k.lower() for k in DACHA_KEYWORDS]

REGION_KEYWORDS = [
    "Чарвак", "Charvak", "Chorvoq", "Charvak", "Чимган", "Chimgan", "Chimyon", "Бельдерсай",
    "Beldersay", "Bo‘stonliq", "Parkent", "Qibray", "Зангиота", "Zangiota"
]

# Amenity/rule regexes (see prompt)
AMENITY_PATTERNS = {
    "pool": re.compile(r"\b(бассейн|hovuz|hovz|pool)\b", re.I),
    "billiards": re.compile(r"\b(бильярд|bilyard)\b", re.I),
    "karaoke": re.compile(r"\b(караоке)\b", re.I),
    "table_tennis": re.compile(r"\b(настольн(?:ый|ый)?\s*теннис|stol\s*tennisi|ping\s*pong)\b", re.I),
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

# ==== LOGGING ====
logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s: %(message)s',
    handlers=[
        logging.FileHandler(LOGFILE),
        logging.StreamHandler()
    ]
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
    """
    Normalize text by lowercasing and stripping punctuation.

    This helper no longer attempts to transliterate Russian Cyrillic characters.
    Previously we would convert certain Uzbek Cyrillic letters (e.g. "ч" → "ch")
    which produced a mixture of Cyrillic and Latin characters.  That made it
    difficult to reliably match Russian keywords like "дача" because the
    transliterated form became "даchа".  Instead we keep the original script
    for Russian words and only normalize whitespace and punctuation.  Transliteration
    of Uzbek specific letters is handled separately when needed in keyword_match.
    """
    if not s:
        return ""
    # Normalise apostrophes and diacritics
    s = re.sub(r"[’`']", "’", s)
    # Unicode NFKC normalisation fixes composed/decomposed forms
    s = unicodedata.normalize("NFKC", s)
    # Lower case everything for case‑insensitive matching
    s = s.lower()
    # Normalise common Uzbek apostrophe spellings
    s = s.replace("o'", "o‘").replace("g'", "g‘")
    # Replace any character that is not a letter, digit, underscore, space or apostrophe with a space
    s = re.sub(r"[^\w\s’]", " ", s)
    # Collapse multiple whitespace into a single space
    s = re.sub(r"\s+", " ", s).strip()
    return s

def extract_flags(text: str):
    t = text or ""
    flags = {k: bool(p.search(t)) for k,p in AMENITY_PATTERNS.items()}
    rules = [k for k,p in RULE_PATTERNS.items() if p.search(t)]
    return flags, "|".join(sorted(rules))

def keyword_match(text: str) -> bool:
    """
    Return True if any dacha keyword is found in the given text.

    We normalise the input text using canon_text and also produce a version
    transliterated from Uzbek Cyrillic to Latin.  Direct substring checks
    are used on both forms.  Additionally we attempt a fuzzy partial match
    via rapidfuzz (when available) with a threshold of 80 instead of 85 to
    increase recall.  Any exceptions from rapidfuzz are ignored so that the
    function degrades gracefully if the library is missing.
    """
    norm = canon_text(text or "")
    # Transliterate Uzbek Cyrillic letters to Latin for additional matching.
    translit = uz_cyr_to_lat(norm)
    for kw in DACHA_KEYWORDS_NORM:
        # direct substring match in either normalised or transliterated text
        if kw in norm or kw in translit:
            return True
        # fuzzy match using rapidfuzz's partial_ratio if available
        try:
            if fuzz.partial_ratio(kw, norm) >= 80 or fuzz.partial_ratio(kw, translit) >= 80:
                return True
        except Exception:
            # rapidfuzz may not be available or may raise; ignore fuzzy match in that case
            continue
    return False

def normalize_phone(phone: str) -> Optional[str]:
    # Only +998XXYYYYYYY allowed
    if not phone: return None
    phone = re.sub(r"[^\d]", "", phone)
    if phone.startswith("998"):
        phone = "+" + phone
    elif phone.startswith("8") and len(phone) == 12:
        phone = "+998" + phone[1:]
    elif phone.startswith("0") and len(phone) == 10:
        phone = "+998" + phone[1:]
    if re.match(r"\+998\d{9}$", phone):
        return phone
    return None

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
    # OLX ad URL: /d/obyavlenie/...
    m = re.search(r"/obyavlenie/([a-zA-Z0-9\-]+)", url)
    if m:
        return m.group(1)
    # fallback: last part of URL
    return url.rstrip("/").split("/")[-1]

def parse_posted_dt(dt_text:str) -> Optional[str]:
    # "Опубликовано 17 августа 2025 г." etc.
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

def update_google_sheet(rows:List[List[Any]], header:List[str], pk_col:int, old_data:pd.DataFrame):
    """
    Insert new rows; update changed rows in place (by primary key col).
    """
    sheet = get_google_sheet()
    # Load current sheet data & index by pk_col
    sheet_data = sheet.get_all_values()
    sheet_df = pd.DataFrame(sheet_data[1:], columns=sheet_data[0])
    need_update = []
    need_insert = []
    for row in rows:
        pk = row[pk_col]
        found = sheet_df[sheet_df.iloc[:, pk_col] == pk]
        if found.empty:
            need_insert.append(row)
        else:
            # Compare fields to update
            ix = found.index[0]
            old_row = found.iloc[0]
            changed = False
            for field in ["price_uzs","negotiable","seller_phone","views_count"]:
                col_idx = header.index(field)
                if str(old_row[field]) != str(row[col_idx]):
                    changed = True
                    break
            if changed:
                need_update.append((ix+2, row)) # +2 for header and 1-indexing
    # Insert new
    if need_insert:
        sheet.append_rows(need_insert, value_input_option="USER_ENTERED")
    # Update in place
    for ix, row in need_update:
        sheet.update(f"A{ix}:{chr(65+len(header)-1)}{ix}", [row])

# ==== SCRAPER ====
def scrape_olx_listings():
    logging.info("Scraping OLX listings...")
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(user_agent=random.choice(UA_LIST))
        page = context.new_page()
        # Navigate to the start URL and wait for the results to load.  We use
        # networkidle to ensure that the initial set of listings has been
        # downloaded.  Without waiting, page.content() may return before
        # dynamic results render and no ad links will be found.
        page.goto(OLX_START_URL, timeout=60000, wait_until="networkidle")
        # Ensure at least one listing card is present before reading the HTML.
        try:
            page.wait_for_selector('a[href*="/d/obyavlenie/"]', timeout=10000)
        except Exception:
            pass
        html = page.content()
        soup = BeautifulSoup(html, "lxml")
        # Find pagination
        pagination = soup.find("ul", {"data-testid": "pagination-list"})
        last_page = 1
        if pagination:
            try:
                last_page = int(pagination.find_all("li")[-2].text)
            except Exception:
                last_page = 1
        ad_links = set()
        for page_num in range(1, last_page+1):
            url = OLX_START_URL + f"&page={page_num}" if page_num > 1 else OLX_START_URL
            page.goto(url, timeout=60000, wait_until="networkidle")
            try:
                page.wait_for_selector('a[href*="/d/obyavlenie/"]', timeout=10000)
            except Exception:
                pass
            html = page.content()
            soup = BeautifulSoup(html, "lxml")
            # Find all ad links
            for link in soup.select('a[href*="/d/obyavlenie/"]'):
                href = link.get("href")
                if href:
                    full_url = "https://www.olx.uz" + href if href.startswith("/") else href
                    ad_links.add(full_url)
            random_delay()
        logging.info("Found %d ad URLs.", len(ad_links))
        browser.close()
    return list(ad_links)

def scrape_olx_ad(url:str, page=None) -> Optional[Dict[str,Any]]:
    try:
        scrape_ts = datetime.now().astimezone().isoformat()
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            context = browser.new_context(user_agent=random.choice(UA_LIST))
            pg = context.new_page()
            # Navigate to the ad page and wait for the network to be idle.  This
            # ensures that dynamic components such as the description and
            # breadcrumb information are available in the DOM.
            pg.goto(url, timeout=60000, wait_until="networkidle")
            # Wait for the description container (if it exists) to be attached.
            try:
                pg.wait_for_selector('div[data-testid="ad-description"]', timeout=10000)
            except Exception:
                pass
            html = pg.content()
            soup = BeautifulSoup(html, "lxml")
            listing_id = get_listing_id(url)
            title = soup.find("h1")
            title = title.text.strip() if title else None
            price_el = soup.find("h3", {"data-testid": "ad-price"})
            price_text = price_el.text.strip() if price_el else None
            negotiable = False
            if price_text and ("договорная" in price_text.lower() or "kelishiladi" in price_text.lower()):
                price_uzs = None
                negotiable = True
            else:
                price_uzs = None
                if price_text:
                    price_uzs = int(re.sub(r"[^\d]", "", price_text)) if re.search(r"\d", price_text) else None
            # region/district from breadcrumbs
            region, district = None, None
            bc = soup.select('a[data-testid="location-link"]')
            if bc:
                if len(bc) >= 1:
                    region = bc[0].text.strip()
                if len(bc) >= 2:
                    district = bc[-1].text.strip()
            # rooms/capacity/area
            rooms = capacity_beds = area_m2 = None
            attrs = soup.find_all("li", {"data-testid":"ad-attribute-value"})
            for li in attrs:
                txt = li.text.strip().lower()
                if "комнат" in txt or "xona" in txt:
                    rooms = int(re.sub(r"[^\d]", "", txt)) if re.search(r"\d", txt) else None
                elif "спальных мест" in txt or "o‘rin" in txt:
                    capacity_beds = int(re.sub(r"[^\d]", "", txt)) if re.search(r"\d", txt) else None
                elif "м²" in txt or "kv" in txt or "м2" in txt:
                    area_m2 = int(re.sub(r"[^\d]", "", txt)) if re.search(r"\d", txt) else None
            posted_dt_local = None
            for span in soup.find_all("span"):
                txt = span.text.strip()
                if "Опубликовано" in txt or "E'lon qilingan" in txt:
                    posted_dt_local = parse_posted_dt(txt)
                    break
            # Seller
            seller_name = seller_type = None
            seller_box = soup.find("div", {"data-testid":"seller-profile"})
            if seller_box:
                seller_name = seller_box.find("h4")
                seller_name = seller_name.text.strip() if seller_name else None
                seller_type = seller_box.find("span")
                seller_type = seller_type.text.strip() if seller_type else None
            # Phone (click to reveal)
            seller_phone = None
            try:
                phone_btn = soup.find("button", attrs={"data-testid":"phone-reveal-button"})
                if phone_btn:
                    phone_btn_selector = '[data-testid="phone-reveal-button"]'
                    pg.click(phone_btn_selector)
                    sleep(2)
                    phone_el = pg.query_selector('[data-testid="phone-reveal-phone"]')
                    seller_phone = phone_el.inner_text().strip() if phone_el else None
            except Exception as e:
                logging.warning("Phone scrape failed for %s: %s", url, e)
            seller_phone = normalize_phone(seller_phone)
            seller_phone_hash = sha256_hash(seller_phone) if seller_phone else None
            # Views count
            views_count = None
            views_el = soup.find("span", {"data-testid":"views-count"})
            if views_el:
                vtxt = views_el.text.strip()
                views_count = int(re.sub(r"[^\d]", "", vtxt)) if re.search(r"\d", vtxt) else None
            # Description, amenities, rules
            desc = soup.find("div", {"data-testid":"ad-description"})
            description = desc.text.strip() if desc else ""
            full_text = (title or "") + " " + description
            lang_detect = detect_script(full_text)
            norm_text = canon_text(full_text)
            flags, rules = extract_flags(full_text + " " + norm_text)
            amenities = "|".join([k for k,v in flags.items() if v])
            # Photos
            photo_count = len(soup.select('img[data-testid="image-gallery-photo"]'))
            # Boolean amenities
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
            # Only keep matching dacha ads
            if not keyword_match(full_text) and not keyword_match(norm_text):
                return None
            # Exclude apartments unless tagged with dacha
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
    pk_col = 1 # listing_id
    # Scrape listing URLs
    ad_urls = scrape_olx_listings()
    rows = []
    new_listing_ids = set(scraped_ids)
    for url in tqdm(ad_urls, desc="Scraping ads"):
        random_delay()
        ad_data = scrape_olx_ad(url)
        if not ad_data: continue
        listing_id = ad_data["listing_id"]
        new_listing_ids.add(listing_id)
        row = [ad_data.get(h) for h in header]
        rows.append(row)
    # Deduplication/update
    # Read existing sheet data for updates
    try:
        sheet = get_google_sheet()
        sheet_data = sheet.get_all_values()
        old_data = pd.DataFrame(sheet_data[1:], columns=sheet_data[0])
    except Exception as e:
        logging.warning("Could not load Google Sheet: %s", e)
        old_data = pd.DataFrame([], columns=header)
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
    save_state(STATE_FILE, state)
    logging.info("Done. Scraped %d ads.", len(rows))

if __name__ == "__main__":
    main()