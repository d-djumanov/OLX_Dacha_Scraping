# OLX Dacha Scraper — Tashkent Region

Scrapes vacation houses/dacha listings from OLX.uz (Tashkent region), with phone reveal, amenities/rules detection, and writes to Google Sheet + daily CSV.

---

## Setup

### 1. Requirements

Python 3.11 recommended.

```bash
pip install -r requirements.txt
playwright install
```

### 2. Service Account

- Place your Google service account JSON file (e.g. `dacha-data-scraping-bc5665b6482e.json`) **in the same folder** as `scrape_olx_dacha_tashkent.py`.
- Share your Google Sheet (`OLX_Dacha_Tashkent`) with the service account email (`olx-scraping-service-account@dacha-data-scraping.iam.gserviceaccount.com`) **as an editor**.

### 3. Google Sheet Setup

- Create Google Sheet: `OLX_Dacha_Tashkent`
- Add worksheet: `raw_listings`
- Set up headers (first row, exactly):

```
scrape_ts | listing_id | url | title | price_uzs | negotiable | region | district | rooms | capacity_beds | area_m2 | posted_dt_local | seller_name | seller_type | seller_phone | seller_phone_hash | views_count | amenities | rules | photo_count | has_pool | has_billiards | has_karaoke | has_table_tennis | has_sauna | has_wifi | has_ac | has_parking | has_terrace | has_garden | lang_detect
```

**Automatic Worksheet Management:**
- When `raw_listings` reaches 50,000 rows, the scraper automatically creates a new worksheet named `raw_listings_YYYYMM` (e.g., `raw_listings_202510` for October 2025)
- Headers are automatically added to new worksheets
- The scraper always uses the most recent worksheet that has capacity

### 4. Running

```bash
python scrape_olx_dacha_tashkent.py
```

- Produces: `olx_dacha_tashkent_raw_{YYYYMMDD}.csv`
- Updates Google Sheet (`OLX_Dacha_Tashkent` → `raw_listings` or most recent dated worksheet)
- Maintains `state.json` for deduplication
- Automatically creates new worksheets when capacity is reached (50,000 rows)

### 5. Cron Job Example

Daily at 04:00 Tashkent time:

```cron
0 1 * * * cd /path/to/scraper && /usr/bin/python3.11 scrape_olx_dacha_tashkent.py >> scrape_olx_dacha_tashkent.log 2>&1
```

*(Server UTC+0; Tashkent UTC+5)*

---

## Features

- **Playwright** for scraping and phone reveal.
- **BeautifulSoup/lxml** for parsing.
- **pandas** for structuring.
- **gspread + google-auth** for Google Sheet write.
- **Rapidfuzz** for fuzzy keyword matching.
- **Deduplication**: primary key is `listing_id`; updates price/phone/views.
- **Random delay** + user-agent rotation for stealth.
- **State file**: `state.json` tracks all scraped `listing_id`s.
- **Logging**: `scrape_olx_dacha_tashkent.log`.

---

## Acceptance Tests

- Scrapes ≥ 20 dacha listings in Tashkent region.
- Inserts under correct headers in Google Sheets.
- Produces daily CSV file.
- Handles phone reveal + view count.
- Amenities/rules detection (RU + UZ).
- Deduplication/update works.
- Script/lang detection works.
- UZ-Cyr → UZ-Latin transliteration.
- Normalization of text for matching.
- Keyword matching robust to noise.
- Logs errors gracefully; skips failed ads.

---

## Audit

- Keeps both original and normalized text (in code, for audit).
- Stores `lang_detect` as: `ru` | `uz_lat` | `uz_cyr` | `mixed`.

---

## Support

If you hit quota limits, check Google API project/service account sharing. For Playwright troubleshooting, rerun `playwright install`.

### Worksheet Capacity Management

The scraper automatically manages Google Sheets capacity:
- Maximum rows per worksheet: 50,000 (configurable via `MAX_SHEET_ROWS` in code)
- When full, creates new worksheet: `raw_listings_YYYYMM`
- Automatically selects the most recent worksheet with capacity
- Headers are initialized automatically in new worksheets

If you need to adjust the threshold, modify `MAX_SHEET_ROWS` in `scrape_olx_dacha_tashkent.py`.

---
