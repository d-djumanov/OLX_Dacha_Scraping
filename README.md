# OLX Dacha Scraper — Tashkent Region

Scrapes vacation houses/dacha listings from OLX.uz (Tashkent region), with phone reveal, amenities/rules detection, and writes to Google Sheet + daily CSV.

---

## ⚠️ IMPORTANT NOTICE - Service Status

**As of December 21, 2025**: The `olx.uz` domain is **NOT RESOLVING** and appears to be discontinued.

**Error**: `net::ERR_NAME_NOT_RESOLVED at https://www.olx.uz/`

### What This Means
The OLX platform in Uzbekistan may have:
- Been discontinued or shut down
- Migrated to a different domain
- Merged with another platform

### What You Need to Do
1. **Verify OLX status** in Uzbekistan (check with local users/sources)
2. **Find the new platform** for dacha listings in Uzbekistan
3. **Update the configuration** (see [TROUBLESHOOTING.md](TROUBLESHOOTING.md) for detailed instructions)

The scraper code is working correctly, but needs the correct target URL to be configured.

**Last known working date**: December 21, 2025 10:19 AM (Tashkent time)

For detailed troubleshooting steps, see [TROUBLESHOOTING.md](TROUBLESHOOTING.md)

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

### 4. Running

```bash
python scrape_olx_dacha_tashkent.py
```

- Produces: `olx_dacha_tashkent_raw_{YYYYMMDD}.csv`
- Updates Google Sheet (`OLX_Dacha_Tashkent` → `raw_listings`)
- Maintains `state.json` for deduplication

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

## Updating Target URL

If OLX.uz has migrated to a new domain or you want to scrape a different platform:

1. **Open** `Project/scrape_olx_dacha_tashkent.py`
2. **Find** the `OLX_BASE_URL` constant (around line 36):
   ```python
   OLX_BASE_URL = "https://www.olx.uz"
   ```
3. **Update** with the new domain (keep the https://)
4. **Optionally update** `OLX_LISTING_PATH` if the URL structure changed
5. **Test** with a small page limit first:
   ```bash
   export OLX_MAX_PAGES=2
   python Project/scrape_olx_dacha_tashkent.py
   ```
6. **Verify** data extraction works correctly

See [config.py](Project/config.py) for configuration guidance and [TROUBLESHOOTING.md](TROUBLESHOOTING.md) for detailed migration instructions.

---

## Support

If you hit quota limits, check Google API project/service account sharing. For Playwright troubleshooting, rerun `playwright install`.

For domain resolution issues, see [TROUBLESHOOTING.md](TROUBLESHOOTING.md).

---
