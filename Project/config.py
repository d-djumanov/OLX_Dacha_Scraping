# OLX Dacha Scraper Configuration
# 
# This file contains the main configuration for the scraper.
# Update these values if OLX.uz domain changes or if you need to adapt
# the scraper to a different platform.

# ==== SITE CONFIGURATION ====

# Main URL to scrape dacha listings from
# Format: https://DOMAIN/path/to/dachas?params
# 
# CURRENT STATUS (2025-12-21): olx.uz domain is NOT RESOLVING
# The OLX.uz service appears to be discontinued in Uzbekistan.
#
# TO FIX THIS:
# 1. Research the current platform for Uzbekistan dacha listings
# 2. Update SITE_BASE_URL below with the new domain
# 3. Verify the LISTING_PATH still works (or update it)
# 4. Test with a small MAX_PAGES value (e.g., 2)
SITE_BASE_URL = "https://www.olx.uz"
LISTING_PATH = "/nedvizhimost/posutochno_pochasovo/dachi/tashkent/?currency=UZS"

# Automatically constructed - usually doesn't need changing
# Unless the new platform uses a completely different URL structure
START_URL = f"{SITE_BASE_URL}{LISTING_PATH}"

# ==== ALTERNATIVE DOMAINS TO TRY ====
# Uncomment and test these if olx.uz is down:
# SITE_BASE_URL = "https://www.olx.com"  # Global OLX platform
# SITE_BASE_URL = "https://olx.co.uz"    # Alternative TLD
# SITE_BASE_URL = "https://NEW_PLATFORM.uz"  # New local platform

# ==== SCRAPING PARAMETERS ====

# Maximum number of listing pages to scrape
# Reduce this for testing (e.g., 2-5 pages)
# Increase for production (e.g., 20-50 pages)
MAX_PAGES = 20

# Stop after this many consecutive empty pages
STOP_AFTER_EMPTY_PAGES = 2

# ==== PAGE SELECTORS ====
# CSS selectors for finding elements on the page
# Update these if the new platform has different HTML structure

# Selector for listing links on search results page
LISTING_LINK_SELECTOR = 'a[href*="/d/obyavlenie/"]'

# Cookie consent buttons (multiple languages)
COOKIE_ACCEPT_BUTTONS = [
    'button:has-text("Принять")',      # Russian
    'button:has-text("Qabul qilish")',  # Uzbek
    'button:has-text("Accept")',        # English
]

# ==== GOOGLE SHEETS CONFIGURATION ====
SERVICE_ACCOUNT_JSON = "dacha-data-scraping-bc5665b6482e.json"
SPREADSHEET_NAME = "OLX_Dacha_Tashkent"
WORKSHEET_NAME = "raw_listings"

# ==== FILE PATHS ====
STATE_FILE = "state.json"
LOG_FILE = "scrape_olx_dacha_tashkent.log"
CSV_FILENAME_PATTERN = "olx_dacha_tashkent_raw_{date}.csv"

# ==== NOTES FOR PLATFORM MIGRATION ====
#
# If OLX.uz has moved to a new platform, you may need to:
#
# 1. UPDATE DOMAIN:
#    Change SITE_BASE_URL to the new domain
#
# 2. VERIFY SELECTORS:
#    The new platform may have different CSS selectors.
#    Check LISTING_LINK_SELECTOR and update if needed.
#
# 3. CHECK URL STRUCTURE:
#    Listing URLs might have a different format.
#    Look for patterns like:
#    - /ad/123456
#    - /listing/123456
#    - /item/123456
#    Update code in scrape_olx_dacha_tashkent.py if needed.
#
# 4. TEST INCREMENTALLY:
#    Start with MAX_PAGES = 2 to test
#    Increase gradually once working
#
# 5. UPDATE DOCUMENTATION:
#    Update README.md with new platform info
#    Note any structural changes in TROUBLESHOOTING.md
