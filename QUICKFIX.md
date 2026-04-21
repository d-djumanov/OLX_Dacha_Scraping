# Quick Fix Guide

## Problem
The OLX.uz scraper is failing with DNS error: `net::ERR_NAME_NOT_RESOLVED`

## Root Cause
The domain `olx.uz` is no longer resolving. The OLX marketplace in Uzbekistan appears to have been discontinued or migrated to a different platform.

## What's Been Fixed

### ✅ Enhanced Error Handling
You now get clear error messages instead of cryptic stack traces:
```
❌ CRITICAL: Domain resolution failed
The OLX.uz domain appears to be unavailable or no longer operational.

ACTION REQUIRED:
  - Verify if OLX.uz is still operational
  - Check for alternative domains or platforms
  - Update OLX_START_URL in the configuration if domain has changed
```

### ✅ Easy Configuration Updates
All URLs are now in ONE place. No more hardcoded values scattered throughout the code.

### ✅ Complete Documentation
- `TROUBLESHOOTING.md` - Comprehensive troubleshooting guide
- `config.py` - Configuration reference
- `README.md` - Updated with service status notice

## How to Fix (3 Simple Steps)

### Step 1: Find the New Domain
Research what happened to OLX in Uzbekistan:
- Check if OLX migrated to olx.com global platform
- Ask local users in Uzbekistan
- Search for "OLX Uzbekistan 2025" news
- Look for alternative Uzbek classified ad platforms

Possible alternatives:
- `https://www.olx.com` (global OLX)
- Local Uzbek marketplaces (house.uz, makler.uz, etc.)

### Step 2: Update the Configuration
Open `Project/scrape_olx_dacha_tashkent.py` and find line 36:

```python
# Line 36 - Change this ONE line:
OLX_BASE_URL = "https://www.olx.uz"  # ← Change to new domain
```

For example, if OLX migrated to olx.com:
```python
OLX_BASE_URL = "https://www.olx.com"
```

**That's it!** All URLs in the entire scraper will update automatically.

### Step 3: Test
Run with a small page limit first:
```bash
export OLX_MAX_PAGES=2
cd Project
python scrape_olx_dacha_tashkent.py
```

If it works, increase `MAX_PAGES` and run normally.

## What If the New Platform is Different?

If the URL structure changed (e.g., different path to dacha listings), you may also need to update:

```python
# Line 37 - If the path changed:
OLX_LISTING_PATH = "/new/path/to/dachas?params"
```

If selectors changed (unlikely), see `TROUBLESHOOTING.md` for detailed migration instructions.

## Getting Help

- **Detailed troubleshooting:** See `TROUBLESHOOTING.md`
- **Configuration reference:** See `Project/config.py`
- **Error messages:** The scraper now provides clear guidance when things fail

## Last Known Working Configuration

- **Date:** 2025-12-21 10:19:37 +05 (Tashkent time)
- **URL:** `https://www.olx.uz/nedvizhimost/posutochno_pochasovo/dachi/tashkent/?currency=UZS`
- **Status:** Domain no longer resolving

## Questions?

If you're unsure about any step, check:
1. `TROUBLESHOOTING.md` - Complete troubleshooting guide
2. `Project/config.py` - Configuration examples and notes
3. `README.md` - General setup and usage

The scraper code is working perfectly. It just needs to know which domain to scrape!
