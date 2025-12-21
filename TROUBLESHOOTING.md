# Troubleshooting Guide

## Domain Resolution Error: OLX.uz Not Found

### Issue
The scraper fails with error:
```
playwright._impl._errors.Error: Page.goto: net::ERR_NAME_NOT_RESOLVED at https://www.olx.uz/...
```

### Root Cause
The domain `olx.uz` is not resolving via DNS, indicating that:
1. **OLX.uz service may have been discontinued** in Uzbekistan
2. The platform may have migrated to a different domain
3. There are network/DNS configuration issues preventing access

### Diagnosis Steps

1. **Check if OLX.uz is accessible**:
   ```bash
   nslookup olx.uz
   curl -I https://www.olx.uz
   ```

2. **Test from different networks**:
   - Try accessing from different locations/networks
   - Use VPN if geo-restrictions apply
   - Check with local users in Uzbekistan

3. **Research current status**:
   - Search for "OLX Uzbekistan 2025 status"
   - Check if OLX migrated to OLX.com global platform
   - Look for announcements about service changes

### Solution

#### If OLX migrated to a new domain:
1. Update `OLX_BASE_URL` in `scrape_olx_dacha_tashkent.py` (around line 36):
   ```python
   OLX_BASE_URL = "https://NEW_DOMAIN"
   ```

2. If the URL structure changed, also update `OLX_LISTING_PATH`:
   ```python
   OLX_LISTING_PATH = "/path/to/dachas?params"
   ```

3. Verify the page structure is compatible:
   - Check if selectors still work
   - Test with small `MAX_PAGES` value
   - Validate data extraction

4. Update README.md with new domain information

#### If OLX is discontinued:
1. Identify alternative platforms for Uzbekistan classified ads
2. Adapt scraper for new platform (may require significant changes)
3. Update repository documentation

### Alternative Platforms to Consider
If OLX.uz is no longer available, consider these Uzbekistan alternatives:
- **olx.com** (global platform, if Uzbekistan listings moved there)
- **Local Uzbekistan marketplaces** (e.g., house.uz, makler.uz if they exist)
- **Regional platforms** (check current popular classifieds in Uzbekistan)

### Getting Help
1. Contact local users in Uzbekistan to confirm OLX status
2. Check local tech forums/communities
3. Consult with repository owner about alternative data sources

### Temporary Workaround
If the service is temporarily down:
- Monitor OLX.uz status
- Set up automated checks
- Use cached data from previous successful runs

### Last Known Working Configuration
- **Date**: 2025-12-21 10:19:37 +05 (Tashkent time)
- **URL**: `https://www.olx.uz/nedvizhimost/posutochno_pochasovo/dachi/tashkent/?currency=UZS`
- **Status**: Domain no longer resolving

---

## Other Common Issues

### Playwright Installation
If you get browser not found errors:
```bash
playwright install
playwright install-deps
```

### Google Sheets Access
If Google Sheets integration fails:
- Verify service account JSON file exists
- Check sheet is shared with service account email
- Validate sheet name and worksheet name match configuration

### Rate Limiting
If scraping is blocked:
- Increase delays between requests
- Rotate user agents (already implemented)
- Reduce MAX_PAGES setting
- Consider using proxies (requires code modification)
