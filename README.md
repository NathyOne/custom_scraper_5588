# Faces Perfumes Scraper

Scrapy project that collects perfume products from faces.sa and exports a Shopify-ready CSV.

## Configuration

Set the site domain via environment variables:

- `SITE_NAME` (root domain, default: `faces.sa`)
- `SITE_BASE_DOMAIN` (optional full host, default: `www.<SITE_NAME>`)

If you use a local `.env` file, load it in your shell before running the spider.

## Usage

```bash
scrapy crawl faces_perfumes_shopify -O output/faces_perfumes_shopify_ar.csv
```
# custom_scraper_5588
