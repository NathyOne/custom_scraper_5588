import csv
import html
import json
import os
import re
import unicodedata
from collections import defaultdict
from pathlib import Path
from urllib.parse import unquote, urlparse

import scrapy


def _normalize_domain(value):
    value = value.strip().lower()
    value = value.replace("https://", "").replace("http://", "")
    return value.strip("/")


SITE_NAME = _normalize_domain(os.getenv("SITE_NAME", "faces.sa"))
SITE_ROOT = SITE_NAME[4:] if SITE_NAME.startswith("www.") else SITE_NAME
BASE_DOMAIN = _normalize_domain(os.getenv("SITE_BASE_DOMAIN", f"www.{SITE_ROOT}"))
ALLOWED_DOMAINS = sorted({SITE_ROOT, BASE_DOMAIN})
if SITE_NAME not in ALLOWED_DOMAINS:
    ALLOWED_DOMAINS.append(SITE_NAME)


class FacesPerfumesShopifySpider(scrapy.Spider):
    name = "faces_perfumes_shopify"
    allowed_domains = ALLOWED_DOMAINS

    base_domain = BASE_DOMAIN
    base_url = f"https://{base_domain}"
    locale_path = "/ar"

    page_size = 48
    max_pages_per_category = 200
    categories = (
        ("perfume-for-women", "عطور نسائية"),
        ("perfume-for-men", "عطور رجالية"),
        ("luxury-perfumes", "عطور نيش"),
    )

    shopify_fields = [
        "Handle",
        "Title",
        "Body (HTML)",
        "Vendor",
        "Product Category",
        "Type",
        "Tags",
        "Published",
        "Option1 Name",
        "Option1 Value",
        "Variant SKU",
        "Variant Grams",
        "Variant Inventory Tracker",
        "Variant Inventory Qty",
        "Variant Inventory Policy",
        "Variant Fulfillment Service",
        "Variant Price",
        "Variant Compare At Price",
        "Variant Requires Shipping",
        "Variant Taxable",
        "Variant Barcode",
        "Image Src",
        "Image Position",
        "Image Alt Text",
        "Gift Card",
        "SEO Title",
        "SEO Description",
        "Status",
    ]

    custom_settings = {
        "FEED_EXPORT_FIELDS": shopify_fields,
        "DOWNLOAD_DELAY": 0.5,
        "CONCURRENT_REQUESTS_PER_DOMAIN": 1,
        "DEFAULT_REQUEST_HEADERS": {
            "Accept-Language": "ar-SA,ar;q=0.9,en-US;q=0.8,en;q=0.7",
        },
    }

    def __init__(self, resume_file="", *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.seen_pages = set()
        self.seen_products = set()
        self.product_categories = defaultdict(set)
        self.handle_counts = defaultdict(int)
        self.resume_keys = set()
        self.resume_handle_bases = set()
        self.resume_enabled = False
        self.resume_file = Path(resume_file).expanduser() if resume_file else None
        self.country_cookies = {
            "country": "SA",
            "locale": "ar-SA",
            "currency": "SAR",
        }
        self._load_resume_state()

    def start_requests(self):
        for slug, label in self.categories:
            url = self._category_url(slug, start=0)
            yield scrapy.Request(
                url=url,
                callback=self.parse_category,
                cb_kwargs={"slug": slug, "label": label, "start": 0},
                cookies=self.country_cookies,
            )

    def parse_category(self, response, slug, label, start):
        page_key = (slug, start)
        if page_key in self.seen_pages:
            return
        self.seen_pages.add(page_key)

        links = self._extract_product_links(response)
        new_products = 0

        for product_url in links:
            self.product_categories[product_url].add(label)
            if product_url in self.seen_products:
                continue
            self.seen_products.add(product_url)
            if self._is_known_product_url(product_url):
                self.crawler.stats.inc_value("resume/skipped_product_links")
                continue
            new_products += 1
            yield response.follow(
                product_url,
                callback=self.parse_product,
                cb_kwargs={"source_category": label},
                cookies=self.country_cookies,
            )

        page_index = start // self.page_size
        should_continue = (
            bool(links)
            and (new_products > 0 or self.resume_enabled)
            and page_index + 1 < self.max_pages_per_category
        )
        if should_continue:
            next_start = start + self.page_size
            yield scrapy.Request(
                url=self._category_url(slug, start=next_start),
                callback=self.parse_category,
                cb_kwargs={"slug": slug, "label": label, "start": next_start},
                cookies=self.country_cookies,
            )

    def parse_product(self, response, source_category):
        canonical = response.css("link[rel='canonical']::attr(href)").get("")
        if canonical.endswith("/ar/404"):
            return

        product_json = self._extract_product_json(response)
        if self._is_out_of_stock(response, product_json):
            self.crawler.stats.inc_value("products/skipped_out_of_stock")
            return
        product_url = self._normalize_product_url(response.url)
        pid = self._extract_pid(response)

        title = self._clean_text(
            self._first_non_empty(
                self._json_get(product_json, "name"),
                response.css("meta[property='og:title']::attr(content)").get(),
                response.css("h1::text").get(),
                self._title_from_url(product_url),
            )
        )
        description_text = self._clean_text(
            self._first_non_empty(
                self._json_get(product_json, "description"),
                response.css("meta[name='description']::attr(content)").get(),
            )
        )
        vendor = self._clean_text(
            self._first_non_empty(
                self._extract_brand(product_json),
                "غير محدد",
            )
        )
        price = self._extract_price(product_json, response.text)
        if not price:
            self.crawler.stats.inc_value("products/skipped_missing_price")
            return
        variations = self._extract_variations(response.text, title)
        variation_value = " | ".join(variations) if variations else "افتراضي"
        sku = self._clean_text(self._first_non_empty(self._json_get(product_json, "sku"), pid))
        resume_key = self._build_resume_key(sku=sku, pid=pid, url=product_url)
        if self.resume_enabled and resume_key in self.resume_keys:
            self.crawler.stats.inc_value("resume/skipped_products")
            return
        images = self._extract_images(response, product_json, pid)
        tags = sorted(self.product_categories.get(product_url, set()) | {source_category})
        tags_text = ", ".join(tags)

        handle = self._build_handle(product_url, pid)
        body_html = self._description_to_html(description_text)

        row = self._blank_row()
        row.update(
            {
                "Handle": handle,
                "Title": title,
                "Body (HTML)": body_html,
                "Vendor": vendor,
                "Product Category": "العطور",
                "Type": "عطور",
                "Tags": tags_text,
                "Published": "TRUE",
                "Option1 Name": "الحجم",
                "Option1 Value": variation_value,
                "Variant SKU": sku,
                "Variant Inventory Policy": "deny",
                "Variant Fulfillment Service": "manual",
                "Variant Price": price,
                "Variant Inventory Tracker": "shopify",
                "Variant Inventory Qty": "1",
                "Variant Requires Shipping": "TRUE",
                "Variant Taxable": "TRUE",
                "Image Src": images[0] if images else "",
                "Image Position": "1" if images else "",
                "Image Alt Text": title,
                "Gift Card": "FALSE",
                "SEO Title": title,
                "SEO Description": description_text[:320],
                "Status": "active",
            }
        )
        if resume_key:
            self.resume_keys.add(resume_key)
        self.resume_keys.add(self._build_resume_key(handle=handle))
        self.resume_handle_bases.add(self._slug_from_url(product_url, pid))
        yield row

        for idx, image_url in enumerate(images[1:], start=2):
            image_row = self._blank_row()
            image_row.update(
                {
                    "Handle": handle,
                    "Image Src": image_url,
                    "Image Position": str(idx),
                    "Image Alt Text": title,
                }
            )
            yield image_row

    def _category_url(self, slug, start):
        return f"{self.base_url}{self.locale_path}/{slug}?start={start}&sz={self.page_size}"

    def _load_resume_state(self):
        if not self.resume_file or not self.resume_file.exists():
            return

        handles = set()
        try:
            with self.resume_file.open("r", encoding="utf-8-sig", newline="") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    sku = self._clean_text(row.get("Variant SKU"))
                    handle = self._clean_text(row.get("Handle"))
                    if sku:
                        self.resume_keys.add(self._build_resume_key(sku=sku))
                    elif handle:
                        self.resume_keys.add(self._build_resume_key(handle=handle))
                    if handle:
                        handles.add(handle)
        except OSError as exc:
            self.logger.warning("Resume file unreadable (%s): %s", self.resume_file, exc)
            return

        for handle in handles:
            slug, ordinal = self._parse_handle(handle, handles)
            if slug:
                self.handle_counts[slug] = max(self.handle_counts[slug], ordinal)
                self.resume_handle_bases.add(slug)

        self.resume_enabled = bool(self.resume_keys)
        if self.resume_enabled:
            self.logger.info(
                "Resume mode: loaded %d product keys from %s",
                len(self.resume_keys),
                self.resume_file,
            )

    @staticmethod
    def _parse_handle(handle, all_handles):
        match = re.match(r"^(.*?)-(\d+)$", handle)
        if not match:
            return handle, 1
        base, suffix = match.groups()
        if not base or base not in all_handles:
            return handle, 1
        return base, int(suffix)

    @staticmethod
    def _build_resume_key(sku="", pid="", url="", handle=""):
        if sku:
            return f"sku:{sku.lower()}"
        if pid:
            return f"pid:{pid.lower()}"
        if url:
            return f"url:{url.lower()}"
        if handle:
            return f"handle:{handle.lower()}"
        return ""

    def _is_known_product_url(self, product_url):
        if not self.resume_enabled:
            return False
        slug = self._slug_from_url(product_url)
        return bool(slug and slug in self.resume_handle_bases)

    def _extract_product_links(self, response):
        hrefs = response.css("a::attr(href)").re(r"/ar/(?:p/[^\"'#?]+|\d+)\.html")
        unique_links = []
        seen = set()
        for href in hrefs:
            normalized = self._normalize_product_url(response.urljoin(html.unescape(href)))
            if normalized in seen:
                continue
            seen.add(normalized)
            unique_links.append(normalized)
        return unique_links

    @staticmethod
    def _normalize_product_url(url):
        parsed = urlparse(url)
        netloc = parsed.netloc.replace("faces.ae", "faces.sa")
        return f"{parsed.scheme}://{netloc}{parsed.path}"

    def _extract_product_json(self, response):
        for raw in response.css("script::text").getall():
            text = (raw or "").strip()
            if not text:
                continue
            try:
                data = json.loads(text)
            except json.JSONDecodeError:
                continue

            candidates = data if isinstance(data, list) else [data]
            for item in candidates:
                if not isinstance(item, dict):
                    continue
                item_type = item.get("@type")
                if item_type == "Product" or (
                    isinstance(item_type, list) and "Product" in item_type
                ):
                    return item
        return {}

    @staticmethod
    def _json_get(product_json, key):
        if not isinstance(product_json, dict):
            return ""
        return product_json.get(key, "")

    def _extract_brand(self, product_json):
        brand = self._json_get(product_json, "brand")
        if isinstance(brand, dict):
            return brand.get("name", "")
        return brand or ""

    @staticmethod
    def _extract_pid(response):
        pids = [p.strip() for p in response.css("[data-pid]::attr(data-pid)").getall() if p.strip()]
        if not pids:
            return ""
        # Product pages usually repeat the active pid multiple times.
        return max(set(pids), key=pids.count)

    def _extract_price(self, product_json, body):
        offers = self._json_get(product_json, "offers")
        price_value = ""
        if isinstance(offers, dict):
            price_value = self._first_non_empty(
                offers.get("price", ""),
                offers.get("lowPrice", ""),
                offers.get("highPrice", ""),
            )
        elif isinstance(offers, list):
            first = offers[0] if offers else {}
            if isinstance(first, dict):
                price_value = self._first_non_empty(
                    first.get("price", ""),
                    first.get("lowPrice", ""),
                    first.get("highPrice", ""),
                )

        if not price_value:
            match = re.search(r'"price"\s*:\s*"?([0-9.,]+)"?', body)
            if match:
                price_value = match.group(1)

        if not price_value:
            meta_match = re.search(
                r'(?:itemprop|property)=["\'](?:price|product:price:amount|og:price:amount)["\']'
                r'[^>]*content=["\']([0-9.,]+)["\']',
                body,
            )
            if meta_match:
                price_value = meta_match.group(1)

        if not price_value:
            data_match = re.search(r'data-(?:sale-)?price=["\']([0-9.,]+)["\']', body)
            if data_match:
                price_value = data_match.group(1)

        if not price_value:
            matches = re.findall(r"(\d[\d,\.]*)\s*(?:ر\.س|SAR)\b", body)
            if matches:
                price_value = matches[0]

        normalized = re.sub(r"[^0-9.]", "", str(price_value))
        if not normalized:
            return ""
        try:
            return f"{float(normalized):.2f}"
        except ValueError:
            return normalized

    def _extract_variations(self, body, title):
        values = re.findall(r'"item_size":"([^"]+)"', body)
        values = [self._clean_text(v) for v in values if self._clean_text(v)]
        values = self._unique_preserve_order(values)

        if values:
            return values

        title_sizes = re.findall(r"(\d+(?:\.\d+)?\s*(?:ml|مل|g|جم|oz))", title, flags=re.I)
        title_sizes = [self._clean_text(v) for v in title_sizes if self._clean_text(v)]
        return self._unique_preserve_order(title_sizes)

    def _extract_images(self, response, product_json, pid):
        urls = re.findall(r"https://(?:www\.)?faces\.(?:sa|ae)/dw/image[^\"'\s<>]+", response.text)
        urls = [html.unescape(u).strip() for u in urls]
        sw800_urls = [u for u in urls if "sw=800" in u]

        if pid:
            pid_sw800 = [u for u in sw800_urls if pid in u]
            if pid_sw800:
                return self._unique_preserve_order(pid_sw800)

        if sw800_urls:
            return self._unique_preserve_order(sw800_urls)

        json_images = self._json_get(product_json, "image")
        if isinstance(json_images, list):
            return self._unique_preserve_order([str(i) for i in json_images if i])
        if isinstance(json_images, str) and json_images:
            return [json_images]
        return []

    def _slug_from_url(self, url, pid=""):
        slug = url.rsplit("/", 1)[-1].replace(".html", "")
        slug = unquote(slug).lower()
        slug = unicodedata.normalize("NFKD", slug).encode("ascii", "ignore").decode("ascii")
        slug = re.sub(r"[^a-z0-9]+", "-", slug).strip("-")

        if not slug and pid:
            slug = f"product-{pid.lower()}"
        if not slug:
            slug = "product"
        return slug

    def _build_handle(self, url, pid):
        slug = self._slug_from_url(url, pid)
        count = self.handle_counts[slug]
        self.handle_counts[slug] += 1
        if count > 0:
            return f"{slug}-{count + 1}"
        return slug

    def _is_out_of_stock(self, response, product_json):
        offers = self._json_get(product_json, "offers")
        availability = ""
        if isinstance(offers, dict):
            availability = offers.get("availability", "")
        elif isinstance(offers, list) and offers:
            first = offers[0]
            if isinstance(first, dict):
                availability = first.get("availability", "")

        availability_text = str(availability).lower()
        if "instock" in availability_text:
            return False
        if "outofstock" in availability_text or "soldout" in availability_text:
            return True

        body = response.text
        out_of_stock_markers = [
            "غير متوفر حالياً",
            "غير متوفر حاليا",
            "نفد من المخزون",
            "out of stock",
            "sold out",
        ]
        add_to_cart_markers = [
            "أضف إلى حقيبة التسوق",
            "أضف إلى السلة",
            "add to cart",
        ]
        if any(marker in body for marker in out_of_stock_markers):
            if any(marker in body for marker in add_to_cart_markers):
                return False
            return True
        return False

    def _title_from_url(self, url):
        slug = url.rsplit("/", 1)[-1].replace(".html", "")
        return self._clean_text(unquote(slug).replace("-", " "))

    @staticmethod
    def _description_to_html(description):
        if not description:
            return ""
        escaped = html.escape(description).replace("\r\n", "\n").replace("\r", "\n")
        escaped = escaped.replace("\n\n", "\n").replace("\n", "<br>")
        return f"<p>{escaped}</p>"

    @staticmethod
    def _first_non_empty(*values):
        for value in values:
            if value:
                return value
        return ""

    @staticmethod
    def _clean_text(value):
        text = html.unescape(str(value or ""))
        text = re.sub(r"\s+", " ", text)
        return text.strip()

    @staticmethod
    def _unique_preserve_order(values):
        seen = set()
        result = []
        for value in values:
            if value in seen:
                continue
            seen.add(value)
            result.append(value)
        return result

    def _blank_row(self):
        return {field: "" for field in self.shopify_fields}
