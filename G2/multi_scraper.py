
#!/usr/bin/env python3
# file: multipage_scraper.py
import argparse, csv, json, sys, time, re, random, os, glob
from dataclasses import dataclass, asdict
from typing import List, Optional, Dict
from urllib.parse import urljoin, urlparse, parse_qs, urlencode, urlunparse
import requests
from bs4 import BeautifulSoup

DEFAULT_UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
              "(KHTML, like Gecko) Chrome/124.0 Safari/537.36")

@dataclass
class Review:
    reviewer: Optional[str]
    rating: Optional[float]
    date: Optional[str]
    text: Optional[str]
    source_url: str  # online URL or synthetic file URL for offline pages

# ------------------- headers/cookies/session -------------------

def load_headers(path: Optional[str]) -> Dict[str, str]:
    headers = {}
    if not path:
        headers.update({
            "User-Agent": DEFAULT_UA,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
            "Connection": "keep-alive",
            "Referer": "https://www.yelp.com/",
            "Upgrade-Insecure-Requests": "1",
        })
        return headers
    try:
        with open(path, encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line or ":" not in line:
                    continue
                k, v = line.split(":", 1)
                headers[k.strip()] = v.strip()
        headers.setdefault("User-Agent", DEFAULT_UA)
        return headers
    except Exception as e:
        sys.stderr.write(f"[warn] failed to load headers from {path}: {e}\n")
        return {"User-Agent": DEFAULT_UA}

def load_cookies(path: Optional[str]) -> Dict[str, str]:
    jar = {}
    if not path:
        return jar
    try:
        with open(path, encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                if "\t" in line:  # Netscape format
                    parts = line.split("\t")
                    if len(parts) >= 7:
                        name = parts[-2].strip()
                        value = parts[-1].strip()
                        jar[name] = value
                elif "=" in line:
                    name, value = line.split("=", 1)
                    jar[name.strip()] = value.strip()
    except Exception as e:
        sys.stderr.write(f"[warn] failed to load cookies from {path}: {e}\n")
    return jar

def make_session(headers_path: Optional[str], cookies_path: Optional[str]) -> requests.Session:
    s = requests.Session()
    s.headers.update(load_headers(headers_path))
    for k, v in load_cookies(cookies_path).items():
        s.cookies.set(k, v)
    return s

# ------------------- fetching & parsing -------------------

def fetch(session: requests.Session, url: str, timeout=30, max_retries=4, backoff=1.8) -> Optional[str]:
    for attempt in range(1, max_retries + 1):
        try:
            r = session.get(url, timeout=timeout)
            if 200 <= r.status_code < 300:
                return r.text
            if r.status_code in (429, 500, 502, 503, 504, 403):
                sys.stderr.write(f"[warn] status {r.status_code} for {url}\n")
                time.sleep(backoff ** attempt)
            else:
                sys.stderr.write(f"[warn] status {r.status_code} for {url}\n")
                return None
        except requests.RequestException as e:
            sys.stderr.write(f"[warn] {e} on {url}\n")
            time.sleep(backoff ** attempt)
    return None

def get_text_or_none(node):
    if not node:
        return None
    txt = node.get_text(strip=True)
    return txt if txt != "" else None

def parse_rating(val: Optional[str]) -> Optional[float]:
    if not val:
        return None
    m = re.search(r"(\d+(?:\.\d+)?)", val)
    try:
        return float(m.group(1)) if m else None
    except ValueError:
        return None

def parse_reviews(html: str, page_url: str, sel: Dict[str, str]) -> List[Review]:
    soup = BeautifulSoup(html, "html.parser")
    items = soup.select(sel["review_container"])
    reviews: List[Review] = []
    for it in items:
        reviewer = get_text_or_none(it.select_one(sel.get("reviewer",""))) if sel.get("reviewer") else None
        rating_node = it.select_one(sel.get("rating","")) if sel.get("rating") else None
        rating_raw = None
        if rating_node:
            rating_raw = rating_node.get("aria-label") or rating_node.get("title") or rating_node.get_text(" ", strip=True)
        rating = parse_rating(rating_raw)
        date = get_text_or_none(it.select_one(sel.get("date",""))) if sel.get("date") else None
        text = get_text_or_none(it.select_one(sel.get("text",""))) if sel.get("text") else None
        if not any([reviewer, rating, date, text]):
            continue
        reviews.append(Review(reviewer=reviewer, rating=rating, date=date, text=text, source_url=page_url))
    return reviews

def find_next_url(html: str, page_url: str, sel_next: Optional[str]) -> Optional[str]:
    soup = BeautifulSoup(html, "html.parser")
    if sel_next:
        n = soup.select_one(sel_next)
        if n and n.get("href"):
            return urljoin(page_url, n["href"])
    n = soup.select_one("a[rel='next'], link[rel='next']")
    if n and n.get("href"):
        return urljoin(page_url, n["href"])
    candidates = soup.find_all("a", string=re.compile(r"\b(Next|Older|More)\b", re.I))
    for a in candidates:
        href = a.get("href")
        if href:
            return urljoin(page_url, href)
    # Yelp fallback: synthesize ?start= offsets (20 per page)
    u = urlparse(page_url)
    if "yelp.com" in u.netloc and "/biz/" in u.path:
        q = parse_qs(u.query)
        start = int(q.get("start", ["0"])[0])
        next_start = start + 20
        q["start"] = [str(next_start)]
        new_query = urlencode({k: v[0] for k, v in q.items()})
        return urlunparse((u.scheme, u.netloc, u.path, u.params, new_query, u.fragment))
    return None

# ------------------- saving -------------------

def save_json(reviews: List[Review], path: str):
    with open(path, "w", encoding="utf-8") as f:
        json.dump([asdict(r) for r in reviews], f, ensure_ascii=False, indent=2)

def save_csv(reviews: List[Review], path: str):
    with open(path, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["reviewer","rating","date","text","source_url"])
        w.writeheader()
        for r in reviews:
            w.writerow(asdict(r))

# ------------------- main -------------------

def main():
    p = argparse.ArgumentParser(description="Multi-page review scraper (BeautifulSoup) with optional offline parsing.")
    p.add_argument("start_url", help="Listing/reviews page URL to start from (ignored if --offline_files is used).")
    p.add_argument("--pages", type=int, default=3, help="How many pages to scrape when online (default: 3).")
    p.add_argument("--delay", type=float, default=1.0, help="Delay (seconds) between requests (default: 1.0).")
    p.add_argument("--out", default="data.json", help="Output file path (default: data.json).")
    p.add_argument("--format", choices=["json","csv"], default="json", help="Output format (default: json).")

    # selectors
    p.add_argument("--review_container", required=True, help="CSS selector for each review card.")
    p.add_argument("--sel_reviewer", default="", help="CSS inside card for reviewer.")
    p.add_argument("--sel_rating", default="", help="CSS inside card for rating.")
    p.add_argument("--sel_date", default="", help="CSS inside card for date.")
    p.add_argument("--sel_text", default="", help="CSS inside card for review text.")
    p.add_argument("--sel_next", default="", help="CSS selector for the 'next page' link (online mode).")

    # anti-block (online mode)
    p.add_argument("--headers", default="headers_ua.txt", help="Path to headers file (optional).")
    p.add_argument("--cookies", default="cookies.txt", help="Path to cookies file (optional).")

    # offline mode
    p.add_argument("--offline_files", nargs="*", default=[],
                   help="Parse these local HTML files instead of fetching online (supports globs like yelp_p*.html).")
    p.add_argument("--offline_base", default="",
                   help="Base URL to record in source_url for offline files (e.g., the real listing URL).")

    args = p.parse_args()

    selectors = {
        "review_container": args.review_container,
        "reviewer": args.sel_reviewer,
        "rating": args.sel_rating,
        "date": args.sel_date,
        "text": args.sel_text,
    }

    all_reviews: List[Review] = []

    # ---------- OFFLINE MODE ----------
    if args.offline_files:
        expanded = []
        for pat in args.offline_files:
            expanded += glob.glob(pat)
        if not expanded:
            sys.stderr.write("[warn] no offline files matched; exiting.\n")
        else:
            sys.stderr.write(f"[info] parsing {len(expanded)} offline files...\n")
            for fp in expanded:
                try:
                    html = open(fp, encoding="utf-8").read()
                except Exception as e:
                    sys.stderr.write(f"[warn] could not read {fp}: {e}\n")
                    continue

                source_url = f"file:///{os.path.abspath(fp).replace(os.sep,'/')}"
                if args.offline_base:
                    # If filename ends with a number N, guess offset (N-1)*20
                    offset = 0
                    m = re.search(r'(\d+)\D*$', os.path.splitext(os.path.basename(fp))[0])
                    if m:
                        try:
                            n = int(m.group(1))
                            offset = max(0, (n - 1) * 20)
                        except ValueError:
                            offset = 0
                    u = urlparse(args.offline_base)
                    q = parse_qs(u.query)
                    q["start"] = [str(offset)]
                    new_query = urlencode({k: v[0] for k, v in q.items()})
                    source_url = urlunparse((u.scheme, u.netloc, u.path, u.params, new_query, u.fragment))

                page_reviews = parse_reviews(html, source_url, selectors)
                sys.stderr.write(f"[info] parsed {len(page_reviews)} reviews from {fp}.\n")
                all_reviews.extend(page_reviews)

    # ---------- ONLINE MODE ----------
    else:
        session = make_session(args.headers, args.cookies)
        url = args.start_url
        seen_urls = set()
        for page_idx in range(1, args.pages + 1):
            if not url or url in seen_urls:
                sys.stderr.write("[info] no new page or repeated page; stopping.\n")
                break
            seen_urls.add(url)

            html = fetch(session, url)
            if not html:
                sys.stderr.write(f"[warn] failed to fetch page {page_idx}: {url}\n")
                break

            page_reviews = parse_reviews(html, url, selectors)
            sys.stderr.write(f"[info] parsed {len(page_reviews)} reviews from page {page_idx}.\n")
            all_reviews.extend(page_reviews)

            next_url = find_next_url(html, url, args.sel_next or None)
            if not next_url:
                sys.stderr.write("[info] no next page link found; stopping.\n")
                break

            time.sleep(args.delay + random.uniform(0, 0.6))
            url = next_url

    # ---------- save ----------
    if len(all_reviews) < 15:
        sys.stderr.write(f"[info] collected {len(all_reviews)} reviews (<15). That is acceptable if the site has fewer or pagination ends early.\n")

    if args.format == "json":
        save_json(all_reviews, args.out)
    else:
        save_csv(all_reviews, args.out)

    print(f"Saved {len(all_reviews)} reviews -> {args.out}")

if __name__ == "__main__":
    main()

