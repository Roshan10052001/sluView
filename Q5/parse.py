
from bs4 import BeautifulSoup
import csv, os, re

INPUTS = [
    "listing_rendered.html",         
    "PINT SIZE BAKERY & COFFEE - Updated September 2025 - 458 Photos & 277 Reviews - 3133 Watson Rd, Saint Louis, Missouri - Bakeries - Phone Number - Yelp.html",
    "listing_ua.html",
    "listing.html",
]
OUTCSV = "parsed.csv"

DATE_RE = re.compile(r"([A-Z][a-z]{2}\s+\d{1,2},\s+\d{4})")
def clean_space(s): return re.sub(r"\s+", " ", (s or "").strip())

def read_html():
    for p in INPUTS:
        if os.path.exists(p):
            with open(p, "r", encoding="utf-8", errors="ignore") as f:
                html = f.read()
            print(f"[info] using {p} ({len(html)} bytes)")
            return html, p
    print("[error] no HTML file found"); return "", ""

def text(el): return el.get_text(" ", strip=True) if el else ""

def first(soup, selectors):
    for sel in selectors:
        el = soup.select_one(sel)
        if el: return el
    return None

def business_fields(soup):
    name = clean_space(text(first(soup, ["h1", "header h1"])))

    rating_el = first(soup, ["[aria-label*='star rating']", "div[role='img'][aria-label]"])
    overall = clean_space(rating_el.get("aria-label") if rating_el and rating_el.has_attr("aria-label") else text(rating_el))

    total = ""
    m = re.search(r"\b([\d,]+)\s+reviews?\b", soup.get_text(" ", strip=True), re.I)
    if m: total = m.group(1)

    price = ""
    for el in soup.select("header span, header div, main span, main div")[:1500]:
        t = text(el)
        if t in {"$", "$$", "$$$", "$$$$"}:
            price = t; break

    cats = []
    for a in soup.select("header a, nav a, main a"):
        t = clean_space(text(a))
        if 1 <= len(t) <= 40 and re.search(r"[A-Za-z]", t):
            if all(x not in t.lower() for x in ["yelp", "login", "write", "directions", "call", "edit"]):
                cats.append(t)
        if len(cats) >= 3: break
    category = ", ".join(dict.fromkeys(cats))

    city = clean_space(text(first(soup, ["address"])))
    if not city:
        page = soup.get_text(" ", strip=True)
        for c in ["Saint Louis, MO 63139","Saint Louis, MO","St. Louis, MO","Saint Louis"]:
            if c in page: city = c; break

    print(f"[info] business: {name}")
    return name, category, city, price, overall, total

def review_containers(soup):
    sec = first(soup, ['section[aria-label="Recommended Reviews"]', 'section[aria-label="reviews"]'])
    root = sec if sec else soup
    cards = root.select('article, li, div[data-review-id], div[class*="review"]')
    if not cards:
        cards = root.select('section[aria-label*="Review"] div, ul li')
    print(f"[debug] fallback containers: {len(cards)}")
    return cards

def parse_card(c):
    # reviewer
    reviewer_el = c.select_one('a[href*="/user_details"], [data-testid="reviewer-name"], a[aria-label*="profile"]')
    reviewer = clean_space(text(reviewer_el))

    # date 
    t = c.select_one("time") or c.select_one('[data-testid="review-date"]') or c.select_one('span[class*="date"], div[class*="date"]')
    if t and t.has_attr("datetime"):
        review_date = t["datetime"]
    else:
        tx = clean_space(text(t)); m = DATE_RE.search(tx); review_date = m.group(1) if m else tx

   
    stars_el = c.select_one('[aria-label*="star rating"], div[role="img"][aria-label*="star rating"]')
    review_rating = clean_space(stars_el["aria-label"] if stars_el and stars_el.has_attr("aria-label") else text(stars_el))

    
    text_el = c.select_one(
        'p[class^="comment__"], p[class*="comment__"], span[class*="raw"], div[data-test-id*="expanded-review"], p'
    )
    body = clean_space(text_el.get_text(" ", strip=True) if text_el else "")

    low = body.lower()
    if ((("am" in low and "pm" in low and "-" in low) or low in {"closed", "open"} or low.startswith("q:") or len(low) < 15)):
        body = ""

    return reviewer, review_date, review_rating, body

def main():
    html, used = read_html()
    if not html: return
    soup = BeautifulSoup(html, "lxml")

    name, category, city, price, overall, total = business_fields(soup)

    rows = []
    for card in review_containers(soup):
        rn, rd, rr, rt = parse_card(card)
        if rt:  # only keep cards with real review text
            rows.append({
                "business_name":  name,
                "category":       category,
                "city":           city,
                "price_range":    price,
                "overall_rating": overall,
                "total_reviews":  total,
                "reviewer_name":  rn,
                "review_date":    rd,
                "review_rating":  rr,
                "review_text":    rt,
            })
        if len(rows) >= 50:
            break

    if not rows:
        print("[warn] 0 reviews parsed — check you opened the browser-saved HTML.")
    else:
        with open(OUTCSV, "w", newline="", encoding="utf-8") as f:
            cols = ["business_name","category","city","price_range","overall_rating","total_reviews",
                    "reviewer_name","review_date","review_rating","review_text"]
            w = csv.DictWriter(f, fieldnames=cols); w.writeheader(); w.writerows(rows)
        print(f"[done] wrote {len(rows)} rows to {OUTCSV} (source used: {used})")

if __name__ == "__main__":
    main()
