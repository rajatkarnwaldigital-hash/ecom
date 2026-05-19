"""
ecomm_sniper.py
EthicalSEO Outbound Signal Engine — Stage 2

Takes qualified companies and produces campaign-ready contacts with
personalised hooks based on real SEO signals detected on each website.

What it does per company:
  1. Detects up to 4 SEO signals (see SIGNALS section below)
  2. Validates keyword and competitor data through Claude Haiku
  3. Writes personalised hook with Claude Sonnet
  4. Post-processes output (strips www., catches hallucinations)

SIGNALS DETECTED:
  page2       — ranking positions 11-20 for a commercial keyword (CPC >= $2)
                 that does not appear on the ranking page
  schema      — missing product/breadcrumb structured data while a competitor
                 shows prices and star ratings in Google results
  speed       — mobile PageSpeed score < 65 while a competitor scores higher
  weak_title  — collection/category page has a generic title like "Collections"
                 that gives Google no ranking signal
  gmc         — company does not appear in Google Shopping / Merchant Center
                 for their own product keywords (UK/Western Europe only,
                 not applicable for Baltics)

KEYWORD VALIDATION (Claude Haiku):
  Only generic category/product terms are used in hooks.
  Rejects: brand names, designer names, person names, specific product models,
  unrelated topics.
  Example: "mens running shoes" passes. "Nike Air Max" fails.

COMPETITOR VALIDATION (Claude Haiku):
  Only actual ecommerce retailers are used as competitors.
  Rejects: brand manufacturers, news sites, social platforms, marketplaces.
  Example: "houseoffraser.co.uk" passes for a fashion brand. "vogue.com" fails.

FALLBACK:
  When no valid keyword/competitor can be found, the hook references the
  company's top organic competitor (domain_organic_organic) without naming
  a specific keyword. No data is ever invented.

INPUT:  qualified.csv (output from qualify.py) + contacts CSV from Apollo/Snov
        Contacts CSV must have columns: first_name, last_name, email,
        linkedin_url, job_title, company_name, company_url
OUTPUT: campaign_ready.csv (see sample_output_sniper.csv)

Run:
  python3 ecomm_sniper.py --qualified qualified.csv --contacts contacts.csv --output campaign_ready.csv

  Or edit INPUT_QUALIFIED / INPUT_CONTACTS / OUTPUT_FILE below and run:
  python3 ecomm_sniper.py
"""

import argparse
import hashlib
import json
import os
import re
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

import anthropic
import pandas as pd
import requests
from bs4 import BeautifulSoup

# ── CONFIG — edit these before running ───────────────────────────────────────
SEMRUSH_API_KEY   = "YOUR_SEMRUSH_API_KEY"
ANTHROPIC_API_KEY = "YOUR_ANTHROPIC_API_KEY"
PAGESPEED_API_KEY = "YOUR_PAGESPEED_API_KEY"  # free from Google Cloud Console

INPUT_QUALIFIED   = "qualified.csv"
INPUT_CONTACTS    = "contacts.csv"
OUTPUT_FILE       = "campaign_ready.csv"
CHECKPOINT_FILE   = "sniper_checkpoint.json"

# Model config
HAIKU_MODEL       = "claude-haiku-4-5-20251001"   # validation (fast + cheap)
SONNET_MODEL      = "claude-sonnet-4-6"             # hook writing (quality)

# Performance
MAX_WORKERS       = 5
SEMRUSH_RPS       = 8
CHECKPOINT_EVERY  = 50

# Signal thresholds
MIN_CPC           = 2.00    # minimum CPC for page 2 keyword signal
MIN_CPC_SCHEMA    = 0.50    # minimum CPC for schema keyword lookup
MIN_VOLUME        = 200     # minimum monthly searches
PAGE2_MIN_POS     = 11      # page 2 starts at position 11
PAGE2_MAX_POS     = 20      # page 2 ends at position 20
PAGESPEED_POOR    = 65      # mobile score below this = signal
PAGESPEED_GAP_MIN = 10      # competitor must be this much faster

# Countries where Google Shopping / GMC is not available
# Don't run GMC check for companies in these countries
GMC_SKIP_COUNTRIES = {"estonia", "latvia", "lithuania", "et", "lv", "lt"}

# Domains to always skip as competitors
SKIP_COMPETITORS = [
    "amazon", "ebay", "etsy", "google", "wikipedia", "pinterest",
    "instagram", "facebook", "twitter", "youtube", "tiktok",
    "espn", "bbc", "dailymail", "theguardian", "reddit",
    "tripadvisor", "trustpilot",
]

# Junk keyword prefixes to filter before Haiku validation
JUNK_PREFIXES = [
    "what is", "how to", "how do", "why", "when", "where", "who",
    "best way", "can i", "should i",
]

# CTA variants — assigned by domain hash so consistent per contact,
# varied across the full list
CTAS = [
    "Want me to show you how to fix it?",
    "Happy to walk you through it if useful.",
    "Worth a quick chat about it?",
    "Let me know if you want me to take a look.",
    "Can show you exactly what to change if you're interested.",
]

# ── RATE LIMITER ──────────────────────────────────────────────────────────────
class RateLimiter:
    """Thread-safe rate limiter for SEMrush API calls."""
    def __init__(self, rps):
        self._lock = threading.Lock()
        self._last = 0.0
        self._gap  = 1.0 / rps

    def acquire(self):
        with self._lock:
            wait = self._gap - (time.monotonic() - self._last)
            if wait > 0:
                time.sleep(wait)
            self._last = time.monotonic()

_semrush = RateLimiter(SEMRUSH_RPS)
_output_lock  = threading.Lock()
_counter_lock = threading.Lock()
_processed    = set()
_counters     = {"hooks": 0, "fallback": 0, "no_signal": 0, "errors": 0, "done": 0}

# ── CHECKPOINT ────────────────────────────────────────────────────────────────
def load_checkpoint():
    if not os.path.exists(CHECKPOINT_FILE):
        return set()
    try:
        with open(CHECKPOINT_FILE) as f:
            return set(json.load(f).get("done", []))
    except Exception:
        return set()

def save_checkpoint():
    with open(CHECKPOINT_FILE, "w") as f:
        json.dump({"done": list(_processed)}, f)

def mark_done(key, counter_key, total):
    _processed.add(key)
    with _counter_lock:
        _counters[counter_key] += 1
        _counters["done"]      += 1
        done = _counters["done"]
    if done % CHECKPOINT_EVERY == 0:
        save_checkpoint()
        print(f"\n  [checkpoint] {done}/{total} processed\n")

# ── SEMRUSH HELPERS ───────────────────────────────────────────────────────────
def _clean(val):
    return str(val).strip().strip('"')

def semrush_get(params, label=""):
    """Make a SEMrush API call with rate limiting. Returns list of row dicts."""
    _semrush.acquire()
    try:
        r = requests.get("https://api.semrush.com/", params=params, timeout=30)
        r.raise_for_status()
        lines = r.text.strip().splitlines()
        if len(lines) < 2:
            return []
        headers = [_clean(h) for h in lines[0].split(";")]
        return [
            {headers[i]: _clean(vals[i]) for i in range(len(headers))}
            for line in lines[1:]
            if len(vals := line.split(";")) == len(headers)
        ]
    except Exception as e:
        if label:
            print(f"    [SEMrush error] {label}: {e}")
        return []

def get_domain_keywords(domain, database="uk", limit=30, page2_only=False):
    """
    Get organic keywords for a domain.
    If page2_only=True, returns only positions 11-20 with CPC >= MIN_CPC.
    Otherwise returns all keywords with CPC >= MIN_CPC_SCHEMA.
    """
    rows = semrush_get({
        "type":           "domain_organic",
        "key":            SEMRUSH_API_KEY,
        "domain":         domain,
        "database":       database,
        "display_limit":  limit,
        "display_sort":   "nq_desc",
        "export_columns": "Ph,Po,Ur,Cp,Nq",
        "export_escape":  1,
    }, label=f"domain_organic:{domain}")

    results = []
    min_cpc = MIN_CPC if page2_only else MIN_CPC_SCHEMA
    for row in rows:
        try:
            pos = int(float(row.get("Position", 0) or 0))
            cpc = float(row.get("CPC", 0) or 0)
            vol = int(float(row.get("Search Volume", 0) or 0))
            kw  = row.get("Keyword", "").strip()
            url = row.get("Url", "").strip()
            if not kw or cpc < min_cpc or vol < MIN_VOLUME or len(kw.split()) < 2:
                continue
            if page2_only and not (PAGE2_MIN_POS <= pos <= PAGE2_MAX_POS):
                continue
            if not page2_only and pos < 1:
                continue
            results.append({
                "keyword":  kw,
                "position": pos,
                "cpc":      round(cpc, 2),
                "volume":   vol,
                "url":      url,
                "database": database,
            })
        except Exception:
            continue
    return results

def get_url_keywords(page_url, database="uk", limit=10):
    """Get keywords a specific URL is ranking for."""
    rows = semrush_get({
        "type":           "url_organic",
        "key":            SEMRUSH_API_KEY,
        "url":            page_url,
        "database":       database,
        "display_limit":  limit,
        "display_sort":   "nq_desc",
        "export_columns": "Ph,Po,Cp,Nq",
        "export_escape":  1,
    }, label=f"url_organic:{page_url}")

    results = []
    for row in rows:
        try:
            pos = int(float(row.get("Position", 0) or 0))
            vol = int(float(row.get("Search Volume", 0) or 0))
            kw  = row.get("Keyword", "").strip()
            if kw and pos >= 1 and len(kw.split()) >= 2:
                results.append({
                    "keyword":  kw,
                    "position": pos,
                    "cpc":      round(float(row.get("CPC", 0) or 0), 2),
                    "volume":   vol,
                })
        except Exception:
            continue
    return results

def get_keyword_competitors(keyword, domain, database="uk"):
    """Get top domains ranking for a keyword, excluding the target domain."""
    rows = semrush_get({
        "type":           "phrase_organic",
        "key":            SEMRUSH_API_KEY,
        "phrase":         keyword,
        "database":       database,
        "display_limit":  8,
        "export_columns": "Dn,Po",
        "export_escape":  1,
    }, label=f"phrase_organic:{keyword}")

    return [
        _clean(row.get("Domain", ""))
        for row in rows
        if (
            _clean(row.get("Domain", ""))
            and domain not in _clean(row.get("Domain", ""))
            and _clean(row.get("Domain", "")) not in domain
            and not any(s in _clean(row.get("Domain", "")) for s in SKIP_COMPETITORS)
        )
    ]

def get_domain_competitors(domain, database="uk"):
    """Get organic competitor domains via domain_organic_organic."""
    rows = semrush_get({
        "type":           "domain_organic_organic",
        "key":            SEMRUSH_API_KEY,
        "domain":         domain,
        "database":       database,
        "display_limit":  10,
        "export_columns": "Dn,Cr,Or",
        "export_escape":  1,
    }, label=f"competitors:{domain}")

    return [
        _clean(r.get("Domain", ""))
        for r in rows
        if _clean(r.get("Domain", ""))
        and not any(s in _clean(r.get("Domain", "")) for s in SKIP_COMPETITORS)
    ]

# ── PAGESPEED ─────────────────────────────────────────────────────────────────
def get_pagespeed_score(url):
    """Returns mobile performance score (0-100) or None."""
    try:
        params = {"url": url, "strategy": "mobile"}
        if PAGESPEED_API_KEY and PAGESPEED_API_KEY != "YOUR_PAGESPEED_API_KEY":
            params["key"] = PAGESPEED_API_KEY
        r    = requests.get(
            "https://www.googleapis.com/pagespeedonline/v5/runPagespeed",
            params=params, timeout=30
        )
        r.raise_for_status()
        score = r.json()["lighthouseResult"]["categories"]["performance"]["score"]
        return int(score * 100)
    except Exception:
        return None

# ── HAIKU VALIDATORS ──────────────────────────────────────────────────────────
def validate_keyword(client, company_name, domain, keywords):
    """
    Uses Haiku to pick the best generic category keyword from a list.

    Accepts: generic product/category terms a customer uses without knowing the brand.
    Rejects: brand names, designer names, person names, unrelated topics.

    Returns the best keyword dict or None.
    """
    if not keywords:
        return None

    kw_list = "\n".join(
        f"{i+1}. \"{kw['keyword']}\" (vol:{kw.get('volume',0):,}, "
        f"cpc:${kw.get('cpc',0)}, pos:{kw.get('position','')})"
        for i, kw in enumerate(keywords[:15])
    )

    prompt = f"""You are validating keywords for a cold outreach hook for an SEO agency.

Company: {company_name} ({domain})

Pick the BEST keyword from this list. The keyword should describe a product type or category this company sells.

GOOD keywords — include these:
- Generic product types: "coffee beans", "high quality coffee beans", "duffel bag", "mens watches"
- Category terms: "luxury candles uk", "organic skincare", "running shoes"
- Descriptive product terms: "cashmere jumper", "solar lights outdoor", "leather wallet mens"
- Buying intent: terms with "buy", "shop", "best", "uk", "online"

BAD keywords — reject these:
- The company's own brand name or obvious variation of it
- Another specific brand name or designer name (e.g. "omega watches", "nike trainers")
- A celebrity or person's name
- A completely unrelated topic (e.g. "dinosaur facts" for a clothing brand)
- Pure navigational queries

Be LENIENT — if the keyword is even loosely related to what the company sells, include it.
"High quality coffee beans" is GOOD for a coffee company.
"Mens leather boots" is GOOD for a footwear retailer.

Keywords:
{kw_list}

Reply ONLY with JSON: {{"index": <1-based number>, "reason": "<one sentence>"}}
If NO keyword qualifies: {{"index": 0, "reason": "<why>"}}"""

    try:
        msg  = client.messages.create(
            model=HAIKU_MODEL, max_tokens=120,
            messages=[{"role": "user", "content": prompt}]
        )
        raw  = msg.content[0].text.strip().replace("```json", "").replace("```", "")
        data = json.loads(re.search(r"\{[^}]+\}", raw, re.DOTALL).group())
        idx  = int(data["index"]) - 1
        return keywords[idx] if 0 <= idx < len(keywords) else None
    except Exception:
        return None

def validate_competitor(client, company_name, domain, keyword, competitors):
    """
    Uses Haiku to validate a competitor is a real ecommerce retailer.

    Accepts: retailers selling similar products the CEO would recognise as competition.
    Rejects: manufacturers, brand owners, news sites, social platforms, marketplaces.

    Returns competitor domain string or None.
    """
    if not competitors:
        return None

    comp_list = "\n".join(f"{i+1}. {c}" for i, c in enumerate(competitors[:8]))

    prompt = f"""You are validating a competitor for a cold outreach hook.

Company: {company_name} ({domain})
Keyword: "{keyword}"

Pick the competitor that is a legitimate ecommerce RETAILER selling similar products.

The competitor MUST be:
- An ecommerce store or retailer selling to end consumers
- Selling similar products to {company_name}
- A company the CEO of {company_name} would recognise as a direct competitor

The competitor MUST NOT be:
- The manufacturer or brand owner of the product in the keyword
- A news site, blog, or content site
- A social media platform
- A marketplace (Amazon, eBay, Etsy)
- Completely unrelated to {company_name}'s business

Competitors:
{comp_list}

Reply ONLY with JSON: {{"index": <1-based number>, "reason": "<one sentence>"}}
If NONE qualify: {{"index": 0, "reason": "<why>"}}"""

    try:
        msg  = client.messages.create(
            model=HAIKU_MODEL, max_tokens=120,
            messages=[{"role": "user", "content": prompt}]
        )
        raw  = msg.content[0].text.strip().replace("```json", "").replace("```", "")
        data = json.loads(re.search(r"\{[^}]+\}", raw, re.DOTALL).group())
        idx  = int(data["index"]) - 1
        return competitors[idx] if 0 <= idx < len(competitors) else None
    except Exception:
        return None

# ── KEYWORD + COMPETITOR FINDERS ──────────────────────────────────────────────
def find_valid_keyword(client, company_name, domain, databases=("uk", "us")):
    """Find a valid generic keyword, trying UK database first then US."""
    for db in databases:
        candidates = get_domain_keywords(domain, database=db, limit=30)
        filtered   = [
            k for k in candidates
            if not any(k["keyword"].lower().startswith(p) for p in JUNK_PREFIXES)
        ]
        if not filtered:
            continue
        best = validate_keyword(client, company_name, domain, filtered)
        if best:
            best["database"] = db
            return best
    return None

def find_valid_page2_keyword(client, company_name, domain, databases=("uk", "us")):
    """Find a valid page 2 keyword (positions 11-20, CPC >= $2)."""
    for db in databases:
        page2 = get_domain_keywords(domain, database=db, limit=40, page2_only=True)
        if not page2:
            continue
        best = validate_keyword(client, company_name, domain, page2)
        if best:
            best["database"] = db
            return best
    return None

def find_valid_url_keyword(client, company_name, domain, page_url, databases=("uk", "us")):
    """Find a valid keyword that a specific URL ranks for."""
    if not page_url or page_url in ("nan", ""):
        return None
    for db in databases:
        candidates = get_url_keywords(page_url, database=db, limit=15)
        if not candidates:
            continue
        best = validate_keyword(client, company_name, domain, candidates)
        if best:
            best["database"] = db
            return best
    return None

def find_valid_competitor(client, company_name, domain, keyword, database="uk"):
    """Find a valid competitor for a given keyword."""
    comps = get_keyword_competitors(keyword, domain, database=database)
    return validate_competitor(client, company_name, domain, keyword, comps) if comps else None

def find_fallback_competitor(client, company_name, domain, databases=("uk", "us")):
    """
    Find the top organic competitor when no keyword signal is available.
    Uses domain_organic_organic which gives overall organic competition.
    """
    for db in databases:
        comps = get_domain_competitors(domain, database=db)
        if not comps:
            continue
        valid = validate_competitor(client, company_name, domain, "general products", comps)
        if valid:
            return valid
    return None

# ── SIGNAL DETECTION ──────────────────────────────────────────────────────────
def detect_page2_signal(client, company_name, domain):
    """
    Detects page 2 keyword signal.
    Returns bullet string and metadata dict, or (None, None).
    """
    kw = find_valid_page2_keyword(client, company_name, domain)
    if not kw:
        return None, None

    # Check if keyword appears on the ranking page
    kw_absent = False
    if kw.get("url"):
        try:
            r    = requests.get(kw["url"], timeout=15, allow_redirects=True,
                                headers={"User-Agent": "Mozilla/5.0"})
            text = BeautifulSoup(r.text, "html.parser").get_text(
                separator=" ", strip=True
            ).lower()
            kw_absent = kw["keyword"].lower() not in text
        except Exception:
            pass

    if kw_absent:
        page_url_str = kw.get("url", "").replace("https://www.", "").replace("https://", "")
        bullet = (
            f"You're at position {kw['position']} for \"{kw['keyword']}\" "
            f"({kw['volume']:,} searches/month, ${kw['cpc']} CPC) but the word doesn't appear "
            f"anywhere on {page_url_str} — easy fix that could move you to page 1."
        )
    else:
        page_url_str = kw.get("url", "").replace("https://www.", "").replace("https://", "")
        bullet = (
            f"You're at position {kw['position']} for \"{kw['keyword']}\" "
            f"({kw['volume']:,} searches/month, ${kw['cpc']} CPC) — "
            f"just one page away from a lot more traffic. Page: {page_url_str}"
        )

    return bullet, kw

def detect_schema_signal(client, company_name, domain):
    """
    Detects missing schema signal.
    Returns bullet string and metadata dict, or (None, None).
    """
    kw = find_valid_keyword(client, company_name, domain)
    if not kw:
        return None, None

    competitor = find_valid_competitor(
        client, company_name, domain, kw["keyword"],
        database=kw.get("database", "uk")
    )
    if not competitor:
        return None, None

    # Strip www. prefix
    competitor = re.sub(r"^www\.", "", competitor)

    bullet = (
        f"{competitor} shows up in Google with prices and star ratings for "
        f"\"{kw['keyword']}\" ({kw['volume']:,} searches/month) — "
        f"{company_name} shows a plain link. Same search, but their result gets more clicks. "
        f"You can close that gap with a quick schema fix."
    )
    return bullet, {"keyword": kw["keyword"], "competitor": competitor,
                    "volume": kw["volume"], "cpc": kw["cpc"]}

def detect_speed_signal(domain):
    """
    Detects site speed signal using PageSpeed API.
    Returns bullet string and metadata dict, or (None, None).

    Fires if: mobile score < PAGESPEED_POOR (65) AND either
    a competitor scores >= PAGESPEED_GAP_MIN better, or score < 40.
    """
    target_url   = f"https://www.{domain}"
    target_score = get_pagespeed_score(target_url)

    if target_score is None or target_score >= PAGESPEED_POOR:
        return None, None

    # Try to find a competitor for comparison
    competitor       = None
    competitor_score = None
    comps = get_keyword_competitors("buy online uk", domain, database="uk")
    if comps:
        competitor       = re.sub(r"^www\.", "", comps[0])
        competitor_score = get_pagespeed_score(f"https://www.{competitor}")

    if competitor_score is not None:
        gap = competitor_score - target_score
        if gap < PAGESPEED_GAP_MIN and target_score >= 40:
            return None, None

    comp_str  = competitor or "top competitors in your space"
    score_str = f"{competitor_score}/100" if competitor_score else "significantly higher"

    bullet = (
        f"Your site scores {target_score}/100 on mobile speed — "
        f"{comp_str} scores {score_str}. "
        f"Slow mobile pages lose ~53% of visitors before a page even loads."
    )
    return bullet, {"score": target_score, "competitor": comp_str,
                    "competitor_score": competitor_score}

def detect_weak_title_signal(client, company_name, domain, page_url, page_title):
    """
    Detects weak category page title signal.

    Uses url_organic to find what the page itself ranks for.
    Falls back to domain_organic if url_organic returns nothing.
    Returns bullet string and metadata dict, or (None, None).
    """
    if not page_url or not page_title or page_title in ("nan", "None", ""):
        return None, None

    # Try page-level keywords first, then domain-level
    kw = find_valid_url_keyword(client, company_name, domain, page_url)
    if not kw:
        kw = find_valid_keyword(client, company_name, domain)
    if not kw:
        return None, None

    path   = re.sub(r"https?://(www\.)?", "", page_url).rstrip("/")
    bullet = (
        f"{path} is titled \"{page_title}\" — "
        f"a page that should be targeting \"{kw['keyword']}\" "
        f"({kw['volume']:,} searches/month) is giving Google nothing to rank it on."
    )
    return bullet, {"keyword": kw["keyword"], "volume": kw["volume"],
                    "url": page_url, "title": page_title}

def detect_gmc_signal(domain, country=""):
    """
    Detects whether a company appears in Google Shopping (Merchant Center).

    Google Shopping = the product listings with images, prices and ratings
    that appear at the top of search results and in Google's AI answers.
    Not being in GMC means missing from Google Shopping entirely AND from
    AI-generated product recommendations.

    Method: uses SEMrush shopping_performance to check if the domain has
    any Google Shopping impressions. If zero shopping keywords, they are
    almost certainly not in GMC.

    NOT applicable for Baltic countries (Estonia, Latvia, Lithuania) —
    Google Shopping is not available there.

    Returns bullet string and metadata dict, or (None, None).
    """
    # Skip Baltic countries
    if country and country.lower().strip() in GMC_SKIP_COUNTRIES:
        return None, None

    # Check SEMrush for shopping keywords
    rows = semrush_get({
        "type":           "domain_shopping",
        "key":            SEMRUSH_API_KEY,
        "domain":         domain,
        "database":       "uk",
        "display_limit":  5,
        "export_columns": "Ph,Po,Nq",
        "export_escape":  1,
    }, label=f"domain_shopping:{domain}")

    if rows:
        # Company has Shopping presence — signal does not fire
        return None, None

    # No shopping keywords — company is likely not in GMC
    # Confirm they have organic presence (qualify.py already checked this,
    # but double-check they're actually selling products)
    bullet = (
        f"{domain} doesn't appear in Google Shopping — meaning no product listings "
        f"with prices and images show up when people search for what you sell. "
        f"Google Shopping is also how brands appear in AI search results now. "
        f"Getting into Merchant Center could open up a whole new traffic source."
    )
    return bullet, {"gmc_present": False}


def write_hook(client, company_name, domain, signals):
    """
    Writes the full personalised hook using Claude Sonnet.
    signals: list of {"type": str, "bullet": str}
    """
    bullets = "\n".join(f"- {s['bullet']}" for s in signals)
    opener  = "Noticed a couple of things" if len(signals) > 1 else "Noticed something"
    cta     = CTAS[int(hashlib.md5(domain.encode()).hexdigest(), 16) % len(CTAS)]

    prompt = f"""Write a short cold outreach message for EthicalSEO. Tone: casual, friendly, simple. Like a founder texting another founder, not a consultant writing a report.

Company: {company_name} ({domain})

Signals:
{bullets}

Write the message:
1. Open with: "{opener} on {company_name}:"
2. Each signal as one short bullet — keep it simple and specific, reference the real numbers
3. Close with: "{cta}"

Rules:
- Simple everyday language — if you wouldn't say it out loud to a friend, cut it
- No fancy phrases like "bleeding traffic", "earned the right to", "genuinely competitive"
- No SEO jargon
- No sign-off, no subject line, no "— Konstantin"
- Short and punchy — under 100 words total
- Output ONLY the message"""

    try:
        msg = client.messages.create(
            model=SONNET_MODEL, max_tokens=300,
            messages=[{"role": "user", "content": prompt}]
        )
        return msg.content[0].text.strip()
    except Exception as e:
        return f"[hook error: {e}]"

def write_fallback_hook(client, company_name, domain, competitor):
    """
    Fallback hook when no valid keyword/competitor found via signals.
    Names the top organic competitor without requiring a specific keyword.
    Never invents data.
    """
    cta      = CTAS[int(hashlib.md5(domain.encode()).hexdigest(), 16) % len(CTAS)]
    comp_str = re.sub(r"^www\.", "", competitor) if competitor else None

    if not comp_str:
        prompt = f"""Write a short cold outreach message for EthicalSEO. Tone: casual, friendly, simple. Like a founder texting another founder.

Company: {company_name} ({domain})

Write the message:
1. Open: "Noticed something on {company_name}:"
2. One bullet: their product pages don't have schema markup — competitors show up in Google with prices and star ratings, {company_name} shows a plain link. Same search results page, but the richer result always gets more clicks.
3. CTA: "{cta}"

Rules: simple language, no jargon, no sign-off, no subject line, under 70 words. Output ONLY the message."""
    else:
        prompt = f"""Write a short cold outreach message for EthicalSEO. Tone: casual, friendly, simple. Like a founder texting another founder.

Company: {company_name} ({domain})
Top competitor: {comp_str}

Write the message:
1. Open: "Noticed something on {company_name}:"
2. One bullet: {comp_str} shows up in Google with prices and star ratings on the same searches where {company_name} shows a plain link. Same search intent, but their result attracts more clicks. You can add 5-10% more commercial traffic just by fixing that.
3. CTA: "{cta}"

Rules:
- Name the actual competitor
- Simple language, no jargon, no sign-off, no subject line
- Under 70 words
- Output ONLY the message"""

    try:
        msg = client.messages.create(
            model=SONNET_MODEL, max_tokens=200,
            messages=[{"role": "user", "content": prompt}]
        )
        return msg.content[0].text.strip()
    except Exception as e:
        return f"[hook error: {e}]"

# ── POST-PROCESSING ───────────────────────────────────────────────────────────
def post_process(df):
    """
    Cleans the output dataframe:
    - Strips www. prefix from competitor names in hook text and column
    - Flags hooks that may contain invented competitor names
      (competitor column is empty but hook contains a domain-like string)
    """
    # Strip www. from competitor column
    if "competitor" in df.columns:
        df["competitor"] = df["competitor"].str.replace(r"^www\.", "", regex=True)

    # Strip www. from hook text
    df["hook"] = df["hook"].apply(
        lambda h: re.sub(r"\bwww\.", "", str(h)) if pd.notna(h) else h
    )

    # Flag potential hallucinations: competitor is empty but hook has domain-like text
    # These should be reviewed before sending
    def check_hallucination(row):
        comp = str(row.get("competitor", "") or "")
        hook = str(row.get("hook", "") or "")
        if comp in ("", "nan", "None"):
            # Check for domain pattern in hook (e.g. something.com)
            if re.search(r"\b\w+\.(com|co\.uk|net|org)\b", hook):
                return True
        return False

    df["review_flag"] = df.apply(check_hallucination, axis=1)
    flagged = df["review_flag"].sum()
    if flagged > 0:
        print(f"\n  WARNING: {flagged} hooks flagged for review "
              f"(possible hallucinated competitor names). "
              f"Check the review_flag column before sending.\n")

    return df

# ── PER-COMPANY WORKER ────────────────────────────────────────────────────────
def process_company(args):
    idx, total, row, client = args

    company    = str(row.get("company_name", "")).strip()
    domain     = str(row.get("domain", "")).strip()
    domain     = re.sub(r"https?://(www\.)?", "", domain).rstrip("/").split("/")[0]
    weak_url   = str(row.get("sig_weak_title_url", "")   or "").strip()
    weak_title = str(row.get("sig_weak_title_text", "")  or "").strip()
    key        = f"{domain}_{idx}"

    print(f"[{idx}/{total}] {company} ({domain})")

    row     = dict(row)
    signals = []
    meta    = {}
    country = str(row.get("country", "") or "").strip()

    # Run all five signal checks
    # Page 2
    p2_bullet, p2_meta = detect_page2_signal(client, company, domain)
    if p2_bullet:
        signals.append({"type": "page2", "bullet": p2_bullet})
        meta.update({f"page2_{k}": v for k, v in (p2_meta or {}).items()})
        print(f"    ✓ page2: \"{p2_meta.get('keyword','')}\" pos {p2_meta.get('position','')}")

    # Schema
    sc_bullet, sc_meta = detect_schema_signal(client, company, domain)
    if sc_bullet:
        signals.append({"type": "schema", "bullet": sc_bullet})
        meta.update({f"schema_{k}": v for k, v in (sc_meta or {}).items()})
        print(f"    ✓ schema: \"{sc_meta.get('keyword','')}\" vs {sc_meta.get('competitor','')}")

    # Speed
    sp_bullet, sp_meta = detect_speed_signal(domain)
    if sp_bullet:
        signals.append({"type": "speed", "bullet": sp_bullet})
        meta.update({f"speed_{k}": v for k, v in (sp_meta or {}).items()})
        print(f"    ✓ speed: {sp_meta.get('score','')} vs {sp_meta.get('competitor','')}")

    # Weak title
    wt_bullet, wt_meta = detect_weak_title_signal(
        client, company, domain, weak_url, weak_title
    )
    if wt_bullet:
        signals.append({"type": "weak_title", "bullet": wt_bullet})
        meta.update({f"weak_title_{k}": v for k, v in (wt_meta or {}).items()})
        print(f"    ✓ weak_title: \"{wt_meta.get('keyword','')}\"")

    # GMC
    gmc_bullet, gmc_meta = detect_gmc_signal(domain, country)
    if gmc_bullet:
        signals.append({"type": "gmc", "bullet": gmc_bullet})
        meta.update({f"gmc_{k}": v for k, v in (gmc_meta or {}).items()})
        print(f"    ✓ gmc: not in Google Shopping")
    if signals:
        hook       = write_hook(client, company, domain, signals)
        competitor = (
            sc_meta.get("competitor") or
            sp_meta.get("competitor") if sp_meta else None
        )
        row["hook"]             = hook
        row["signals_detected"] = ", ".join(s["type"] for s in signals)
        row["competitor"]       = re.sub(r"^www\.", "", competitor) if competitor else ""
        row.update(meta)
        print(f"    ✓ hook written ({len(signals)} signal(s))")
        mark_done(key, "hooks", total)
    else:
        # Fallback
        print(f"    – no signals, writing fallback...")
        competitor = find_fallback_competitor(client, company, domain)
        hook       = write_fallback_hook(client, company, domain, competitor)
        row["hook"]             = hook
        row["signals_detected"] = "fallback"
        row["competitor"]       = re.sub(r"^www\.", "", competitor) if competitor else ""
        print(f"    ✓ fallback (competitor: {row['competitor'] or 'none found'})")
        mark_done(key, "fallback", total)

    return row

# ── INPUT LOADING + MERGING ───────────────────────────────────────────────────
def load_and_merge(qualified_csv, contacts_csv):
    """
    Loads qualified companies and contacts, merges on domain.
    Returns merged dataframe with one row per contact.
    """
    qualified = pd.read_csv(qualified_csv)
    contacts  = pd.read_csv(contacts_csv)

    def clean_domain(url):
        if pd.isna(url): return ""
        url = re.sub(r"https?://(www\.)?", "", str(url).lower().strip())
        url = url.rstrip("/").split("/")[0]
        parts = url.split(".")
        if len(parts) > 2 and parts[0] in ("uk", "us", "www"):
            url = ".".join(parts[1:])
        return url

    # Normalise domain column in both
    qualified["domain_key"] = qualified["domain"].apply(clean_domain)

    # Try to find domain column in contacts
    for col in ("company_url", "Website", "domain", "Company URL"):
        if col in contacts.columns:
            contacts["domain_key"] = contacts[col].apply(clean_domain)
            break
    else:
        raise ValueError(
            "Contacts CSV must have one of: company_url, Website, domain, Company URL"
        )

    merged = contacts.merge(
        qualified[["domain_key", "domain", "monthly_traffic", "authority_score",
                   "sig_weak_title_url", "sig_weak_title_text"]
                  if "sig_weak_title_url" in qualified.columns
                  else ["domain_key", "domain", "monthly_traffic", "authority_score"]],
        on="domain_key", how="inner"
    )

    print(f"Contacts loaded    : {len(contacts)}")
    print(f"Qualified companies: {len(qualified)}")
    print(f"Merged (matched)   : {len(merged)}")
    return merged

# ── MAIN ──────────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(description="EthicalSEO — ecommerce sniper")
    parser.add_argument("--qualified", default=INPUT_QUALIFIED)
    parser.add_argument("--contacts",  default=INPUT_CONTACTS)
    parser.add_argument("--output",    default=OUTPUT_FILE)
    args = parser.parse_args()

    print("=" * 60)
    print("  EthicalSEO — Ecommerce Sniper")
    print(f"  Qualified : {args.qualified}")
    print(f"  Contacts  : {args.contacts}")
    print(f"  Output    : {args.output}")
    print(f"  Workers   : {MAX_WORKERS} | SEMrush: {SEMRUSH_RPS} req/sec")
    print("=" * 60)

    df    = load_and_merge(args.qualified, args.contacts)
    rows  = df.to_dict("records")
    total = len(rows)
    print(f"\nTotal contacts to process: {total}")

    done_keys = load_checkpoint()
    client    = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
    args_list = [
        (i+1, total, row, client)
        for i, row in enumerate(rows)
        if f"{re.sub(r'https?://(www.)?','',str(row.get('domain','')).lower()).split('/')[0]}_{i+1}"
           not in done_keys
    ]
    print(f"Already done (checkpoint): {len(rows) - len(args_list)}")
    print(f"Remaining: {len(args_list)}\n")

    results = {
        i: row for i, row in enumerate(rows)
        if f"{re.sub(r'https?://(www.)?','',str(row.get('domain','')).lower()).split('/')[0]}_{i+1}"
           in done_keys
    }

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(process_company, a): a for a in args_list}
        for future in as_completed(futures):
            a = futures[future]
            try:
                results[a[0]-1] = future.result()
            except Exception as e:
                print(f"  [thread error] row {a[0]}: {e}")
                results[a[0]-1] = a[2]

    out_df = pd.DataFrame([results[i] for i in sorted(results.keys())])
    out_df = post_process(out_df)
    out_df.to_csv(args.output, index=False)

    if os.path.exists(CHECKPOINT_FILE):
        os.remove(CHECKPOINT_FILE)

    print("\n" + "=" * 60)
    print("  COMPLETE")
    print(f"  Hooks written : {_counters['hooks']}")
    print(f"  Fallback hooks: {_counters['fallback']}")
    print(f"  No signal     : {_counters['no_signal']}")
    print(f"  Errors        : {_counters['errors']}")
    print(f"  Output        : {args.output}")
    print(f"  Finished      : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)

if __name__ == "__main__":
    main()
