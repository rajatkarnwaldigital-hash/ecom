"""
qualify.py
EthicalSEO Outbound Signal Engine — Stage 1

Takes a raw company list from Apollo and filters down to companies
worth running signal detection on. Generic — works for any niche.

Filters applied in order:
  1. Site alive     — skips dead/unreachable domains before burning SEMrush credits
  2. English        — skips non-English sites (outreach copy is English)
  3. Traffic >= 500 — minimum organic presence worth pursuing
  4. Authority >= 10 — minimum domain credibility

INPUT:  Apollo accounts CSV export (see sample_input.csv for expected columns)
OUTPUT: qualified.csv (see sample_output_qualify.csv for output structure)

Run:
  python3 qualify.py --input your_apollo_export.csv --output qualified.csv

  Or edit INPUT_FILE / OUTPUT_FILE constants below and run directly:
  python3 qualify.py
"""

import argparse
import os
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock

import pandas as pd
import requests
from bs4 import BeautifulSoup

# ── CONFIG — edit these before running ───────────────────────────────────────
SEMRUSH_API_KEY  = "YOUR_SEMRUSH_API_KEY"
INPUT_FILE       = "apollo_export.csv"
OUTPUT_FILE      = "qualified.csv"

MIN_TRAFFIC      = 500    # minimum monthly organic visits
MIN_AS           = 10     # minimum SEMrush Authority Score
REQUEST_TIMEOUT  = 10     # seconds per HTTP request
DELAY_BETWEEN    = 0.3    # seconds between SEMrush calls per thread
THREADS          = 10     # parallel workers
CHECKPOINT_EVERY = 100    # save progress every N companies

# ── DOMAIN HELPERS ────────────────────────────────────────────────────────────
def normalize_domain(domain):
    """Ensure domain has https:// prefix and no trailing slash."""
    domain = domain.strip().lower()
    if not domain.startswith("http"):
        domain = "https://" + domain
    return domain.rstrip("/")

def clean_domain_from_url(url):
    """Strip protocol, www, path from a URL to get a bare domain."""
    if not url or str(url) in ("nan", ""):
        return ""
    url = str(url).lower().strip()
    url = re.sub(r"https?://(www\.)?", "", url)
    return url.rstrip("/").split("/")[0]

# ── SITE ALIVE CHECK ──────────────────────────────────────────────────────────
def check_site_alive(url):
    """
    Try multiple URL variants (https/http, www/non-www).
    Returns ("alive", response), ("bot_blocked", None), or ("dead", None).
    """
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        )
    }
    from urllib.parse import urlparse
    hostname = urlparse(url).netloc

    if hostname.startswith("www."):
        non_www  = hostname[4:]
        variants = [
            f"https://{hostname}", f"https://{non_www}",
            f"http://{hostname}",  f"http://{non_www}",
        ]
    else:
        variants = [
            f"https://{hostname}", f"https://www.{hostname}",
            f"http://{hostname}",  f"http://www.{hostname}",
        ]

    got_403 = False
    for variant in variants:
        try:
            r = requests.head(
                variant, timeout=REQUEST_TIMEOUT,
                allow_redirects=True, headers=headers
            )
            if r.status_code < 400:
                r = requests.get(
                    variant, timeout=REQUEST_TIMEOUT,
                    allow_redirects=True, headers=headers
                )
                return "alive", r
            elif r.status_code == 403:
                got_403 = True
        except Exception:
            pass

        for attempt in range(2):
            try:
                r = requests.get(
                    variant, timeout=REQUEST_TIMEOUT,
                    allow_redirects=True, headers=headers
                )
                if r.status_code < 400:
                    return "alive", r
                elif r.status_code == 403:
                    got_403 = True
                break
            except Exception:
                if attempt == 0:
                    time.sleep(2)
                continue

    return ("bot_blocked", None) if got_403 else ("dead", None)

# ── LANGUAGE DETECTION ────────────────────────────────────────────────────────
def detect_language(response):
    """
    Returns True if site appears to be English.
    Checks HTML lang attribute and Content-Language meta tag.
    Defaults to True if detection fails — avoids false negatives.
    """
    try:
        soup     = BeautifulSoup(response.text, "html.parser")
        html_tag = soup.find("html")
        if html_tag and html_tag.get("lang"):
            return html_tag.get("lang", "").lower().startswith("en")
        meta = soup.find(
            "meta", attrs={"http-equiv": re.compile("content-language", re.I)}
        )
        if meta:
            return "en" in meta.get("content", "").lower()
        return True
    except Exception:
        return True

# ── SEMRUSH CALLS ─────────────────────────────────────────────────────────────
def get_traffic(domain):
    """
    Returns monthly organic traffic from SEMrush domain_rank.
    Uses US database which has broadest coverage for international brands.
    Returns None if data unavailable.
    """
    try:
        r = requests.get(
            "https://api.semrush.com/",
            params={
                "type":           "domain_rank",
                "key":            SEMRUSH_API_KEY,
                "export_columns": "Dn,Or,Ot",
                "domain":         domain,
                "database":       "us",
            },
            timeout=REQUEST_TIMEOUT,
        )
        lines  = r.text.strip().split("\n")
        if len(lines) < 2:
            return None
        values = lines[1].split(";")
        return int(values[2]) if len(values) > 2 and values[2].isdigit() else None
    except Exception:
        return None

def get_authority_score(domain):
    """
    Returns SEMrush Authority Score (0-100) for the domain.
    Measures overall domain strength based on backlink profile.
    Returns None if data unavailable.
    """
    try:
        r = requests.get(
            "https://api.semrush.com/analytics/v1/",
            params={
                "key":         SEMRUSH_API_KEY,
                "type":        "backlinks_overview",
                "target":      domain,
                "target_type": "root_domain",
                "export_columns": "ascore",
            },
            timeout=REQUEST_TIMEOUT,
        )
        lines  = r.text.strip().split("\n")
        if len(lines) < 2:
            return None
        values = lines[1].split(";")
        return int(values[0]) if len(values) > 0 and values[0].isdigit() else None
    except Exception:
        return None

# ── APOLLO CSV NORMALISATION ──────────────────────────────────────────────────
def normalize_apollo_columns(df):
    """
    Maps Apollo export column names to internal names.
    Cleans and deduplicates domain column.
    """
    col_map = {
        "Company Name": "company_name",
        "Industry":     "industry",
        "Website":      "domain",
        "Company City": "city",
        "# Employees":  "employees",
        "Country":      "country",
    }
    df = df.rename(columns={k: v for k, v in col_map.items() if k in df.columns})
    if "domain" in df.columns:
        df["domain"] = df["domain"].astype(str).apply(clean_domain_from_url)
    df = df[df["domain"] != ""].reset_index(drop=True)
    df = df.drop_duplicates(subset="domain").reset_index(drop=True)
    return df

# ── PER-COMPANY PROCESSING ────────────────────────────────────────────────────
def process_row(args):
    """
    Runs all four filters on a single company.
    Returns (index, result_dict, log_string).
    """
    i, total, row = args
    company = str(row.get("company_name", "")).strip()
    domain  = str(row.get("domain", "")).strip()

    result = {
        "company_name":    company,
        "domain":          domain,
        "industry":        row.get("industry", ""),
        "city":            row.get("city", ""),
        "country":         row.get("country", ""),
        "employees":       row.get("employees", ""),
        "site_alive":      None,
        "is_english":      None,
        "authority_score": None,
        "monthly_traffic": None,
        "qualified":       False,
        "fail_reason":     "",
    }

    if not domain:
        result["fail_reason"] = "no_domain"
        return i, result, f"[{i}/{total}] {company} — no domain"

    base_url = normalize_domain(domain)

    # Filter 1: site alive
    status, response = check_site_alive(base_url)
    result["site_alive"] = status == "alive"
    if status == "bot_blocked":
        result["fail_reason"] = "bot_blocked"
        return i, result, f"[{i}/{total}] {company} ({domain}) — bot blocked"
    if status == "dead":
        result["fail_reason"] = "dead_site"
        return i, result, f"[{i}/{total}] {company} ({domain}) — dead site"

    # Filter 2: English
    is_english = detect_language(response)
    result["is_english"] = is_english
    if not is_english:
        result["fail_reason"] = "non_english"
        return i, result, f"[{i}/{total}] {company} ({domain}) — non-English"

    # Filter 3: traffic
    time.sleep(DELAY_BETWEEN)
    traffic = get_traffic(domain)
    result["monthly_traffic"] = traffic
    if traffic is not None and traffic < MIN_TRAFFIC:
        result["fail_reason"] = "low_traffic"
        return i, result, f"[{i}/{total}] {company} ({domain}) — low traffic ({traffic})"

    # Filter 4: authority score
    time.sleep(DELAY_BETWEEN)
    ascore = get_authority_score(domain)
    result["authority_score"] = ascore
    if ascore is not None and ascore < MIN_AS:
        result["fail_reason"] = "low_authority_score"
        return i, result, f"[{i}/{total}] {company} ({domain}) — low AS ({ascore})"

    result["qualified"] = True
    return (
        i, result,
        f"[{i}/{total}] {company} ({domain}) — QUALIFIED (AS:{ascore} Traffic:{traffic})"
    )

# ── MAIN ──────────────────────────────────────────────────────────────────────
def qualify(input_csv, output_csv):
    df    = pd.read_csv(input_csv)
    df    = normalize_apollo_columns(df)
    total = len(df)
    print(f"\nLoaded {total} companies (after dedup and empty domain removal)")
    print(f"Filters: site alive → English → traffic >= {MIN_TRAFFIC} → AS >= {MIN_AS}\n")

    checkpoint_file = output_csv + ".checkpoint"
    done_domains    = set()
    results         = []

    if os.path.exists(checkpoint_file):
        existing     = pd.read_csv(checkpoint_file)
        done_domains = set(existing["domain"].tolist())
        results      = existing.to_dict("records")
        print(f"Resuming from checkpoint — {len(done_domains)} already processed\n")

    df        = df[~df["domain"].isin(done_domains)].reset_index(drop=True)
    remaining = len(df)
    print(f"Processing {remaining} remaining companies with {THREADS} threads\n")

    lock      = Lock()
    processed = 0
    args_list = [
        (i + len(done_domains) + 1, total, row)
        for i, row in df.iterrows()
    ]

    with ThreadPoolExecutor(max_workers=THREADS) as executor:
        futures = {executor.submit(process_row, args): args for args in args_list}
        for future in as_completed(futures):
            try:
                idx, result, log = future.result()
                with lock:
                    results.append(result)
                    processed += 1
                    print(log)
                    if processed % CHECKPOINT_EVERY == 0:
                        pd.DataFrame(results).to_csv(checkpoint_file, index=False)
                        print(f"\nCheckpoint saved ({processed}/{remaining})\n")
            except Exception as e:
                print(f"Error: {e}")

    out_df = pd.DataFrame(results)
    out_df.to_csv(output_csv, index=False)

    if os.path.exists(checkpoint_file):
        os.remove(checkpoint_file)

    qualified = int(out_df["qualified"].sum())
    failed    = len(out_df) - qualified
    print(f"\n{'─' * 50}")
    print(f"Qualified    : {qualified}")
    print(f"Filtered out : {failed}")
    print(f"Total        : {len(out_df)}")
    print("\nFail breakdown:")
    for reason, count in out_df[out_df["fail_reason"] != ""]["fail_reason"].value_counts().items():
        print(f"  {reason}: {count}")
    print(f"\nOutput saved to: {output_csv}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="EthicalSEO — qualify company list")
    parser.add_argument("--input",  default=INPUT_FILE,  help="Apollo CSV export")
    parser.add_argument("--output", default=OUTPUT_FILE, help="Output CSV path")
    args = parser.parse_args()
    qualify(args.input, args.output)
