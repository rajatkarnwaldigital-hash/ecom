# EthicalSEO Outbound Signal Engine — Ecommerce

Personalised LinkedIn and email outreach pipeline for ecommerce companies. Detects real SEO problems on each prospect's website and writes a specific, data-driven hook for each one.

Built for EthicalSEO's ecommerce experiment. Repeatable for any new batch.

---

## What it does

Takes a raw list of ecommerce companies from Apollo and produces a campaign-ready CSV where every row has a personalised hook based on a real SEO problem found on that company's website.

Each hook names the specific keyword, position, search volume, CPC, or competitor — not generic "your SEO could be better" copy. The goal is for a founder to read the message and immediately recognise a real problem costing them revenue.

---

## Two scripts

```
qualify.py       — Stage 1: filter companies worth pursuing
ecomm_sniper.py  — Stage 2: detect signals, validate data, write hooks
```

### qualify.py

Filters a raw Apollo export down to companies with genuine organic presence. Applies four checks in order:

1. **Site alive** — skips dead or unreachable domains before spending SEMrush credits
2. **English** — skips non-English sites (outreach copy is in English)
3. **Traffic >= 500** — minimum organic visitors per month
4. **Authority Score >= 10** — minimum domain credibility (SEMrush metric)

No sector filtering, no blog check, no digital-vs-physical check. Designed for ecommerce where we already know the industry from the Apollo export.

Input: Apollo accounts CSV export
Output: `qualified.csv`

### ecomm_sniper.py

Takes qualified companies and their contacts, detects SEO signals on each domain, validates the data through Claude Haiku, and writes the hook with Claude Sonnet.

Merges the qualified company list with a contacts CSV (from Apollo or Snov) on domain, then processes each contact's company.

Input: `qualified.csv` + contacts CSV
Output: `campaign_ready.csv`

---

## Signals detected

Four signals are checked per company. All that fire are included as bullets in the hook.

### 1. Page 2 keywords
Checks for commercial keywords (CPC >= $2, positions 11-20) that do not appear anywhere on the ranking page. These are quick-fix opportunities — the company is close to page 1 but the keyword isn't even on the page.

Hook format: "You're at position 14 for 'mens leather boots' (2,400 searches/month, $3.40 CPC) — the keyword doesn't appear anywhere on the ranking page. That's a quick fix leaving real revenue on the table."

### 2. Missing product schema
Checks whether the company's category/product pages have structured data (Product, BreadcrumbList, Offer schema). If they don't, competitors showing prices and star ratings in Google results will get more clicks even at the same ranking position.

Hook format: "houseoffraser.co.uk is showing prices and star ratings directly in Google results for 'cashmere jumpers uk' (8,100 searches/month) — your listing shows nothing. That gap costs you clicks you're already earning."

### 3. Site speed
Checks mobile PageSpeed score against a top competitor ranking for the same keyword. Fires if the target scores below 65 and a competitor scores at least 10 points higher.

Hook format: "Your site scores 38/100 on mobile speed — cotswoldoutdoor.com scores 74/100. Slow mobile pages lose ~53% of visitors before a page even loads."

### 4. Weak category page titles
Checks collection/category page titles. If a page is titled "Collections", "Shop", "Products" or anything generic under 20 characters, Google has no signal for what the page sells.

Uses `url_organic` to find what keywords the specific page is ranking for (not the domain's top keyword — that was the mistake in v1). Falls back to `domain_organic` if `url_organic` returns no data.

Hook format: "balancecoffee.co.uk/collections is titled 'Collections' — a page that should be targeting 'specialty coffee beans uk' (4,400 searches/month) is giving Google nothing to rank it on."

### Signal 5 — Google Merchant Center (GMC)
Checks whether the company appears in Google Shopping using SEMrush's domain_shopping endpoint. If the domain has zero Shopping keywords, they're almost certainly not in Google Merchant Center — meaning no product listings with images, prices, and ratings appear when people search for what they sell. GMC is also the primary way brands appear in Google's AI-generated answers. Automatically skipped for Baltic countries (Estonia, Latvia, Lithuania) where Google Shopping is not available.

---

## Validation logic (why this matters)

In an earlier version of this pipeline, hooks were broken because:
- The keyword picker was pulling brand names ("omega watches" for a multi-brand watch retailer)
- The competitor finder was naming manufacturers ("Ariat" as a competitor for a store selling Ariat boots)
- Some fallback hooks invented competitor names when no data was found

All three issues are fixed through two Claude Haiku validation steps that run before every hook is written.

### Keyword validation
Haiku receives up to 15 keyword candidates and picks the best generic category/product term.

**Accepts:** "mens running shoes", "luxury candles uk", "high quality coffee beans", "cashmere jumper"
**Rejects:** brand names, designer names, celebrity names, specific product models, completely unrelated topics

### Competitor validation
Haiku receives up to 8 competitor candidates for a keyword and picks a valid ecommerce retailer.

**Accepts:** retailers selling similar products that the CEO would recognise as competition
**Rejects:** brand manufacturers, brand owners of the keyword product, news sites, social platforms, marketplaces (Amazon/eBay/Etsy)

### Fallback (when no valid keyword or competitor found)
Uses `domain_organic_organic` to find the top organic competitor and writes a hook naming them without requiring a specific keyword. Never invents data.

If no competitor can be found at all, writes a generic pain point hook about structured data without naming anyone.

### Post-processing (automatic)
After every run, the script automatically:
- Strips `www.` prefix from all competitor names in hooks and data columns
- Flags rows where the competitor column is empty but the hook contains a domain-like string (potential hallucination)

---

## API keys required

| Key | Used in | Where to get |
|-----|---------|--------------|
| SEMrush API key | Both scripts | SEMrush → Account → API |
| Anthropic API key | ecomm_sniper.py | console.anthropic.com |
| PageSpeed API key | ecomm_sniper.py | Google Cloud Console → APIs → PageSpeed Insights (free) |

Edit the CONFIG section at the top of each script before running.

---

## Installation

```bash
pip install anthropic requests pandas beautifulsoup4
```

Python 3.9+ required.

---

## Usage

### Step 1 — Export companies from Apollo

Filter in Apollo:
- Industry: Retail, Consumer Goods, Apparel & Fashion
- Geography: target region (e.g. UK, Baltics)
- Employee count: 11-200

Export as accounts CSV. See `sample_input.csv` for the expected column structure.

### Step 2 — Run qualify.py

```bash
python3 qualify.py --input apollo_export.csv --output qualified.csv
```

Or edit `INPUT_FILE` and `OUTPUT_FILE` at the top of the script and run:
```bash
python3 qualify.py
```

Expected output: `qualified.csv` with a `qualified` column (True/False) and fail reasons for filtered companies. See `sample_output_qualify.csv`.

Typical results: 30-40% of companies pass qualification.

### Step 3 — Source contacts

Take the qualified domains, run through Apollo or Snov to find CEO/Founder/Owner contacts. Export as CSV.

Contacts CSV must include:
- `first_name`, `last_name`
- `email` (optional but recommended)
- `linkedin_url`
- `job_title`
- `company_name`
- `company_url` (or `Website` or `domain`) — used to match back to qualified companies

### Step 4 — Run ecomm_sniper.py

```bash
python3 ecomm_sniper.py --qualified qualified.csv --contacts contacts.csv --output campaign_ready.csv
```

See `sample_output_sniper.csv` for the output structure.

Typical runtime: 1-2 minutes per company with 5 workers. 500 companies ≈ 2-3 hours.

---

## Output columns

### qualified.csv
| Column | Description |
|--------|-------------|
| company_name | Company name |
| domain | Cleaned domain |
| industry | Industry from Apollo |
| city, country | Location |
| employees | Employee count |
| site_alive | Whether site responded |
| is_english | Whether site is English |
| monthly_traffic | SEMrush organic traffic |
| authority_score | SEMrush Authority Score |
| qualified | True if passed all filters |
| fail_reason | Why the company was filtered out |

### campaign_ready.csv
| Column | Description |
|--------|-------------|
| first_name, last_name | Contact name |
| email | Email address (if sourced) |
| linkedin_url | LinkedIn profile URL |
| job_title | Contact's job title |
| company_name | Company name |
| hook | Personalised outreach hook — use this as DM body |
| signals_detected | Which signals fired (page2, schema, speed, weak_title, fallback) |
| competitor | Competitor named in hook (if applicable) |
| review_flag | True if hook should be manually reviewed before sending |
| page2_keyword, page2_position, page2_cpc, page2_volume | Page 2 signal data |
| schema_keyword, schema_competitor | Schema signal data |
| speed_score, speed_competitor, speed_competitor_score | Speed signal data |
| weak_title_keyword, weak_title_url, weak_title_title | Weak title signal data |

---

## Checkpointing and resume

Both scripts checkpoint progress every 50-100 companies and resume automatically if interrupted. Delete `.checkpoint` and `.json` checkpoint files to start fresh.

---

## Performance

| Stage | Typical throughput | SEMrush credits |
|-------|-------------------|-----------------|
| qualify.py | ~100 companies/min (10 threads) | ~2 credits/company |
| ecomm_sniper.py | ~20 companies/min (5 workers) | ~5 credits/company |

For 1,000 companies: qualify uses ~2,000 credits, sniper uses ~5,000 credits.

---

## Known limitations

1. **SEMrush UK database coverage** — smaller UK brands sometimes have no data in the UK database. The scripts fall back to US database automatically. Very small brands may produce fallback hooks.

2. **url_organic coverage** — the weak title signal uses `url_organic` to find keywords a specific page ranks for. This endpoint has limited coverage for smaller sites. Domain-level fallback is used when url_organic returns nothing.

3. **PageSpeed rate limits** — without a PageSpeed API key, Google limits to 400 requests/day. With a key, limits are much higher. Get a free key from Google Cloud Console.

4. **LinkedIn URL quality** — Apollo sometimes maps company LinkedIn pages to contact profiles. Check for slugs that look like business names rather than person names before loading into Waalaxy or Engine B.

---

## File structure

```
/
├── qualify.py              Stage 1 qualification script
├── ecomm_sniper.py         Stage 2 signal detection + hook writing
├── sample_input.csv        Example Apollo export (anonymised)
├── sample_output_qualify.csv   Example qualified.csv output
├── sample_output_sniper.csv    Example campaign_ready.csv output
└── README.md               This file
```

---

## Waalaxy sequence (reference)

The `hook` column maps to `{{hook}}` in Waalaxy's custom variable system.

| Step | Timing | Content |
|------|--------|---------|
| Connection request | Day 0 | Blank — no note |
| DM 1 | Day 1 after accept | `Hi {{first_name}}, {{hook}}` |
| DM 2 | Day 6, no reply | Value-add: explain structured data CTR impact |
| DM 3 | Day 13, no reply | Short breakup, leave door open |

---

## Engine B (LinkedIn commenting — Airtable)

Load `linkedin_url` column from `campaign_ready.csv` into the Airtable ICP List table. Engine B uses these profiles to scrape company posts and generate relevant comments from Konstantin's profile, warming up visibility before the connection request lands.
