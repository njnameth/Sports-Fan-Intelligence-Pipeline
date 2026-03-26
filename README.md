# Fan Intelligence Pipeline

A Python/pandas ETL pipeline that ingests raw ticket and point-of-sale merch data
from a cloud data warehouse, cleans and normalizes both datasets, and joins them into
a single fan-level "superprofile" — one row per unique fan with attendance history,
ticket financials, and lifetime merch spend.

Built for a multi-sport professional sports organization operating two teams across
two sports. All org-specific identifiers have been abstracted for this portfolio version.

---

## What it does

| Block | Step | Key techniques |
|-------|------|----------------|
| 1 | Dependencies & connection | Snowflake connector, env-var credentials, CSV fallback mode |
| 2 | Pull raw data | Three source tables + a ZIP crosswalk reference |
| 3 | Data quality audit | Null rate + unique count report for every column |
| 4 | Clean tickets | Column pruning by null-rate, geography healing, financial normalization |
| 5 | Clean merch (Square) | Column pruning, mixed-field parsing, financial normalization |
| 6 | Build join keys | Email key (strong) + normalized name key (fallback) |
| 7 | Aggregate tickets → fan | `groupby` aggregation: counts, sums, `nunique`, mode, custom lambdas |
| 8 | Aggregate merch → fan | Same pattern, flattened MultiIndex from date min/max |
| 9 | Two-pass join | Pass 1: email match · Pass 2: name match on unmatched fans |
| 10 | Sanity check | Duplicate key check, fill rate report, financial total audit |
| 11 | Export | CSV + Colab auto-download |

### Notable design decisions

**Geography healing** — A two-priority resolver handles dirty location data:
1. If a valid ZIP is present, look it up against a USPS crosswalk to get canonical city + state.
2. If only a city name exists (possibly misspelled), fuzzy-match it against known cities in
   the inferred state using `rapidfuzz` (score cutoff 85). Results are cached to avoid
   redundant fuzzy passes on large datasets.

**Two-tier join key** — Merch POS data often has email addresses in the "Customer Name"
field where ticket data has full names, and vice versa. A regex detector routes each merch
row to either an email key or a normalized name key. The join runs two passes: email first
(higher confidence), then name for the remaining unmatched ticket fans.

**Schema-safe drops** — Every `drop(columns=...)` call filters the list against the
actual DataFrame columns first, so the pipeline tolerates upstream schema changes without
raising `KeyError`.

---

## Running it

### Option A — CSV mode (no credentials needed)

The default. Uses the synthetic sample data included in `sample_data/`.

```bash
pip install -r requirements.txt
jupyter notebook fan_intelligence_pipeline.ipynb
```

Leave `DATA_SOURCE` unset (or set it to `'csv'`) and run all cells.

### Option B — Live Snowflake

Set environment variables before launching:

```bash
export DATA_SOURCE=snowflake
export SF_USER=your_user
export SF_PASSWORD=your_password
export SF_ACCOUNT=your_account_id
export SF_WAREHOUSE=your_warehouse   # optional, defaults to compute_wh
export SF_ROLE=your_role             # optional, defaults to SYSADMIN

# DB names (optional — override if your schema differs)
export SF_TEAM_A_DB=team_a_tickets
export SF_TEAM_B_DB=team_b_tickets
export SF_MERCH_DB=merch
export SF_ZIP_DB=us_zip_code

jupyter notebook fan_intelligence_pipeline.ipynb
```

> **Never hardcode credentials.** The notebook reads all secrets exclusively from
> environment variables.

### Google Colab

Upload the notebook and the `sample_data/` folder, then run all cells.
Block 11 will trigger an automatic download of the output CSV.

---

## Output schema

The exported `fan_superprofile_master.csv` has one row per unique fan.

| Column group | Columns |
|---|---|
| Identity | `FAN_KEY`, `FIRST_NAME`, `LAST_NAME`, `EMAIL`, `PHONE`, `COMPANY` |
| Geography | `ADDRESS`, `CLEAN_CITY`, `CLEAN_STATE`, `CLEAN_ZIP` |
| Account | `ACCOUNT_NUM`, `ACCOUNT_NAME` |
| Ticket activity | `TOTAL_TICKETS`, `TOTAL_TICKET_SPEND`, `TOTAL_TICKET_PAID`, `GAMES_ATTENDED` |
| Attendance window | `FIRST_GAME`, `LAST_GAME` |
| Engagement | `OPPONENTS_SEEN`, `SPORTS_ATTENDED`, `MOST_COMMON_SECTION`, `TICKET_TYPE` |
| Scan behavior | `TICKETS_SCANNED`, `SCAN_RATE` |
| Merch spend | `MERCH_NET_TOTAL`, `MERCH_GROSS_SALES`, `MERCH_TOTAL_TRANSACTIONS` |
| Merch detail | `MERCH_CARD`, `MERCH_CASH`, `MERCH_REFUNDS`, `MERCH_FIRST_PURCHASE`, `MERCH_LAST_PURCHASE` |
| Flag | `IS_MERCH_BUYER` |

---

## Sample data

`sample_data/` contains four synthetic CSV files that mirror the real source schemas.
All names, emails, phone numbers, and financial figures are completely fabricated.

| File | Description |
|------|-------------|
| `raw_tickets_team_a.csv` | ~130 ticket rows for Team A (Sport A) |
| `raw_tickets_team_b.csv` | ~50 ticket rows for Team B (Sport B) |
| `raw_merch.csv` | ~70 Square POS transaction rows |
| `zip_reference.csv` | USPS ZIP crosswalk (Midwest sample) |

The data is intentionally messy in places — misspelled city names, missing ZIPs, mixed
email/name entries in the merch `Customer Name` field — to demonstrate the cleaning logic.

---

## Dependencies

```
pandas
snowflake-connector-python[pandas]
rapidfuzz
jupyter
```

See `requirements.txt` for pinned versions.

---

## Skills demonstrated

- Multi-source ETL with pandas
- Fuzzy string matching for real-world dirty data
- Two-tier join key design (strong signal → weak signal fallback)
- Schema-resilient data cleaning patterns
- Credential-safe notebook design with env-var injection
- Data quality auditing as a first-class pipeline step
