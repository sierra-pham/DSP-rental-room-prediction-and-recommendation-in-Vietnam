# Vietnam Rental Market Intelligence — Implementation Plan

**Goal:** Build an end-to-end big data pipeline that crawls ~800k Vietnamese rental
listings, maintains a 30-day daily panel, and produces a fair-rent model plus a
time-on-market survival model on Spark/AWS.

**Architecture:** Scrapy crawlers on a single always-on EC2 instance write immutable
gzipped raw HTML to an S3 bronze layer. A free three-node Spark standalone cluster on
Oracle Cloud Always Free, reading S3 over `s3a://`, parses bronze into
a partitioned Parquet silver layer, deduplicate via MinHash LSH, assemble a
listing×day panel fact table in gold, and train Spark MLlib models. Parsers are pure
functions over HTML strings, unit-tested against saved fixtures, so parser bugs are
fixed by re-running against bronze rather than re-crawling.

**Tech Stack:** Python 3.11, Scrapy 2.11, Playwright (fallback only), SQLite, AWS S3,
Oracle Cloud Always Free (3 ARM VMs), Spark 3.5 standalone, PySpark MLlib, `hadoop-aws`
S3A connector, AWS Glue Catalog, Athena, pytest, folium, H3.

**Spec:** `02-technical-design-spec.md` (architecture, schemas, algorithms)
**Charter:** `01-project-charter.md` (problem, success criteria, scope)

---

## Global Constraints

Every task's requirements implicitly include this section.

- **Python 3.11+.** Scrapy 2.11+, PySpark 3.5 (must match the Spark installed on the
  cluster VMs exactly — a driver/cluster version mismatch fails at submit time).
- **The crawler must be live and collecting by 2026-09-18 (Day 5).** No exceptions.
  Survival labels cannot be back-filled. Week 1 optimises for *running*, not *elegant*.
- **Bronze is immutable.** Nothing ever deletes, rewrites, or edits a bronze object.
- **Parsers never make network calls.** `parsers/` takes an HTML string, returns a dict.
- **Every Spark job is idempotent**: `--date X` twice produces identical output.
- **Politeness ceiling: 1 request/second per domain.** Hard limit, enforced in settings.
- **User-Agent must be descriptive and include a contact email.** Exact string:
  `VN-Rental-Research/1.0 (academic project; nguyenpnt4@fpt.com)`
- **No personal data past bronze.** Phone numbers, emails, and social handles are
  stripped in the bronze→silver transition. A regex scan gates every silver write.
- **All S3 paths are `s3://vn-rental-dsp/<layer>/...`** — bucket name comes from
  `config/aws.yaml`, never hardcoded in a job.
- **Compute is free and stays free.** All Spark runs on the Oracle Always Free cluster.
  No managed Spark service is provisioned at any point.
- **Watch AWS egress, not compute.** Reading S3 from outside AWS is billed beyond
  100 GB/month. Daily processing is incremental; full re-parses run at most weekly.
- **Commit after every task.** Small commits; the git log is evidence for Report 4.
- **Budget ceiling: $50.** AWS Budgets alerts at $10 / $25 / $50 configured in Task 2.
  Spark compute is free; the only meaningful AWS cost is S3 storage and egress.

---

## Phase 0 — Foundation (Week 1, Days 1–2)

### Task 1: Repository skeleton and dependency setup

**Files:**
- Create: `pyproject.toml`, `README.md`, `.gitignore`, `config/aws.yaml`,
  `config/sources.yaml`
- Create: `tests/__init__.py`, `tests/test_smoke.py`

**Interfaces:**
- Consumes: nothing
- Produces: an importable package layout; `config.load(name) -> dict` used by every
  later task, defined in `config/__init__.py`

- [ ] **Step 1: Create the directory tree**

```bash
mkdir -p vn-rental-dsp/{config,crawler/spiders,spark,parsers,analysis,tests/fixtures,infra,docs}
cd vn-rental-dsp && git init
touch parsers/__init__.py crawler/__init__.py tests/__init__.py
```

- [ ] **Step 2: Write `config/__init__.py`**

```python
from pathlib import Path
import yaml

_CONFIG_DIR = Path(__file__).parent

def load(name: str) -> dict:
    """Load config/<name>.yaml as a dict. Raises FileNotFoundError if absent."""
    path = _CONFIG_DIR / f"{name}.yaml"
    with path.open(encoding="utf-8") as fh:
        return yaml.safe_load(fh)
```

- [ ] **Step 3: Write the failing test**

```python
# tests/test_smoke.py
import config

def test_aws_config_has_bucket():
    cfg = config.load("aws")
    assert cfg["bucket"] == "vn-rental-dsp"
    assert cfg["region"] == "ap-southeast-1"

def test_sources_config_lists_four_sources():
    cfg = config.load("sources")
    assert set(cfg["sources"]) == {"batdongsan", "phongtro123", "mogi", "nhatot"}
```

- [ ] **Step 4: Run it and watch it fail**

Run: `pytest tests/test_smoke.py -v`
Expected: FAIL — `FileNotFoundError: config/aws.yaml`

- [ ] **Step 5: Write `config/aws.yaml`**

```yaml
bucket: vn-rental-dsp
region: ap-southeast-1
spark:
  master_url: "spark://spark-master:7077"
  master_host: spark-master          # SSH alias in ~/.ssh/config
  remote_root: /opt/vn-rental-dsp
  driver_memory: 4g
  executor_memory: 3g
  executor_cores: 1
  shuffle_partitions: 24
```

- [ ] **Step 6: Write `config/sources.yaml`**

```yaml
user_agent: "VN-Rental-Research/1.0 (academic project; nguyenpnt4@fpt.com)"
max_rps_per_domain: 1.0
sources:
  batdongsan:
    domain: batdongsan.com.vn
    sitemap_urls: ["https://batdongsan.com.vn/sitemap.xml"]
    rental_url_pattern: "/cho-thue-"
    needs_js: false
  phongtro123:
    domain: phongtro123.com
    sitemap_urls: ["https://phongtro123.com/sitemap.xml"]
    rental_url_pattern: "/"
    needs_js: false
  mogi:
    domain: mogi.vn
    sitemap_urls: ["https://mogi.vn/sitemap.xml"]
    rental_url_pattern: "/thue-"
    needs_js: false
  nhatot:
    domain: nhatot.com
    sitemap_urls: ["https://www.nhatot.com/sitemap.xml"]
    rental_url_pattern: "/thue-"
    needs_js: true
    sitemap_only: true   # robots.txt disallows search/filter params
```

- [ ] **Step 7: Write `pyproject.toml` dependencies**

```toml
[project]
name = "vn-rental-dsp"
version = "0.1.0"
requires-python = ">=3.11"
dependencies = [
  "scrapy>=2.11", "pyyaml>=6.0", "boto3>=1.34", "lxml>=5.0",
  "pytest>=8.0", "python-dateutil>=2.9",
]

[project.optional-dependencies]
spark = ["pyspark==3.5.1", "h3>=4.0", "pandas>=2.2", "pyarrow>=15.0"]
viz = ["matplotlib>=3.8", "seaborn>=0.13", "folium>=0.16", "plotly>=5.20"]
js = ["playwright>=1.44", "scrapy-playwright>=0.0.34"]
```

- [ ] **Step 8: Install and re-run the test**

Run: `pip install -e ".[spark,viz]" && pytest tests/ -v`
Expected: 2 passed

- [ ] **Step 9: Commit**

`git add -A && git commit -m "Repo skeleton, config loader, source definitions"`

---

### Task 2: AWS account setup and cost guardrails

**Files:**
- Create: `infra/setup_s3.sh`, `infra/budgets.sh`, `docs/aws-setup-log.md`

**Interfaces:**
- Consumes: `config/aws.yaml`
- Produces: an S3 bucket with the medallion prefix layout; budget alerts

**Do this before writing a single crawler line.** An unmonitored AWS account is how
student projects end in a $900 bill.

- [ ] **Step 1: Create the bucket and prefix layout**

```bash
# infra/setup_s3.sh
set -euo pipefail
BUCKET=vn-rental-dsp
REGION=ap-southeast-1

aws s3api create-bucket --bucket "$BUCKET" --region "$REGION" \
  --create-bucket-configuration LocationConstraint="$REGION"

aws s3api put-public-access-block --bucket "$BUCKET" \
  --public-access-block-configuration \
  "BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true"

for p in bronze silver gold images models; do
  aws s3api put-object --bucket "$BUCKET" --key "$p/"
done
```

- [ ] **Step 2: Add a lifecycle rule moving bronze to Intelligent-Tiering at 30 days**

```bash
aws s3api put-bucket-lifecycle-configuration --bucket vn-rental-dsp \
  --lifecycle-configuration '{
    "Rules": [{
      "ID": "bronze-to-intelligent-tiering",
      "Status": "Enabled",
      "Filter": {"Prefix": "bronze/"},
      "Transitions": [{"Days": 30, "StorageClass": "INTELLIGENT_TIERING"}]
    }]
  }'
```

- [ ] **Step 3: Create budget alerts at $10, $25, $50**

Create these in the AWS Budgets console (simpler than the CLI JSON for a one-off):
Billing → Budgets → Create budget → Cost budget → Monthly → $50 →
alerts at 20%, 50%, 100% of budgeted amount → email `nguyenpnt4@fpt.com`.

Thresholds are deliberately low. With Spark compute running free on Oracle, expected
AWS spend is ~$20/month — so a $25 alert means something is wrong, not that the project
is going well.

- [ ] **Step 3b: Add a data-transfer alarm**

```bash
aws cloudwatch put-metric-alarm \
  --alarm-name s3-egress-high \
  --namespace AWS/S3 --metric-name BytesDownloaded \
  --statistic Sum --period 86400 --evaluation-periods 1 \
  --threshold 85899345920 --comparison-operator GreaterThanThreshold
```

Egress is the one cost that can still surprise you, because reading S3 from the Oracle
cluster leaves AWS. 80 GB is the warning line against the 100 GB/month free allowance.

- [ ] **Step 4: Verify the bucket is reachable from Python**

```python
# tests/test_aws.py
import boto3, config

def test_bucket_exists():
    cfg = config.load("aws")
    s3 = boto3.client("s3", region_name=cfg["region"])
    resp = s3.list_objects_v2(Bucket=cfg["bucket"], MaxKeys=1)
    assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
```

Run: `pytest tests/test_aws.py -v`
Expected: PASS

- [ ] **Step 5: Record setup in `docs/aws-setup-log.md`**

Note the account ID, region, IAM user/role used, and the date budgets were created.
Report 2 needs this; reconstructing it later from memory wastes an hour.

- [ ] **Step 6: Commit**

`git add -A && git commit -m "S3 bucket, lifecycle rules, budget guardrails"`

---

## Phase 1 — Get Data Flowing (Week 1, Days 3–5) — CRITICAL PATH

### Task 3: Parser base class and the first parser (batdongsan)

Parsers come before crawlers deliberately: writing the parser first forces you to look
at the actual HTML and discover early that a field you assumed exists does not.

**Files:**
- Create: `parsers/base.py`, `parsers/batdongsan.py`, `parsers/normalise.py`
- Create: `tests/test_parsers.py`, `tests/fixtures/batdongsan_sample_01.html`
- Test: `tests/test_normalise.py`

**Interfaces:**
- Consumes: `config.load("sources")`
- Produces:
  - `parsers.base.Parser` — ABC with `parse(html: str, url: str) -> dict`
  - `parsers.normalise.parse_price_vnd(text: str) -> int | None`
  - `parsers.normalise.parse_area_sqm(text: str) -> float | None`
  - `parsers.normalise.strip_pii(text: str) -> str`
  - `parsers.batdongsan.BatdongsanParser`

- [ ] **Step 1: Save three real HTML fixtures by hand**

Open three batdongsan rental listings in a browser, View Source, save to
`tests/fixtures/batdongsan_sample_{01,02,03}.html`. Pick deliberately different ones:
an apartment with full fields, a room with sparse fields, and one priced "thỏa thuận".

- [ ] **Step 2: Write the failing normalisation test**

```python
# tests/test_normalise.py
import pytest
from parsers.normalise import parse_price_vnd, parse_area_sqm, strip_pii

@pytest.mark.parametrize("text,expected", [
    ("7 triệu/tháng", 7_000_000),
    ("7tr5", 7_500_000),
    ("7,500,000 đ", 7_500_000),
    ("7.5 triệu", 7_500_000),
    ("12tr500", 12_500_000),
    ("850 nghìn", 850_000),
    ("Thỏa thuận", None),
    ("", None),
])
def test_parse_price_vnd(text, expected):
    assert parse_price_vnd(text) == expected

@pytest.mark.parametrize("text,expected", [
    ("25 m²", 25.0), ("25m2", 25.0), ("25,5 m²", 25.5), ("Đang cập nhật", None),
])
def test_parse_area_sqm(text, expected):
    assert parse_area_sqm(text) == expected

def test_strip_pii_removes_phone_and_zalo():
    raw = "Liên hệ 0901234567 hoặc zalo 090.123.4567, mail a@b.com"
    out = strip_pii(raw)
    assert "0901234567" not in out
    assert "090.123.4567" not in out
    assert "a@b.com" not in out
    assert "<PHONE>" in out and "<EMAIL>" in out
```

- [ ] **Step 3: Run it and confirm it fails**

Run: `pytest tests/test_normalise.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'parsers.normalise'`

- [ ] **Step 4: Implement `parsers/normalise.py`**

```python
import re, unicodedata

_PHONE_RE = re.compile(r"(?:\+?84|0)[\s.\-]?\d{2,3}[\s.\-]?\d{3}[\s.\-]?\d{3,4}")
_EMAIL_RE = re.compile(r"[\w.+-]+@[\w-]+\.[\w.]+")
_SOCIAL_RE = re.compile(r"(?:zalo|facebook|fb|viber)[\s:]*[\w./]+", re.IGNORECASE)

def strip_pii(text: str) -> str:
    if not text:
        return ""
    text = _EMAIL_RE.sub("<EMAIL>", text)
    text = _SOCIAL_RE.sub("<SOCIAL>", text)
    text = _PHONE_RE.sub("<PHONE>", text)
    return text

def normalise_text(text: str) -> str:
    if not text:
        return ""
    text = unicodedata.normalize("NFC", text)
    return re.sub(r"\s+", " ", text).strip().lower()

def parse_price_vnd(text: str) -> int | None:
    """Handle 7tr5 / 7.5 triệu / 7,500,000 / 850 nghìn. Returns None if not a number."""
    if not text:
        return None
    t = normalise_text(text)
    if "thỏa thuận" in t or "thoa thuan" in t:
        return None
    # The whole part may carry a decimal separator. It must be part of this pattern,
    # not a separate fallback: on "7.5 triệu" a leading r"(\d+)\s*tr" skips the "7."
    # and matches "5 triệu", silently returning 5_000_000.
    m = re.search(r"(\d+(?:[.,]\d+)?)\s*tr(?:iệu)?\s*(\d+)?", t)
    if m:
        whole, frac = m.group(1), m.group(2)
        if "." in whole or "," in whole:
            # "7.5 triệu" -> 7.5 million. A decimal whole part is the whole value,
            # so any trailing digits are not a fraction.
            return int(float(whole.replace(",", ".")) * 1_000_000)
        if frac is None:
            return int(whole) * 1_000_000
        # "7tr5" -> 7.5 million; "12tr500" -> 12.5 million
        return int(whole) * 1_000_000 + int(frac) * (10 ** (6 - len(frac)))
    m = re.search(r"([\d.,]+)\s*(?:nghìn|ngàn|k)\b", t)
    if m:
        return int(float(m.group(1).replace(",", ".")) * 1_000)
    m = re.search(r"([\d.,]{7,})", t)
    if m:
        digits = re.sub(r"[.,]", "", m.group(1))
        return int(digits)
    return None

def parse_area_sqm(text: str) -> float | None:
    if not text:
        return None
    t = normalise_text(text)
    m = re.search(r"([\d]+(?:[.,]\d+)?)\s*m(?:2|²)", t)
    return float(m.group(1).replace(",", ".")) if m else None
```

- [ ] **Step 5: Run the test until green**

Run: `pytest tests/test_normalise.py -v`
Expected: 13 passed

- [ ] **Step 6: Write the failing parser test**

```python
# tests/test_parsers.py
from pathlib import Path
import pytest
from parsers.batdongsan import BatdongsanParser

FIXTURES = Path(__file__).parent / "fixtures"

@pytest.fixture
def sample_html():
    return (FIXTURES / "batdongsan_sample_01.html").read_text(encoding="utf-8")

def test_parse_returns_required_fields(sample_html):
    out = BatdongsanParser().parse(sample_html, "https://batdongsan.com.vn/cho-thue-x/pr1")
    for field in ("listing_id", "source", "asking_rent_vnd", "area_sqm",
                  "province", "district", "property_type", "description_clean"):
        assert field in out, f"missing {field}"

def test_parse_strips_pii(sample_html):
    out = BatdongsanParser().parse(sample_html, "https://batdongsan.com.vn/cho-thue-x/pr1")
    import re
    assert not re.search(r"(?:\+?84|0)\d{9}", out["description_clean"])

def test_parse_sparse_listing_does_not_crash():
    html = (FIXTURES / "batdongsan_sample_02.html").read_text(encoding="utf-8")
    out = BatdongsanParser().parse(html, "https://batdongsan.com.vn/cho-thue-y/pr2")
    assert out["listing_id"]        # must always resolve
    assert out["asking_rent_vnd"] is None or out["asking_rent_vnd"] > 0
```

- [ ] **Step 7: Run it, confirm it fails, then implement `parsers/base.py` and
      `parsers/batdongsan.py`**

`base.py`:

```python
from abc import ABC, abstractmethod

class Parser(ABC):
    source: str

    @abstractmethod
    def parse(self, html: str, url: str) -> dict:
        """Return a dict matching the silver.listings schema. Never raises on
        missing optional fields — returns None for them instead."""
```

`batdongsan.py` uses `lxml.html` with CSS selectors read from the fixtures. Selectors
live as module-level constants so they are easy to fix when the site changes.

- [ ] **Step 8: Run the parser tests until green**

Run: `pytest tests/test_parsers.py -v`
Expected: 3 passed

- [ ] **Step 9: Commit**

`git add -A && git commit -m "Parser ABC, Vietnamese normalisation, batdongsan parser"`

---

### Task 4: Crawl frontier (SQLite)

**Files:**
- Create: `crawler/frontier.py`
- Test: `tests/test_frontier.py`

**Interfaces:**
- Consumes: nothing
- Produces:
  - `Frontier(db_path: str)` with methods:
    - `add_urls(source: str, urls: list[str]) -> int` — returns count of *new* URLs
    - `next_batch(source: str, kind: str, limit: int) -> list[str]`
    - `mark_fetched(url: str, status: int, content_hash: str) -> None`
    - `mark_failed(url: str) -> None` — increments consecutive failure count
    - `freeze_panel_cohort(size: int) -> int` — selects cohort, returns count
    - `panel_due(as_of: date) -> list[str]`

- [ ] **Step 1: Write the failing test**

```python
# tests/test_frontier.py
import datetime as dt
import pytest
from crawler.frontier import Frontier

@pytest.fixture
def fr(tmp_path):
    return Frontier(str(tmp_path / "frontier.db"))

def test_add_urls_deduplicates(fr):
    assert fr.add_urls("mogi", ["https://mogi.vn/a", "https://mogi.vn/b"]) == 2
    assert fr.add_urls("mogi", ["https://mogi.vn/b", "https://mogi.vn/c"]) == 1

def test_next_batch_returns_unfetched_only(fr):
    fr.add_urls("mogi", ["https://mogi.vn/a", "https://mogi.vn/b"])
    fr.mark_fetched("https://mogi.vn/a", 200, "hash1")
    batch = fr.next_batch("mogi", kind="discovery", limit=10)
    assert batch == ["https://mogi.vn/b"]

def test_two_consecutive_failures_marks_gone(fr):
    fr.add_urls("mogi", ["https://mogi.vn/a"])
    fr.mark_failed("https://mogi.vn/a")
    assert fr.is_gone("https://mogi.vn/a") is False
    fr.mark_failed("https://mogi.vn/a")
    assert fr.is_gone("https://mogi.vn/a") is True

def test_success_resets_failure_counter(fr):
    fr.add_urls("mogi", ["https://mogi.vn/a"])
    fr.mark_failed("https://mogi.vn/a")
    fr.mark_fetched("https://mogi.vn/a", 200, "hash1")
    fr.mark_failed("https://mogi.vn/a")
    assert fr.is_gone("https://mogi.vn/a") is False

def test_freeze_panel_cohort_selects_requested_size(fr):
    urls = [f"https://mogi.vn/{i}" for i in range(500)]
    fr.add_urls("mogi", urls)
    for u in urls:
        fr.mark_fetched(u, 200, "h")
    assert fr.freeze_panel_cohort(size=200) == 200
    assert len(fr.panel_due(as_of=dt.date(2026, 9, 20))) == 200
```

- [ ] **Step 2: Run it and confirm failure**

Run: `pytest tests/test_frontier.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'crawler.frontier'`

- [ ] **Step 3: Implement `crawler/frontier.py`**

Schema:

```sql
CREATE TABLE IF NOT EXISTS urls (
  url TEXT PRIMARY KEY,
  source TEXT NOT NULL,
  first_seen DATE NOT NULL,
  last_fetch_ts TEXT,
  last_status INTEGER,
  content_hash TEXT,
  consecutive_failures INTEGER NOT NULL DEFAULT 0,
  in_panel INTEGER NOT NULL DEFAULT 0,
  gone_date DATE
);
CREATE INDEX IF NOT EXISTS idx_source_fetch ON urls(source, last_fetch_ts);
CREATE INDEX IF NOT EXISTS idx_panel ON urls(in_panel, last_fetch_ts);
```

Open the connection with `PRAGMA journal_mode=WAL` — the crawler writes while the
scheduler reads, and rollback-journal mode will deadlock them.

`freeze_panel_cohort` selects a **stratified** sample: proportional across
`source × province` so the panel is not 90% HCMC apartments.

- [ ] **Step 4: Run the tests until green**

Run: `pytest tests/test_frontier.py -v`
Expected: 5 passed

- [ ] **Step 5: Commit**

`git add -A && git commit -m "SQLite crawl frontier with panel cohort management"`

---

### Task 5: Sitemap poller

**Files:**
- Create: `crawler/sitemap_poller.py`
- Test: `tests/test_sitemap_poller.py`, `tests/fixtures/sitemap_sample.xml`

**Interfaces:**
- Consumes: `config.load("sources")`, `crawler.frontier.Frontier`
- Produces: `poll_source(source: str, frontier: Frontier) -> int` — new URLs added;
  `extract_urls(xml: str, pattern: str) -> list[str]`

- [ ] **Step 1: Write the failing test (parsing only — no network in tests)**

```python
# tests/test_sitemap_poller.py
from pathlib import Path
from crawler.sitemap_poller import extract_urls

def test_extract_urls_filters_by_pattern():
    xml = (Path(__file__).parent / "fixtures" / "sitemap_sample.xml").read_text()
    urls = extract_urls(xml, pattern="/cho-thue-")
    assert all("/cho-thue-" in u for u in urls)
    assert len(urls) > 0

def test_extract_urls_handles_sitemap_index():
    xml = """<?xml version="1.0"?>
    <sitemapindex xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
      <sitemap><loc>https://x.com/sitemap-1.xml</loc></sitemap>
    </sitemapindex>"""
    assert extract_urls(xml, pattern="") == ["https://x.com/sitemap-1.xml"]
```

- [ ] **Step 2: Run, confirm failure, implement**

Use `lxml.etree` with namespace-agnostic matching (`local-name()`), because sitemap
namespace declarations vary between these four sites. Handle both `<urlset>` and
`<sitemapindex>` roots; recurse one level into index files.

- [ ] **Step 3: Run the test until green**

Run: `pytest tests/test_sitemap_poller.py -v`
Expected: 2 passed

- [ ] **Step 4: Run it for real against one source, manually**

```bash
python -m crawler.sitemap_poller --source phongtro123 --db /var/data/frontier.db
```

Expected: prints a count in the tens of thousands. If it prints 0, the URL pattern in
`config/sources.yaml` is wrong — fix the config, not the code.

- [ ] **Step 5: Commit**

`git add -A && git commit -m "Sitemap poller with index recursion"`

---

### Task 6: First spider + S3 bronze writer — GO LIVE

**This is the Day 5 deadline task.** Everything before it exists to make it work.

**Files:**
- Create: `crawler/spiders/batdongsan.py`, `crawler/middlewares.py`,
  `crawler/settings.py`, `crawler/s3_writer.py`
- Test: `tests/test_s3_writer.py`

**Interfaces:**
- Consumes: `Frontier`, `config.load("sources")`, `config.load("aws")`
- Produces: `S3BatchWriter(bucket, prefix, batch_mb=128)` with `.write(record: dict)`
  and `.flush()`; bronze objects at
  `bronze/listings/dt=<date>/source=<source>/part-NNNN.jsonl.gz`

- [ ] **Step 1: Write the failing S3 writer test (against a local temp dir)**

```python
# tests/test_s3_writer.py
import gzip, json
from crawler.s3_writer import S3BatchWriter

def test_writer_batches_and_flushes(tmp_path):
    w = S3BatchWriter(bucket=None, prefix=str(tmp_path), batch_mb=0.001)
    for i in range(50):
        w.write({"listing_id": f"x:{i}", "html_gz_b64": "A" * 500})
    w.flush()
    files = list(tmp_path.rglob("*.jsonl.gz"))
    assert len(files) >= 1
    with gzip.open(files[0], "rt") as fh:
        rows = [json.loads(line) for line in fh]
    assert rows[0]["listing_id"] == "x:0"
```

- [ ] **Step 2: Run, confirm failure, implement `crawler/s3_writer.py`**

Buffer records in memory; when the gzipped buffer exceeds `batch_mb`, upload via
`boto3.put_object` and start a new part. `bucket=None` writes to the local path
instead — this is what makes the writer testable without AWS.

- [ ] **Step 3: Write `crawler/settings.py` with the politeness settings**

```python
BOT_NAME = "vn_rental_dsp"
USER_AGENT = "VN-Rental-Research/1.0 (academic project; nguyenpnt4@fpt.com)"
ROBOTSTXT_OBEY = True
CONCURRENT_REQUESTS_PER_DOMAIN = 2
DOWNLOAD_DELAY = 1.0
AUTOTHROTTLE_ENABLED = True
AUTOTHROTTLE_TARGET_CONCURRENCY = 1.0
AUTOTHROTTLE_MAX_DELAY = 30.0
RETRY_TIMES = 2
RETRY_HTTP_CODES = [500, 502, 503, 504, 522, 524, 408, 429]
HTTPCACHE_ENABLED = False
LOG_LEVEL = "INFO"
```

`ROBOTSTXT_OBEY = True` is not optional — it is the mechanical enforcement of the
compliance claim in the governance document.

- [ ] **Step 4: Write the spider**

It reads a batch from the frontier, yields requests, and on response writes the raw
gzipped body to the S3 writer and calls `frontier.mark_fetched()`. **It does not
parse.** Parsing happens in Spark against bronze.

- [ ] **Step 5: Smoke-test against 100 URLs**

```bash
scrapy crawl batdongsan -s CLOSESPIDER_PAGECOUNT=100
aws s3 ls s3://vn-rental-dsp/bronze/listings/ --recursive | head
```

Expected: at least one `.jsonl.gz` object exists; frontier shows 100 fetched rows.

- [ ] **Step 6: Throughput validation — the gate from the spec**

Run for one hour and count pages:

```bash
timeout 3600 scrapy crawl batdongsan
sqlite3 /var/data/frontier.db \
  "SELECT COUNT(*) FROM urls WHERE last_fetch_ts > datetime('now','-1 hour');"
```

Expected: ≥ 2,000 pages/hour for a single source. Extrapolated across 4 sources this
gives ~8,000/hour → ~190k/day. **If below 1,500/hour, stop and diagnose now** — check
CPU credit balance (`aws cloudwatch get-metric-statistics` on `CPUCreditBalance`) and
whether AutoThrottle has backed off due to slow responses.

- [ ] **Step 7: Deploy to EC2 and start the cron**

```bash
# crontab -e on the t3.micro
0 1 * * *  cd /opt/vn-rental-dsp && python -m crawler.sitemap_poller --all >> /var/log/poll.log 2>&1
0 2 * * *  cd /opt/vn-rental-dsp && ./run_discovery.sh   >> /var/log/crawl.log 2>&1
0 * * * *  aws s3 cp /var/data/frontier.db s3://vn-rental-dsp/state/frontier.db
```

- [ ] **Step 8: Confirm data is landing, then commit**

```bash
aws s3 ls s3://vn-rental-dsp/bronze/listings/ --recursive --summarize | tail -3
git add -A && git commit -m "batdongsan spider live, S3 bronze writer, cron deployment"
```

**Milestone: 🟢 CRAWLER LIVE.** Record the date and time in `docs/aws-setup-log.md`.
This timestamp is the start of the observation window and every survival duration is
measured from it.

---

### Task 7: Remaining three spiders

**Files:**
- Create: `crawler/spiders/phongtro123.py`, `crawler/spiders/mogi.py`,
  `crawler/spiders/nhatot.py`
- Create: `parsers/phongtro123.py`, `parsers/mogi.py`, `parsers/nhatot.py`
- Create: `tests/fixtures/{phongtro123,mogi,nhatot}_sample_01.html`
- Modify: `tests/test_parsers.py` — add the same three tests per source

**Interfaces:**
- Consumes: `parsers.base.Parser`, `crawler.s3_writer.S3BatchWriter`
- Produces: `Phongtro123Parser`, `MogiParser`, `NhatotParser` — each conforming to the
  same `parse(html, url) -> dict` contract as `BatdongsanParser`

- [ ] **Step 1: Save 3 HTML fixtures per source** (9 files)

- [ ] **Step 2: Write the parametrised failing test**

```python
# append to tests/test_parsers.py
import pytest
from parsers.batdongsan import BatdongsanParser
from parsers.phongtro123 import Phongtro123Parser
from parsers.mogi import MogiParser
from parsers.nhatot import NhatotParser

ALL_PARSERS = [BatdongsanParser, Phongtro123Parser, MogiParser, NhatotParser]
REQUIRED = ("listing_id", "source", "asking_rent_vnd", "area_sqm",
            "province", "district", "property_type", "description_clean")

@pytest.mark.parametrize("parser_cls", ALL_PARSERS)
def test_every_parser_returns_required_fields(parser_cls, request):
    src = parser_cls.source
    html = (FIXTURES / f"{src}_sample_01.html").read_text(encoding="utf-8")
    out = parser_cls().parse(html, f"https://{src}.com/x/1")
    for field in REQUIRED:
        assert field in out, f"{src} missing {field}"
    assert out["source"] == src

@pytest.mark.parametrize("parser_cls", ALL_PARSERS)
def test_every_parser_normalises_property_type(parser_cls):
    src = parser_cls.source
    html = (FIXTURES / f"{src}_sample_01.html").read_text(encoding="utf-8")
    out = parser_cls().parse(html, f"https://{src}.com/x/1")
    assert out["property_type"] in {
        "apartment", "house", "room", "studio", "shophouse", "townhouse", "other"}
```

- [ ] **Step 3: Run, confirm failure, implement the three parsers**

- [ ] **Step 4: Write `config/property_type_map.yaml`** mapping each source's raw
      category strings to the controlled vocabulary. Unmapped values → `other`, with a
      logged warning.

- [ ] **Step 5: Run the full parser suite**

Run: `pytest tests/test_parsers.py -v`
Expected: 8 passed

- [ ] **Step 6: Add nhatot's Playwright fallback** — `scrapy-playwright` enabled only
      for that spider, with `CLOSESPIDER_PAGECOUNT` capped at 20,000/day so it cannot
      consume the whole crawl budget.

- [ ] **Step 7: Start all four spiders on the cron and verify**

```bash
aws s3 ls s3://vn-rental-dsp/bronze/listings/dt=$(date +%F)/ --recursive
```

Expected: four `source=` prefixes present.

- [ ] **Step 8: Commit**

`git add -A && git commit -m "All four spiders and parsers live"`

---

### Task 8: Report 1 — Project Proposal

**Files:**
- Create: `reports/report1-proposal.md` (or .docx)

- [ ] **Step 1:** Adapt Charter §2 (big data context), §3 (problem + approach), §4
      (data requirements and collection), and Spec §1 (architecture diagram).
- [ ] **Step 2:** State the primary approach as **Predictive with a Prescriptive
      extension**, and justify why Descriptive alone would be insufficient.
- [ ] **Step 3:** Include the robots.txt compliance table from the governance doc —
      it demonstrates diligence that most proposals lack.
- [ ] **Step 4:** Include the volume/velocity/variety arithmetic from Spec §5, showing
      the calculation rather than asserting the conclusion.
- [ ] **Step 5:** Add a screenshot of live bronze data in S3. Proposing a pipeline is
      ordinary; showing one already running at proposal time is not.
- [ ] **Step 6: Freeze the provisional panel cohort** (insurance against Risk R-04)

```bash
python -m crawler.frontier --freeze-panel --size 50000 --label provisional
```

Whatever has been crawled by Sep 20 becomes a smaller cohort that starts its
observation window five days before the full one. It costs one command and buys five
extra days of survival observation — the single cheapest risk mitigation in the project.

- [ ] **Step 7: Commit and submit.**

**🚩 GATE — End of Week 1:** crawler live, ≥ 50k listings in bronze, provisional cohort
frozen, Report 1 submitted.

---

## Phase 2 — Bronze to Silver (Week 2)

### Task 9: Spark parse job (bronze → silver)

**Files:**
- Create: `spark/parse_bronze.py`
- Create: `spark/common.py` — shared session builder and S3 path helpers
- Test: `tests/test_spark_jobs.py::test_parse_bronze_local`

**Interfaces:**
- Consumes: all four parser classes; `bronze/listings/dt=*/source=*`
- Produces:
  - `spark.common.get_spark(app_name: str) -> SparkSession`
  - `spark.common.s3_path(layer: str, *parts) -> str`
  - Output at `silver/listings/province=*/dt=*/` matching Spec §3.2

- [ ] **Step 1: Write the failing local Spark test**

```python
# tests/test_spark_jobs.py
import gzip, base64, json, pytest
from pyspark.sql import SparkSession

@pytest.fixture(scope="session")
def spark():
    s = (SparkSession.builder.master("local[2]")
         .appName("test").config("spark.sql.shuffle.partitions", "2").getOrCreate())
    yield s
    s.stop()

def _bronze_row(html: str, listing_id: str) -> dict:
    return {
        "listing_id": listing_id, "source": "batdongsan",
        "url": f"https://batdongsan.com.vn/x/{listing_id}",
        "crawl_ts": "2026-09-20T03:00:00Z", "http_status": 200,
        "content_hash": "sha256:abc",
        "html_gz_b64": base64.b64encode(gzip.compress(html.encode())).decode(),
        "fetch_ms": 500, "crawl_kind": "discovery",
    }

def test_parse_bronze_local(spark, tmp_path):
    from pathlib import Path
    from spark.parse_bronze import parse_partition
    html = (Path("tests/fixtures/batdongsan_sample_01.html")).read_text(encoding="utf-8")
    src = tmp_path / "bronze"; src.mkdir()
    (src / "part-0000.jsonl").write_text(json.dumps(_bronze_row(html, "batdongsan:pr1")))
    df = spark.read.json(str(src))
    out = parse_partition(df)
    rows = out.collect()
    assert len(rows) == 1
    assert rows[0]["listing_id"] == "batdongsan:pr1"
    assert rows[0]["province"] is not None

def test_parse_bronze_is_idempotent(spark, tmp_path):
    """Running twice over the same input yields identical row counts and hashes."""
    from pathlib import Path
    from spark.parse_bronze import parse_partition
    html = (Path("tests/fixtures/batdongsan_sample_01.html")).read_text(encoding="utf-8")
    src = tmp_path / "bronze"; src.mkdir()
    (src / "part-0000.jsonl").write_text(json.dumps(_bronze_row(html, "batdongsan:pr1")))
    df = spark.read.json(str(src))
    a = [r.asDict() for r in parse_partition(df).collect()]
    b = [r.asDict() for r in parse_partition(df).collect()]
    assert a == b
```

- [ ] **Step 2: Run, confirm failure**

Run: `pytest tests/test_spark_jobs.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'spark.parse_bronze'`

- [ ] **Step 3: Implement `spark/parse_bronze.py`**

Structure it as a `mapInPandas` or `flatMap` over bronze rows that gunzips
`html_gz_b64`, dispatches on `source` to the right parser, and returns silver-schema
rows. Wrap each parse in try/except: a parse failure emits a row with
`parse_ok = False` and the exception class, never kills the job. Explicit output
schema — no `inferSchema` on 7 million rows.

- [ ] **Step 4: Run tests until green**

Run: `pytest tests/test_spark_jobs.py -v`
Expected: 2 passed

- [ ] **Step 5: Add the PII gate**

```python
# in spark/parse_bronze.py, before writing silver
PHONE = r"(?:\+?84|0)[0-9]{9,10}"
leaks = out.filter(F.col("description_clean").rlike(PHONE)).count()
if leaks > 0:
    raise RuntimeError(f"PII gate failed: {leaks} rows contain phone numbers")
```

- [ ] **Step 6: Add the data quality assertion job**

Assert, and fail the job if violated: `asking_rent_vnd` between 300,000 and
500,000,000 or null; `area_sqm` between 5 and 1,000 or null; `province` in the six
expected values; `listing_id` unique within `(dt, source)`; parse success rate ≥ 95%.

- [ ] **Step 7: Run on the real cluster against one day of bronze**

```bash
./infra/submit.sh spark/parse_bronze.py --date 2026-09-20
aws s3 ls s3://vn-rental-dsp/silver/listings/ --recursive --summarize | tail -3
```

Expected wall-clock on the free cluster: ~5–10 minutes for one day's partition
(~30k listings). If a single day takes over 30 minutes, check `spark.sql.shuffle
.partitions` — the default of 200 cripples a 4-core cluster.

- [ ] **Step 8: Commit**

`git add -A && git commit -m "Spark bronze->silver parse job with PII and QA gates"`

---

### Task 10: Free Spark cluster on Oracle Cloud Always Free

**Files:**
- Create: `infra/provision_oracle.md`, `infra/install_spark.sh`, `infra/spark-env.sh`,
  `infra/spark-defaults.conf`, `infra/submit.sh`

**Interfaces:**
- Consumes: `config/aws.yaml` (bucket, region), `config/spark` block
- Produces: `./infra/submit.sh <script.py> [args...]` — rsyncs the repo to the master
  and runs `spark-submit` against the standalone cluster

**Do this task on Monday of Week 2, not later.** If Oracle has no free ARM capacity in
your region, you need the whole week to work through the fallbacks in Spec §9.6.

- [ ] **Step 1: Provision three Always Free Ampere VMs**

In the Oracle Cloud console: Compute → Instances → Create. Shape `VM.Standard.A1.Flex`,
image Ubuntu 24.04 (aarch64).

| Name | OCPU | RAM | Boot volume |
|---|---|---|---|
| `spark-master` | 2 | 12 GB | 80 GB |
| `spark-worker-1` | 1 | 6 GB | 60 GB |
| `spark-worker-2` | 1 | 6 GB | 60 GB |

Total: 4 OCPU / 24 GB / 200 GB — exactly the Always Free ceiling. Creating a fourth
instance, or one byte more storage, starts billing.

**If creation fails with `Out of host capacity`:** this is normal and not an account
problem. Retry in a different availability domain, then a different region. If nothing
succeeds within a day, go to Spec §9.6 and pick a fallback.

- [ ] **Step 2: Open the cluster ports inside the VCN only**

Add ingress rules to the subnet security list for **source CIDR = the subnet's own
range**, never `0.0.0.0/0`:

| Port | Purpose |
|---|---|
| 7077 | Spark master RPC |
| 8080 | Master web UI |
| 4040 | Driver UI |
| 7078, 8081 | Worker RPC and UI |

Ubuntu images also ship `iptables` rules that block these by default:

```bash
sudo iptables -I INPUT -s 10.0.0.0/24 -j ACCEPT
sudo netfilter-persistent save
```

Forgetting this step produces workers that start cleanly and silently never register
with the master. If `http://spark-master:8080` shows zero workers, this is why.

- [ ] **Step 3: Write and run `infra/install_spark.sh` on all three VMs**

```bash
#!/usr/bin/env bash
set -euo pipefail
SPARK_VER=3.5.1
HADOOP_AWS=3.3.4
AWS_SDK=1.12.262

sudo apt-get update
sudo apt-get install -y openjdk-17-jdk python3.11 python3-pip build-essential python3-dev

curl -fsSL "https://archive.apache.org/dist/spark/spark-${SPARK_VER}/spark-${SPARK_VER}-bin-hadoop3.tgz" \
  | sudo tar -xz -C /opt
sudo ln -sfn "/opt/spark-${SPARK_VER}-bin-hadoop3" /opt/spark

# S3A connector — versions MUST match Spark's bundled Hadoop
sudo wget -q -P /opt/spark/jars/ \
  "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_AWS}/hadoop-aws-${HADOOP_AWS}.jar"
sudo wget -q -P /opt/spark/jars/ \
  "https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK}/aws-java-sdk-bundle-${AWS_SDK}.jar"

python3 -m pip install --break-system-packages \
  pyspark==3.5.1 lxml pyyaml h3 pandas pyarrow boto3
```

- [ ] **Step 4: Verify the ARM wheels actually installed**

```bash
python3 -c "import lxml, h3, pyarrow, pandas; print('ok', h3.__version__)"
```

Expected: `ok 4.x`. If `h3` fails to build, pin an older version with a published
aarch64 wheel rather than fighting the source build.

- [ ] **Step 5: Write `infra/spark-defaults.conf` and start the cluster**

Copy the configuration block from Spec §9.2 to `/opt/spark/conf/spark-defaults.conf` on
all three VMs, with AWS keys in `/opt/spark/conf/spark-env.sh` at mode `600`.

```bash
# on spark-master
/opt/spark/sbin/start-master.sh
# on each worker
/opt/spark/sbin/start-worker.sh spark://spark-master:7077
```

- [ ] **Step 6: Confirm all three nodes registered**

Open `http://spark-master:8080` through an SSH tunnel.
Expected: **2 workers ALIVE, 2 cores total, 8 GB memory total.** If you see 0 workers,
return to Step 2.

- [ ] **Step 7: Verify S3 access from the cluster — the step that actually fails**

```bash
/opt/spark/bin/spark-submit --master spark://spark-master:7077 \
  -c "spark.driver.memory=2g" \
  /opt/vn-rental-dsp/infra/smoke_s3.py
```

where `smoke_s3.py` is:

```python
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("smoke").getOrCreate()
df = spark.read.json("s3a://vn-rental-dsp/bronze/listings/dt=2026-09-20/")
print("ROWS:", df.count())
spark.stop()
```

Expected: a row count. Two failures are near-certain on the first attempt:

| Error | Cause | Fix |
|---|---|---|
| `NoSuchMethodError` or `ClassNotFoundException: S3AFileSystem` | `hadoop-aws` version ≠ Spark's bundled Hadoop | Check `ls /opt/spark/jars/hadoop-common-*.jar` and match exactly |
| `403 Forbidden` | IAM user lacks `s3:ListBucket` on the bucket *itself*, not just objects | Add both `arn:aws:s3:::vn-rental-dsp` and `.../*` to the policy |

- [ ] **Step 8: Write `infra/submit.sh`**

```bash
#!/usr/bin/env bash
set -euo pipefail
JOB="$1"; shift
rsync -az --exclude '.git' --exclude 'tests/fixtures' ./ spark-master:/opt/vn-rental-dsp/
ssh spark-master "cd /opt/vn-rental-dsp && /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 --deploy-mode client $JOB $*"
```

- [ ] **Step 9: Capture cluster screenshots now, while it is fresh**

Save the master UI showing three live nodes, and one job's Spark UI showing stages
distributed across workers. Report 2's "Big Data" section needs this evidence, and
recreating it in Week 6 wastes an hour.

- [ ] **Step 10: Commit**

`git add -A && git commit -m "Free 3-node Spark standalone cluster on Oracle Always Free"`

---

### Task 11: Deduplication (MinHash LSH)

**Files:**
- Create: `spark/dedupe.py`
- Test: `tests/test_spark_jobs.py::test_dedupe_clusters_reposts`

**Interfaces:**
- Consumes: `silver/listings/`
- Produces: `gold/listing_dim/` with `canonical_id`, `cluster_size`, `is_canonical`;
  `dedupe.build_clusters(df) -> DataFrame` with columns
  `(listing_id, canonical_id, cluster_size)`

**Why this is a real task and not a nicety:** an unmerged re-post makes the original
look like it rented and the copy look newly listed. Every survival label downstream
depends on getting this right.

- [ ] **Step 1: Write the failing test**

```python
def test_dedupe_clusters_reposts(spark):
    from spark.dedupe import build_clusters
    rows = [
        ("a", "HCM", "Q7", 25.0, 7_000_000, "phòng trọ đẹp gần lotte mart có gác"),
        ("b", "HCM", "Q7", 25.0, 7_000_000, "phòng trọ đẹp gần lotte mart có gác"),
        ("c", "HCM", "Q7", 60.0, 20_000_000, "căn hộ cao cấp view sông 2 phòng ngủ"),
    ]
    df = spark.createDataFrame(
        rows, "listing_id string, province string, district string, "
              "area_sqm double, asking_rent_vnd long, description_clean string")
    out = {r["listing_id"]: r["canonical_id"] for r in build_clusters(df).collect()}
    assert out["a"] == out["b"], "identical re-posts must share a canonical_id"
    assert out["c"] != out["a"], "a distinct listing must not be merged"
```

- [ ] **Step 2: Run, confirm failure, implement**

Pipeline: blocking key `concat(province, district, round(area_sqm), round(rent,-5))` →
`Tokenizer` → `NGram(n=3)` on characters → `HashingTF` → `MinHashLSH(numHashTables=5)`
→ `approxSimilarityJoin(threshold=0.20)` (Jaccard distance, so 0.20 distance = 0.80
similarity) → connected components → `canonical_id` = `min(listing_id)` per component.

- [ ] **Step 3: Run tests until green**

Run: `pytest tests/test_spark_jobs.py::test_dedupe_clusters_reposts -v`
Expected: 1 passed

- [ ] **Step 4: Run on real data and eyeball 20 clusters manually**

Sample 20 multi-member clusters and read them. If unrelated listings are being merged,
raise the similarity threshold; if obvious re-posts are being missed, lower it. Record
the chosen threshold and the reasoning — Report 2 should justify it, not just state it.

- [ ] **Step 5: Log the duplicate rate.** Expect 10–25%. A rate under 3% means the
      blocking key is too strict and the join is finding nothing.

- [ ] **Step 6: Commit**

`git add -A && git commit -m "MinHash LSH near-duplicate detection and canonicalisation"`

---

### Task 12: Freeze the panel cohort

**Files:**
- Modify: `crawler/frontier.py` — `freeze_panel_cohort` (already tested in Task 4)
- Create: `crawler/panel_scheduler.py`
- Test: `tests/test_panel_scheduler.py`

**Interfaces:**
- Consumes: `Frontier.panel_due(as_of)`, `S3BatchWriter`
- Produces: daily `bronze/probes/dt=<date>/` records with
  `(listing_id, url, obs_date, http_status, is_present)`

**⏰ Run this on 2026-09-25 (end of Week 2) and not later.** The cohort's observation
window starts the day it is frozen.

- [ ] **Step 1: Write the failing test**

```python
# tests/test_panel_scheduler.py
import datetime as dt
from crawler.panel_scheduler import classify_probe

def test_200_is_present():
    assert classify_probe(200, "<html>...listing...</html>")["is_present"] is True

def test_404_is_absent():
    assert classify_probe(404, "")["is_present"] is False

def test_soft_delete_page_is_absent():
    html = "<html><body>Tin đăng không tồn tại hoặc đã bị gỡ</body></html>"
    assert classify_probe(200, html)["is_present"] is False
```

The third test matters most: these sites return HTTP 200 with a "listing removed"
page rather than a 404. Treating that as "present" would silently censor every real
event in the panel.

- [ ] **Step 2: Run, confirm failure, implement `classify_probe`**

Check status first; then match the page body against a list of removal phrases held in
`config/sources.yaml` per source (`tin đăng không tồn tại`, `đã bị gỡ`,
`không tìm thấy`, `hết hạn`).

- [ ] **Step 3: Run tests until green**

Run: `pytest tests/test_panel_scheduler.py -v`
Expected: 3 passed

- [ ] **Step 4: Freeze the cohort**

```bash
python -m crawler.frontier --freeze-panel --size 200000
sqlite3 /var/data/frontier.db "SELECT source, COUNT(*) FROM urls WHERE in_panel=1 GROUP BY source;"
```

Expected: ~200,000 total, stratified across sources and provinces.

- [ ] **Step 5: Add the daily panel cron**

```bash
0 4 * * *  cd /opt/vn-rental-dsp && python -m crawler.panel_scheduler --run >> /var/log/panel.log 2>&1
```

- [ ] **Step 6: Verify two consecutive days of probes land in S3**

Do not proceed to Week 3 until two days of `bronze/probes/` exist. This is the asset
the entire Report 3 rests on.

- [ ] **Step 7: Commit**

`git add -A && git commit -m "Panel cohort freeze and daily liveness probing"`

**🚩 GATE — End of Week 2:** ≥ 300k listings in bronze, silver layer building, panel
cohort frozen and probing daily.

---

## Phase 3 — Panel, Enrichment, EDA (Week 3)

### Task 13: Build the panel fact table

**Files:**
- Create: `spark/build_panel.py`
- Test: `tests/test_spark_jobs.py::test_panel_survival_labels`

**Interfaces:**
- Consumes: `bronze/probes/`, `gold/listing_dim/`
- Produces: `gold/panel_fact/` (Spec §3.3); `build_panel.survival_labels(panel_df,
  censor_date) -> DataFrame` with `(listing_id, first_seen, duration_days,
  event_observed, left_truncated)`

- [ ] **Step 1: Write the failing test — this encodes the censoring logic**

```python
import datetime as dt

def test_panel_survival_labels(spark):
    from spark.build_panel import survival_labels
    d = lambda s: dt.date.fromisoformat(s)
    rows = [
        # listing a: present 3 days, then absent twice -> event observed at day 3
        ("a", d("2026-09-25"), True), ("a", d("2026-09-26"), True),
        ("a", d("2026-09-27"), True), ("a", d("2026-09-28"), False),
        ("a", d("2026-09-29"), False),
        # listing b: present throughout -> right-censored
        ("b", d("2026-09-25"), True), ("b", d("2026-09-26"), True),
        ("b", d("2026-09-27"), True), ("b", d("2026-09-28"), True),
        ("b", d("2026-09-29"), True),
        # listing c: one absence then present again -> NOT an event (transient failure)
        ("c", d("2026-09-25"), True), ("c", d("2026-09-26"), False),
        ("c", d("2026-09-27"), True), ("c", d("2026-09-28"), True),
        ("c", d("2026-09-29"), True),
    ]
    df = spark.createDataFrame(rows, "listing_id string, obs_date date, is_present boolean")
    out = {r["listing_id"]: r for r in
           survival_labels(df, censor_date=d("2026-09-29")).collect()}

    assert out["a"]["event_observed"] is True
    assert out["a"]["duration_days"] == 3
    assert out["b"]["event_observed"] is False
    assert out["b"]["duration_days"] == 4
    assert out["c"]["event_observed"] is False, "a single absence must not fire an event"
```

- [ ] **Step 2: Run, confirm failure**

Run: `pytest tests/test_spark_jobs.py::test_panel_survival_labels -v`
Expected: FAIL — `ImportError: cannot import name 'survival_labels'`

- [ ] **Step 3: Implement using a window function**

Partition by `listing_id`, order by `obs_date`, use `lead(is_present, 1)` to find the
first date where both the current and next observation are absent. Join the result to
`gold/listing_dim` on `canonical_id` so a cluster's event is the disappearance of its
**last** surviving member.

- [ ] **Step 4: Run tests until green**

Run: `pytest tests/test_spark_jobs.py -v`
Expected: all passed

- [ ] **Step 5: Run on real data and check the event rate**

```sql
-- via Athena
SELECT event_observed, COUNT(*) FROM gold.features_survival GROUP BY event_observed;
```

Expected: ≥ 15% uncensored by end of Week 3, trending toward ≥ 25% by Week 5.
**If below 8% at this checkpoint, escalate to Risk R-04** — the cohort is turning over
too slowly and the model will have too few events to learn from.

- [ ] **Step 6: Commit**

`git add -A && git commit -m "Panel fact table and right-censored survival labels"`

---

### Task 14: Geographic enrichment

**Files:**
- Create: `spark/geo_enrich.py`, `scripts/fetch_osm.py`
- Test: `tests/test_geo.py`

**Interfaces:**
- Consumes: `silver/listings/` (lat/lon), Overpass API
- Produces: `gold/geo_enrichment/` with `h3_res8`, `dist_to_cbd_km`,
  `dist_to_nearest_university_km`, `dist_to_nearest_industrial_zone_km`,
  `poi_count_1km`, plus `geo_enrich.assign_h3(df, res=8) -> DataFrame`

- [ ] **Step 1: Fetch OSM POIs once, cache to S3**

`scripts/fetch_osm.py` queries Overpass for universities, industrial zones, markets,
hospitals, and bus stations in the six provinces; writes to
`s3://vn-rental-dsp/reference/osm_pois.parquet`. Run once; never in a Spark job.

- [ ] **Step 2: Write the failing test**

```python
# tests/test_geo.py
def test_assign_h3_is_stable(spark):
    from spark.geo_enrich import assign_h3
    df = spark.createDataFrame(
        [("a", 10.7769, 106.7009)], "listing_id string, latitude double, longitude double")
    out = assign_h3(df, res=8).collect()[0]
    assert out["h3_res8"].startswith("8")
    assert len(out["h3_res8"]) == 15

def test_assign_h3_handles_null_coords(spark):
    from spark.geo_enrich import assign_h3
    df = spark.createDataFrame(
        [("a", None, None)], "listing_id string, latitude double, longitude double")
    assert assign_h3(df, res=8).collect()[0]["h3_res8"] is None
```

- [ ] **Step 3: Run, confirm failure, implement with a pandas UDF wrapping `h3`**

- [ ] **Step 4: Handle missing coordinates** — roughly 30–50% of listings will lack
      lat/lon. Fall back to the district centroid and set `geo_precision` to
      `'exact' | 'district'`. Carry `geo_precision` as a model feature; do not silently
      pretend a centroid is a real coordinate.

- [ ] **Step 5: Run tests until green, then run the job on real data**

- [ ] **Step 6: Commit**

`git add -A && git commit -m "H3 indexing and OSM proximity enrichment"`

---

### Task 15: Exploratory Data Analysis

**Files:**
- Create: `analysis/01_eda_market_structure.ipynb`,
  `analysis/02_eda_geography.ipynb`
- Create: `reports/figures/` (exported PNG/SVG at 300 dpi)

**Interfaces:**
- Consumes: `gold/` tables via Athena and Spark
- Produces: figures referenced by Report 2

- [ ] **Step 1: Market structure notebook.** Listings by source, province, property
      type; rent distributions (log scale — raw VND is unreadable); rent-per-m²
      by district; missingness heatmap; duplicate-rate by source.

- [ ] **Step 2: Geography notebook.** Folium choropleth of median rent-per-m² by
      district; H3 hexbin density map; distance-to-CBD vs. rent scatter with a LOWESS
      fit; the industrial-belt contrast (Binh Duong/Dong Nai vs. HCMC).

- [ ] **Step 3: Temporal notebook section.** New listings per day; panel survival
      curve (Kaplan–Meier) overall and split by price quintile. **This chart is the
      project's thesis in one image** — if cheaper listings visibly rent faster, the
      whole premise is validated in a single figure.

- [ ] **Step 4: Write down five concrete findings** with numbers attached. Not "rents
      vary by district" but "median rent-per-m² in District 1 is 2.3× that of District
      12". Report 2 needs findings, not descriptions of charts.

- [ ] **Step 5: Export every figure at 300 dpi** to `reports/figures/`.

- [ ] **Step 6: Commit**

`git add -A && git commit -m "EDA notebooks and Report 2 figures"`

---

### Task 16: Report 2 — Data Tasks

**Files:**
- Create: `reports/report2-data-tasks.md`

- [ ] **Step 1:** Collection architecture — sitemap-driven frontier, politeness
      controls, panel design. Include the crawler architecture diagram.
- [ ] **Step 2:** Storage design — medallion layers, partitioning strategy with the
      reasoning, small-file management, actual sizes measured from S3.
- [ ] **Step 3:** Cleaning methodology — Vietnamese normalisation, price parsing edge
      cases, PII stripping, deduplication with the threshold justification.
- [ ] **Step 4:** **Explicit Spark/Hadoop justification.** Give the measured numbers:
      rows processed, cluster configuration, wall-clock runtime, and a single-node
      extrapolation showing why it is infeasible. This section is what the "Big Data"
      column in the grading sheet is asking for — answer it with evidence.
- [ ] **Step 5:** EDA findings with the figures from Task 15.
- [ ] **Step 6:** Data quality assessment — parse rates, missingness, known biases,
      the Facebook coverage gap stated honestly as a limitation.
- [ ] **Step 7: Commit and submit.**

**🚩 GATE — End of Week 3:** ≥ 500k listings, silver+gold built, ≥ 10 days panel data,
Report 2 submitted.

---

## Phase 4 — Modelling (Weeks 4–5)

### Task 17: Feature engineering

**Files:**
- Create: `spark/features.py`
- Test: `tests/test_features.py`

**Interfaces:**
- Consumes: `gold/listing_dim/`, `gold/geo_enrichment/`
- Produces: `gold/features_rent/`;
  `features.build_rent_features(df) -> DataFrame`,
  `features.temporal_split(df, cutoff: date) -> (train_df, test_df)`

- [ ] **Step 1: Write the failing test for the temporal split**

```python
# tests/test_features.py
import datetime as dt

def test_temporal_split_has_no_overlap(spark):
    from spark.features import temporal_split
    d = lambda s: dt.date.fromisoformat(s)
    df = spark.createDataFrame(
        [("a", d("2026-09-20")), ("b", d("2026-10-05"))],
        "listing_id string, first_seen date")
    train, test = temporal_split(df, cutoff=d("2026-10-01"))
    assert [r["listing_id"] for r in train.collect()] == ["a"]
    assert [r["listing_id"] for r in test.collect()] == ["b"]

def test_temporal_split_excludes_duplicate_clusters_across_sides(spark):
    """A canonical cluster must live entirely on one side of the split."""
    from spark.features import temporal_split
    d = lambda s: dt.date.fromisoformat(s)
    df = spark.createDataFrame(
        [("a", d("2026-09-20"), "c1"), ("b", d("2026-10-05"), "c1")],
        "listing_id string, first_seen date, canonical_id string")
    train, test = temporal_split(df, cutoff=d("2026-10-01"))
    assert test.count() == 0, "cluster c1 already appears in train; it must not leak"
```

The second test is the leakage guard. Without it, a re-post of a training listing
lands in the test set and the reported MAPE becomes fiction.

- [ ] **Step 2: Run, confirm failure, implement**

Features to build:
- Numeric: `area_sqm`, `bedrooms`, `bathrooms`, `log_area`, `rooms_per_sqm`
- Geo: `dist_to_cbd_km`, `dist_to_university_km`, `dist_to_industrial_km`,
  `poi_count_1km`, `h3_res8` (as a categorical via target encoding)
- Categorical: `property_type`, `furnishing`, `source`, `district`, `geo_precision`
  → `StringIndexer` → `OneHotEncoder`
- Text: `description_clean` → `Tokenizer` → `HashingTF(numFeatures=2**14)` → `IDF`
- Amenity flags: a fixed vocabulary of ~25 binary indicators
  (`has_aircon`, `has_loft`, `private_bathroom`, `allows_cooking`, `no_curfew`, …)
- Target: `log(asking_rent_vnd)` — log-transform, because rent is right-skewed and
  squared error on raw VND is dominated by luxury outliers

- [ ] **Step 3: Run tests until green**

Run: `pytest tests/test_features.py -v`
Expected: 2 passed

- [ ] **Step 4: Assemble with `VectorAssembler`, persist to `gold/features_rent/`**

- [ ] **Step 5: Commit**

`git add -A && git commit -m "Feature engineering with leakage-safe temporal split"`

---

### Task 18: Stage 1 — Fair rent model

**Files:**
- Create: `spark/train_rent_model.py`, `spark/score_price_gap.py`
- Test: `tests/test_models.py::test_baseline_beats_nothing`

**Interfaces:**
- Consumes: `gold/features_rent/`
- Produces: `models/rent_gbt_v*/` (Spark ML `PipelineModel`),
  `models/metrics/rent_*.json`, `gold/features_survival/` with `price_gap` attached

- [ ] **Step 1: Build the baseline first, and record its score**

District-median rent-per-m² × area. One line of SQL. **Do not skip this.** Every
subsequent model claim is measured against it, and a gradient-boosted tree that fails
to beat a median is telling you something important about your features.

```python
# tests/test_models.py
def test_baseline_beats_nothing(spark):
    from spark.train_rent_model import district_median_baseline, mape
    df = spark.createDataFrame(
        [("HCM", "Q1", 50.0, 20_000_000), ("HCM", "Q1", 100.0, 40_000_000),
         ("HCM", "Q7", 50.0, 10_000_000)],
        "province string, district string, area_sqm double, asking_rent_vnd long")
    preds = district_median_baseline(df)
    assert mape(preds) < 0.5
```

- [ ] **Step 2: Run, confirm failure, implement the baseline and the MAPE helper**

- [ ] **Step 3: Train `GBTRegressor`** on the temporal split.
      Start with `maxDepth=6, maxIter=100, stepSize=0.1`.

- [ ] **Step 4: Evaluate and record** MAPE, RMSE (in VND, after
      back-transforming from log), R², and MAPE broken out by province and by price
      decile. A model that is excellent overall and terrible on cheap rooms is a
      finding worth reporting, not a defect to hide.

- [ ] **Step 5: Tune with `CrossValidator`**, 3 folds, over
      `maxDepth ∈ {4,6,8}` × `maxIter ∈ {50,100,200}`. Use `TrainValidationSplit`
      instead if the cross-validation exceeds 40 minutes — the budget matters more
      than the last 1% of MAPE.

- [ ] **Step 6: Verify the success criterion** — MAPE ≤ 20% and ≥ 30% relative
      improvement over baseline. If not met, the likely cause in order of probability:
      (a) duplicate leakage not fully removed, (b) `thỏa thuận` listings polluting the
      training set, (c) missing coordinates making location features weak.

- [ ] **Step 7: Score all listings and write `price_gap`** to
      `gold/features_survival/`.

- [ ] **Step 8: Commit**

`git add -A && git commit -m "Fair-rent GBT model, baseline comparison, price_gap scoring"`

---

### Task 19: Stage 2 — Survival model

**Files:**
- Create: `spark/train_survival.py`
- Test: `tests/test_models.py::test_aft_handles_censoring`

**Interfaces:**
- Consumes: `gold/features_survival/`
- Produces: `models/survival_aft_v*/`, `models/metrics/survival_*.json`;
  `train_survival.concordance_index(preds_df) -> float`

- [ ] **Step 1: Write the failing test**

```python
def test_aft_handles_censoring(spark):
    """AFT must accept censored rows and produce finite predictions for them."""
    from spark.train_survival import fit_aft
    from pyspark.ml.linalg import Vectors
    rows = [(Vectors.dense([0.1]), 10.0, 1.0), (Vectors.dense([0.5]), 25.0, 0.0),
            (Vectors.dense([0.2]), 12.0, 1.0), (Vectors.dense([0.8]), 30.0, 0.0)]
    df = spark.createDataFrame(rows, ["features", "duration_days", "event_observed"])
    model = fit_aft(df)
    preds = model.transform(df).collect()
    assert all(p["prediction"] > 0 and p["prediction"] < 1e6 for p in preds)
```

Note the convention: in Spark's `AFTSurvivalRegression`, `censorCol` is **1 for an
observed event and 0 for censored** — the opposite of several R packages. Getting this
backwards trains the model on exactly the wrong population, and it will not error.

- [ ] **Step 2: Run, confirm failure, implement `fit_aft`**

```python
from pyspark.ml.regression import AFTSurvivalRegression

def fit_aft(df, features_col="features"):
    aft = AFTSurvivalRegression(
        featuresCol=features_col,
        labelCol="duration_days",
        censorCol="event_observed",   # 1.0 = event observed, 0.0 = censored
        quantileProbabilities=[0.25, 0.5, 0.75],
        quantilesCol="quantiles",
    )
    return aft.fit(df)
```

- [ ] **Step 3: Run tests until green**

Run: `pytest tests/test_models.py -v`
Expected: 2 passed

- [ ] **Step 4: Train on real data with `price_gap` included, then implement
      concordance index** and verify ≥ 0.65.

- [ ] **Step 5: Run the ablation that answers the project's core question**

Fit twice — with and without `price_gap` — and compare concordance. **The delta is the
headline result of Report 3.** If `price_gap` adds nothing, say so plainly; a
well-evidenced negative result is a legitimate finding and defends well orally.

- [ ] **Step 6: Sensitivity analysis on left-truncation.** Re-fit excluding
      `left_truncated` rows. Report both; if conclusions diverge, that is a finding.

- [ ] **Step 7: Commit**

`git add -A && git commit -m "AFT survival model, concordance, price_gap ablation"`

---

### Task 20: Prescriptive layer

**Files:**
- Create: `spark/prescriptive.py`
- Test: `tests/test_prescriptive.py`

**Interfaces:**
- Consumes: both trained models
- Produces: `recommend_price(listing_features, vacancy_cost_per_day) -> dict` with
  `optimal_rent_vnd`, `expected_days`, `expected_revenue_90d`, and a price-vs-days
  curve for plotting

- [ ] **Step 1: Write the failing test**

```python
# tests/test_prescriptive.py
def test_recommend_price_returns_monotone_curve(spark):
    """Higher asking price must never predict a shorter time-on-market."""
    from spark.prescriptive import price_days_curve
    curve = price_days_curve(
        base_features={"area_sqm": 30.0, "district": "Q7", "property_type": "room"},
        fair_rent=7_000_000, grid=[0.8, 0.9, 1.0, 1.1, 1.2])
    days = [pt["expected_days"] for pt in curve]
    assert days == sorted(days), f"curve not monotone: {days}"

def test_optimal_price_maximises_90day_revenue():
    from spark.prescriptive import pick_optimal
    curve = [
        {"rent": 6_000_000, "expected_days": 5},
        {"rent": 7_000_000, "expected_days": 12},
        {"rent": 9_000_000, "expected_days": 60},
    ]
    best = pick_optimal(curve, horizon_days=90)
    assert best["rent"] == 7_000_000
```

- [ ] **Step 2: Run, confirm failure, implement**

`price_days_curve` varies `price_gap` across the grid, re-predicts with the AFT model,
and returns the resulting curve. `pick_optimal` maximises
`rent × (horizon_days − expected_days) / 30`.

- [ ] **Step 3: Handle the monotonicity violation** — tree/AFT combinations can
      produce non-monotone curves from noise. Apply isotonic smoothing over the grid
      and **state in the report that this constraint was imposed**, rather than
      presenting a raw curve that implies raising rent makes a unit rent faster.

- [ ] **Step 4: Generate a worked example for the report** — take three real listings
      (cheap room, mid apartment, expensive unit), show the full curve and
      recommendation for each. This is the slide that lands in the oral presentation.

- [ ] **Step 5: Build the mispricing alert table** — the 100 listings with the most
      negative `price_gap` and high predicted liquidity, i.e. genuine bargains.

- [ ] **Step 6: Commit**

`git add -A && git commit -m "Prescriptive pricing recommendation with isotonic smoothing"`

---

### Task 21: Interpretation and visualisation

**Files:**
- Create: `analysis/03_model_interpretation.ipynb`,
  `analysis/04_survival_analysis.ipynb`
- Create: `reports/figures/` additions

- [ ] **Step 1: Feature importance** from the GBT model, plotted and interpreted in
      words. Name the top 10 drivers of rent and explain each in one sentence.
- [ ] **Step 2: Partial dependence** for `area_sqm`, `dist_to_cbd_km`, and
      `dist_to_university_km`, computed by grid-scoring on a sample.
- [ ] **Step 3: Kaplan–Meier curves** split by `price_gap` quartile. If the curves
      separate cleanly, this single figure carries Report 3.
- [ ] **Step 4: Residual analysis** — where the rent model fails, by district, price
      band, and source. Failure modes are a finding.
- [ ] **Step 5: The prescriptive worked examples** from Task 20.
- [ ] **Step 6: A district-level interactive folium map** — median rent, predicted
      liquidity, bargain count per district. This is the portfolio artefact.
- [ ] **Step 7: Commit**

`git add -A && git commit -m "Model interpretation and Report 3 figures"`

---

### Task 22: Report 3 — Model & Results

**Files:**
- Create: `reports/report3-models.md`

- [ ] **Step 1:** Model development — feature engineering, the two-stage design, the
      reasoning for AFT over ordinary regression.
- [ ] **Step 2:** Evaluation — baseline comparison table, temporal split rationale,
      the leakage guard, all metrics with breakdowns.
- [ ] **Step 3:** Fine-tuning — the CV grid, what changed, what it bought.
- [ ] **Step 4:** Interpretation — feature importance, PDPs, KM curves, residuals.
- [ ] **Step 5:** The `price_gap` ablation result, stated plainly whichever way it went.
- [ ] **Step 6:** Conclusions and recommendations — the prescriptive output, the
      bargain table, and district-level findings.
- [ ] **Step 7:** Limitations — disappearance ≠ rented, left-truncation, coverage bias
      from excluding social channels, six-week observation window.
- [ ] **Step 8: Commit and submit.**

**🚩 GATE — End of Week 5:** both models meet criteria, Report 3 submitted.

---

## Phase 5 — Consolidation (Week 6)

### Task 23: Repository and reproducibility polish

- [ ] **Step 1:** Write a real `README.md` — architecture diagram, setup, the exact
      command sequence to re-run the pipeline end to end, sample outputs.
- [ ] **Step 2:** Verify reproducibility on a clean checkout. Follow your own README
      literally. Every step that fails is a step a recruiter would also hit.
- [ ] **Step 3:** `pytest tests/ -v` — the full suite green. Record the count.
- [ ] **Step 4:** Scrub the repo for credentials: `git log -p | grep -iE "AKIA|secret|password"`.
- [ ] **Step 5:** Add `LICENSE` (MIT) and a `DATA_ETHICS.md` summarising the
      governance document. Recruiters notice this; most portfolio projects lack it.
- [ ] **Step 6: Commit and push.**

---

### Task 24: Report 4 and the oral presentation

**Files:**
- Create: `reports/report4-final.md`, `presentation/capstone.pptx`

- [ ] **Step 1:** Consolidate Reports 1–3, resolving contradictions between what was
      proposed in Week 1 and what was actually built. Where the design changed,
      **say so and say why** — documented course-correction reads as engineering
      judgement, not failure.
- [ ] **Step 2:** Build the deck, ~15 slides:
      1–2 problem and why it matters · 3 why this is big data, with the numbers ·
      4 architecture diagram · 5–6 collection and the panel design ·
      7–8 EDA highlights and the map · 9 the two-stage model design ·
      10–11 results and the KM curves · 12 the prescriptive worked example ·
      13 limitations · 14 what I would do next · 15 the repository link.
- [ ] **Step 3:** Prepare for the four questions you will be asked:
      - *"Is this really big data?"* → the volume/velocity/variety arithmetic, the
        measured cluster runtimes, and the single-node infeasibility extrapolation.
      - *"Why is disappearance the same as rented?"* → it is not; here is the
        assumption, here is the sensitivity analysis, here is the bound on the bias.
      - *"Was the crawling legal and ethical?"* → the robots.txt compliance matrix,
        the rate limits, the PII policy, and why Facebook was excluded.
      - *"What would you do with more time?"* → a real answer, not a list of buzzwords.
- [ ] **Step 4:** Rehearse to time. Twice.
- [ ] **Step 5:** Final commit and tag: `git tag v1.0-capstone`.

**🚩 GATE — End of Week 6:** all four reports submitted, presentation delivered,
repository public.

---

## Self-Review

### Spec coverage

| Spec requirement | Task |
|---|---|
| Medallion S3 layout (§2) | Task 2, 9 |
| bronze/silver/gold schemas (§3) | Task 9, 11, 13, 17 |
| Scrapy + AutoThrottle + robots (§4) | Task 6 |
| Sitemap-driven frontier (§4) | Task 4, 5 |
| Panel design, 2-failure rule (§7.3) | Task 12, 13 |
| MinHash LSH dedup (§7.1) | Task 11 |
| Vietnamese normalisation + PII (§7.2) | Task 3, 9 |
| AFT survival with censoring (§4, §7.3) | Task 19 |
| Temporal split, no leakage (§8) | Task 17 |
| Idempotency verification (§8) | Task 9 |
| PII automated gate (§8) | Task 9 |
| Cost guardrails (§5) | Task 2, 10 |
| H3 + OSM enrichment (§4) | Task 14 |
| Free Spark cluster on Oracle (§1, §9) | Task 10 |
| S3A connector config (§9.2) | Task 10 |
| ARM wheel verification (§9.4) | Task 10 |

No uncovered requirements.

### Placeholder scan

No TBD/TODO items. Every step states a concrete action, command, or code block. Test
code is literal, not described.

### Naming consistency

Verified consistent across tasks: `listing_id`, `canonical_id`, `asking_rent_vnd`,
`area_sqm`, `price_gap`, `duration_days`, `event_observed`, `is_present`,
`h3_res8`, `geo_precision`, `parse_ok`, `left_truncated`.

Function names verified: `parse_price_vnd`, `parse_area_sqm`, `strip_pii`,
`normalise_text`, `parse_partition`, `build_clusters`, `classify_probe`,
`survival_labels`, `assign_h3`, `build_rent_features`, `temporal_split`,
`district_median_baseline`, `mape`, `fit_aft`, `concordance_index`,
`price_days_curve`, `pick_optimal`.
