# Technical Design Spec — Vietnam Urban Rental Market Intelligence

**Version:** 1.1 — compute migrated from EMR to Oracle Always Free (2026-09-11)
**Date:** 2026-09-11
**Charter:** `01-project-charter.md`
**Platform:** AWS S3 + Glue + Athena (storage & catalog) · Oracle Cloud Always Free
(Spark compute) · AWS EC2 free tier (crawler)

---

## 1. Architecture Overview

```
┌────────────────────────────────────────────────────────────────────────┐
│  COLLECTION                          EC2 t3.micro (always-on, 24/7)    │
│                                                                        │
│  sitemap_poller ──> frontier.db ──> scrapy_crawler ──> panel_scheduler │
│    (daily)           (SQLite)        (4 spiders)         (daily cron)  │
│                          │                   │                         │
│                          └── state export ───┴──> gzip batches ──┐     │
└──────────────────────────────────────────────────────────────────┼─────┘
                                                                   ▼
┌────────────────────────────────────────────────────────────────────────┐
│  STORAGE — Amazon S3  (s3://vn-rental-dsp/)                            │
│                                                                        │
│  bronze/   raw HTML .jsonl.gz, immutable, partitioned by dt/source     │
│  silver/   parsed Parquet, deduped, PII-stripped, partitioned          │
│  gold/     feature tables + panel facts, Parquet, model-ready          │
│  images/   sampled listing photos                                      │
│  models/   serialised Spark ML pipelines + metrics                     │
└────────────────────────────────────────────────────────────────────────┘
                                   │
                                   ▼
┌────────────────────────────────────────────────────────────────────────┐
│  PROCESSING — Spark standalone cluster on Oracle Cloud Always Free     │
│                                                                        │
│   spark-master   (2 OCPU / 12 GB)  ← driver + master                   │
│   spark-worker-1 (1 OCPU /  6 GB)                                      │
│   spark-worker-2 (1 OCPU /  6 GB)     all reading s3a://vn-rental-dsp  │
│                                                                        │
│  parse_bronze.py ─> dedupe.py ─> build_panel.py ─> features.py         │
│                                                       │                │
│                          ┌────────────────────────────┘                │
│                          ▼                                             │
│  train_rent_model.py ──> score_price_gap.py ──> train_survival.py      │
│                                                  ──> prescriptive.py   │
│                                                                        │
│  Glue Data Catalog  <──  crawler registers silver/gold tables          │
│  Athena             <──  ad-hoc SQL for EDA                            │
└────────────────────────────────────────────────────────────────────────┘
                                   │
                                   ▼
┌────────────────────────────────────────────────────────────────────────┐
│  PRESENTATION                                                          │
│  Jupyter on spark-master · matplotlib/seaborn · folium maps ·          │
│  Streamlit dashboard (stretch) · report figures exported to PNG/SVG    │
└────────────────────────────────────────────────────────────────────────┘
```

### Why this shape

**A free Spark cluster, not managed EMR.** EMR is the obvious choice and it is ruled
out on cost alone: a persistent 3-node cluster for six weeks is roughly $1,100, and
even transient auto-terminating clusters accumulate ~$100 across the project. Oracle
Cloud's Always Free tier provides 4 ARM OCPUs and 24 GB of RAM permanently — not a
12-month trial — which is enough to run a real three-node Spark standalone cluster at
zero cost.

The cluster reads and writes the existing S3 buckets over `s3a://`, so **every PySpark
job in the implementation plan is unchanged**. Only the submit command differs:
`spark-submit --master spark://spark-master:7077` instead of an EMR step. The jobs
remain idempotent, S3-in/S3-out batch units — which was good engineering practice
independent of where they run, and is what makes this substitution a one-line change.

**Three VMs, not one.** Oracle's Ampere allocation can be split into up to four
instances. Splitting it into a master and two workers gives a genuinely distributed
cluster — separate hosts, real network shuffle, real executor scheduling — rather than
a single-node Spark pretending to be a cluster. That distinction matters when Report 2
has to justify the use of distributed computing, and the Spark UI screenshots showing
work distributed across three hosts are the evidence for it.

**The trade-off, stated honestly.** Four ARM cores against EMR's 12 vCPUs makes full
passes roughly 3× slower: a complete re-parse of 1M pages goes from ~40 minutes to
~2–3 hours. This is absorbed by making daily processing **incremental** — only the new
day's bronze partition is parsed each night (~5–10 minutes) — with full re-parses run
overnight and only when a parser changes. Slow-but-free beats fast-but-unaffordable
when the job runs unattended at 2am anyway.

**Medallion (bronze/silver/gold), not a database.** Raw HTML is kept immutable and
forever. Every parser bug found in Week 4 can be fixed by re-running against bronze
rather than re-crawling — and re-crawling is impossible, because the pages have
changed. This single decision is the difference between a recoverable project and an
unrecoverable one.

**SQLite for crawl state, not DynamoDB or RDS.** Crawl state is single-writer,
single-node, and under 2 GB. A managed database would add cost, latency, and failure
modes for no benefit. State is exported to S3 daily so the instance is disposable.

---

## 2. Storage Layout

```
s3://vn-rental-dsp/
├── bronze/
│   ├── listings/dt=2026-09-18/source=batdongsan/part-0000.jsonl.gz
│   ├── listings/dt=2026-09-18/source=phongtro123/part-0000.jsonl.gz
│   ├── sitemaps/dt=2026-09-18/source=mogi/urls.jsonl.gz
│   └── probes/dt=2026-09-18/part-0000.jsonl.gz        # HEAD-style liveness checks
├── silver/
│   ├── listings/province=HCM/dt=2026-09-18/*.parquet   # one row per listing-crawl
│   └── listing_events/province=HCM/*.parquet           # first_seen / gone events
├── gold/
│   ├── listing_dim/*.parquet                           # one row per unique listing
│   ├── panel_fact/*.parquet                            # listing × day observations
│   ├── features_rent/*.parquet                         # Stage 1 training set
│   ├── features_survival/*.parquet                     # Stage 2 training set
│   └── geo_enrichment/*.parquet                        # OSM-derived features
├── images/sampled/listing_id=<id>/img_0.jpg
└── models/
    ├── rent_gbt_v3/                                    # Spark ML PipelineModel
    ├── survival_aft_v2/
    └── metrics/*.json
```

### Partitioning rationale

- **bronze** partitions by `dt` then `source` — writes are append-only per crawl day,
  and re-parsing a single bad source-day is a targeted operation.
- **silver** partitions by `province` then `dt` — almost every analytical query filters
  by city, so this prunes aggressively. Province has 6 values; date has ~40. Neither
  creates small-file problems at this volume.
- **gold** is unpartitioned for `listing_dim` (fits comfortably in memory as a
  broadcast join) and partitioned by `province` for `panel_fact`.

**Small-file discipline:** bronze writes are batched to ~128 MB per file. Spark jobs
call `.coalesce()` sized to produce 128–256 MB output parts. This matters: 7 million
tiny Parquet files would make every subsequent job slower than the crawl itself.

---

## 3. Data Model

### 3.1 bronze.listings (JSONL, one object per fetched page)

```json
{
  "listing_id": "batdongsan:pr32847191",
  "source": "batdongsan",
  "url": "https://batdongsan.com.vn/cho-thue-can-ho-chung-cu-.../pr32847191",
  "crawl_ts": "2026-09-18T03:14:22Z",
  "http_status": 200,
  "content_hash": "sha256:9f2c...",
  "html_gz_b64": "<base64 of gzipped response body>",
  "fetch_ms": 842,
  "crawl_kind": "discovery"
}
```

`crawl_kind` is one of `discovery` (first fetch) or `panel` (daily re-fetch).
`content_hash` enables skipping the parse of unchanged pages — a large saving, since
most listings do not change day to day.

### 3.2 silver.listings (Parquet)

| Column | Type | Notes |
|---|---|---|
| `listing_id` | string | `<source>:<native_id>` |
| `source` | string | partition-adjacent |
| `crawl_date` | date | |
| `asking_rent_vnd` | long | normalised to VND/month |
| `area_sqm` | double | |
| `bedrooms` | int | nullable |
| `bathrooms` | int | nullable |
| `property_type` | string | enum, see 3.5 |
| `province` | string | partition key |
| `district` | string | normalised, diacritic-stripped |
| `ward` | string | nullable |
| `latitude` | double | nullable |
| `longitude` | double | nullable |
| `furnishing` | string | enum: `none`/`basic`/`full`/`unknown` |
| `amenities` | array\<string\> | normalised vocabulary |
| `description_clean` | string | **PII-stripped** |
| `posted_date` | date | nullable |
| `image_count` | int | |
| `content_hash` | string | |
| `is_active` | boolean | |

### 3.3 gold.panel_fact (Parquet) — the core asset

One row per `(listing_id, observation_date)`:

| Column | Type | Notes |
|---|---|---|
| `listing_id` | string | |
| `obs_date` | date | |
| `asking_rent_vnd` | long | tracks price changes over time |
| `is_present` | boolean | observed alive on this date |
| `days_since_first_seen` | int | |
| `price_changed` | boolean | vs. previous observation |

### 3.4 gold.features_survival (Parquet) — one row per listing

| Column | Type | Notes |
|---|---|---|
| `listing_id` | string | |
| `duration_days` | int | first_seen → gone, or → censor_date |
| `event_observed` | boolean | `false` = right-censored |
| `price_gap` | double | `(asking − fair) / fair`, from Stage 1 |
| `initial_rent_vnd` | long | |
| ... | | all Stage 1 features carried forward |

### 3.5 Controlled vocabularies

`property_type` ∈ `{apartment, house, room, studio, shophouse, townhouse, other}`

Source-specific category strings are mapped to this set by an explicit lookup table
committed to the repo (`config/property_type_map.yaml`), **not** by fuzzy matching.
Unmapped values raise a warning and fall through to `other`, with the count logged so
vocabulary drift is visible.

---

## 4. Tech Stack

| Layer | Choice | Why this and not the alternative |
|---|---|---|
| Crawling | **Scrapy 2.11+** | AutoThrottle, sitemap spiders, retry/dedup middleware, and robots.txt obedience are built in. Writing this with `requests` means rebuilding all of it worse. |
| JS rendering | **Playwright** (fallback only) | Needed only where content is client-rendered. Budgeted and capped — it is ~20× the per-page cost. |
| Crawl state | **SQLite (WAL mode)** | Single-writer workload. Zero cost, zero ops. |
| Object storage | **Amazon S3** | Standard tier for hot bronze; lifecycle rule to Intelligent-Tiering at 30 days. |
| Distributed compute | **Spark 3.5 standalone on Oracle Always Free** | 3 VMs (2+1+1 OCPU, 12+6+6 GB). Free permanently. Reads S3 via `s3a://` with `hadoop-aws` + `aws-java-sdk-bundle` on the classpath. |
| Hadoop evidence (optional) | **HDFS + YARN via Docker Compose** on the master VM | Only if Report 2 needs explicit HDFS/YARN screenshots. The data lake stays on S3; this is a demonstration cluster processing a sample. |
| Table format | **Parquet + Snappy** | Delta/Iceberg add operational complexity this project does not need — writes are append-only and idempotent by partition overwrite. |
| Catalog | **AWS Glue Data Catalog** | Free at this scale; makes Athena work with no extra setup. |
| Ad-hoc SQL | **Amazon Athena** | Serverless EDA without paying for a cluster. ~$5/TB scanned — hence the partitioning discipline above. |
| ML | **Spark MLlib** | `GBTRegressor`, `RandomForestRegressor`, `AFTSurvivalRegression`, `CrossValidator`. |
| Text features | **Spark `Tokenizer` → `HashingTF` → `IDF`**, plus `Word2Vec` | Vietnamese-aware normalisation done in a UDF before tokenisation. PhoBERT embeddings are a stretch goal, not baseline. |
| Geo enrichment | **OSMnx / Overpass API**, H3 hexagon indexing | H3 gives a clean spatial join key and a natural aggregation unit for maps. |
| Visualisation | **matplotlib, seaborn, folium, plotly** | Folium for choropleths — the money shot for the presentation. |
| Orchestration | **cron + bash** — crawler cron on EC2, nightly job chain on `spark-master` | Airflow is defensible but costs a week of setup this timeline does not have. A chained bash script that exits non-zero on failure is sufficient for a 7-job DAG. |
| Repo | **Git + GitHub**, `uv` or `venv` for deps | |

### On the survival model

`AFTSurvivalRegression` is available in Spark MLlib and handles right-censoring
natively via its `censorCol`. This is the technically correct tool and it happens to
live inside the required big-data framework — which makes it an easy and genuine
"we used SparkML appropriately" point in Report 3.

A Cox proportional-hazards model is not in MLlib. If proportional hazards is wanted for
comparison, fit it with `lifelines` on a sampled extract and present it as a
single-node cross-check, clearly labelled as such.

---

## 5. Volume and Throughput Budget

### Crawl throughput

```
Target:        200,000 pages/day
             = 2.32 pages/second aggregate
             = 0.58 req/s per domain across 4 domains
Politeness:    ≤ 1 req/s per domain → comfortably within budget
Bandwidth:     200,000 × 150 KB ≈ 30 GB/day inbound
               (EC2 inbound is free; EC2 → S3 same-region is free)
Instance:      t3.micro, 2 vCPU burstable, 1 GB RAM
               Scrapy is I/O-bound; validate CPU credit balance in Week 1
```

**Validation gate (Week 1, Task 6):** if a 1-hour test does not sustain ≥ 8,000
pages/hour, scale to a second instance or drop to a 150k/day target. Do not discover
this in Week 3.

### Storage cost

| Item | Size | Monthly |
|---|---|---|
| S3 Standard (bronze, hot) | ~250 GB | ~$5.75 |
| S3 Intelligent-Tiering (aged) | ~100 GB | ~$1.30 |
| S3 requests (PUT/GET) | ~10M | ~$8.00 |
| **Storage subtotal** | | **~$15/month** |

### Compute cost

| Item | Detail | Cost |
|---|---|---|
| **Oracle Always Free — 3 Ampere VMs** | 4 OCPU / 24 GB RAM / 200 GB block storage, 24/7 | **$0 — permanent, no expiry** |
| EC2 t3.micro crawler, 24/7, 6 weeks | 1,008 hours | ~$0 (free tier) → ~$10 if exceeded |
| Athena | ~200 GB scanned total | ~$1 |
| S3 → Oracle transfer (AWS egress) | ~60 GB total across all passes | **$0** — within AWS's 100 GB/month free egress allowance |
| **Compute subtotal** | | **~$1–11** |

**Total projected: ~$16–26/month**, down from ~$150. Effectively all remaining cost is
S3 storage and request charges.

**The one cost to watch: AWS egress.** Reading S3 from outside AWS is billed as data
transfer out at ~$0.09/GB beyond the free 100 GB/month. A full re-parse pass reads
~25–30 GB, so three full passes a month stays inside the allowance and ten does not.
This is the real reason daily processing is incremental — it is a cost control first
and a performance optimisation second.

**Mitigations if egress becomes tight:**

- Cache `silver/` and `gold/` on the Oracle VMs' local block storage (200 GB, free) and
  read from there during iterative modelling. Only bronze re-parses need to hit S3.
- Write model outputs and metrics locally, then sync to S3 once.
- Run full re-parses at most weekly, and only when a parser has actually changed.

### Cost controls (set these up in Week 1, not after the first surprise bill)

- AWS Budgets alert at **$10, $25, $50** — low thresholds, so any anomaly shows up
  immediately instead of hiding inside expected spend
- CloudWatch alarm on monthly `DataTransfer-Out-Bytes` at 80 GB
- Oracle reclaims Always Free instances that are genuinely idle for 7 days. The crawler
  and nightly jobs keep them active, but confirm all three VMs are still running as
  part of the Monday review
- A hard rule: no cluster larger than 4 nodes without a written reason in the log
- Weekly cost review, recorded in the project log

---

## 6. Repository Structure

```
vn-rental-dsp/
├── README.md
├── pyproject.toml
├── config/
│   ├── sources.yaml               # per-source selectors, sitemap URLs, rate limits
│   ├── property_type_map.yaml     # controlled vocabulary mapping
│   ├── district_aliases.yaml      # district name normalisation
│   └── aws.yaml                   # bucket names, region, cluster sizing
├── crawler/
│   ├── spiders/
│   │   ├── batdongsan.py
│   │   ├── phongtro123.py
│   │   ├── mogi.py
│   │   └── nhatot.py
│   ├── middlewares.py             # throttle, UA, retry, S3 batch writer
│   ├── frontier.py                # SQLite frontier + panel cohort management
│   ├── sitemap_poller.py
│   └── panel_scheduler.py
├── spark/
│   ├── parse_bronze.py
│   ├── dedupe.py
│   ├── build_panel.py
│   ├── geo_enrich.py
│   ├── features.py
│   ├── train_rent_model.py
│   ├── score_price_gap.py
│   ├── train_survival.py
│   └── prescriptive.py
├── parsers/
│   ├── base.py                    # Parser ABC
│   ├── batdongsan.py
│   ├── phongtro123.py
│   ├── mogi.py
│   ├── nhatot.py
│   └── normalise.py               # price/area/text/PII normalisation
├── analysis/
│   ├── 01_eda_market_structure.ipynb
│   ├── 02_eda_geography.ipynb
│   ├── 03_model_interpretation.ipynb
│   └── 04_survival_analysis.ipynb
├── tests/
│   ├── fixtures/                  # saved HTML samples per source
│   ├── test_parsers.py
│   ├── test_normalise.py
│   ├── test_frontier.py
│   └── test_spark_jobs.py
├── infra/
│   ├── setup_s3.sh
│   ├── provision_oracle.md      # VM creation steps, VCN/firewall rules
│   ├── install_spark.sh         # runs on each VM: JDK 17, Spark 3.5, S3A jars
│   ├── spark-env.sh             # master/worker memory + core allocation
│   ├── submit.sh                # spark-submit wrapper (replaces launch_emr.sh)
│   └── docker-compose.hadoop.yml  # optional HDFS/YARN demo cluster
└── docs/
    ├── 01-project-charter.md
    ├── 02-technical-design-spec.md
    ├── 03-implementation-plan.md
    ├── 04-schedule-and-milestones.md
    ├── 05-risk-register.md
    └── 06-data-governance-ethics.md
```

### Boundaries

- **`parsers/` never touches the network.** It takes an HTML string and returns a dict.
  This makes every parser unit-testable against saved fixtures with no crawling, which
  is what allows parser bugs to be fixed and re-run cheaply against bronze.
- **`crawler/` never parses.** It fetches and stores raw bytes. Separation is what
  makes bronze immutable and re-processable.
- **`spark/` jobs are pure S3-in, S3-out** and take `--input`/`--output`/`--date`
  arguments. No job reads configuration from a database or embeds a bucket name.
- **Every Spark job is idempotent**: re-running for a given date overwrites that
  date's partition and produces identical output.

---

## 7. Key Algorithms

### 7.1 Near-duplicate detection

Listings are re-posted constantly — the same unit appears under different IDs, and
sometimes across sources. Without deduplication the panel is corrupted: a re-post looks
like a new listing and the original looks like it rented.

**Approach:** blocking, then similarity.

1. **Block** on `(province, district, round(area_sqm), round(rent, -5))` — reduces
   comparisons from O(n²) to something tractable.
2. **MinHash LSH** (`spark.ml.feature.MinHashLSH`) on shingled description text within
   each block.
3. **Link** pairs with Jaccard ≥ 0.80 into clusters via connected components
   (GraphFrames).
4. **Canonicalise** each cluster: earliest `first_seen` wins; the cluster's disappearance
   event is when the **last** member vanishes.

Step 4 is the one that matters for correctness of the survival labels.

### 7.2 Vietnamese text normalisation

Applied before any tokenisation:

- Unicode NFC normalisation (Vietnamese diacritics have multiple valid encodings —
  this silently breaks string matching if skipped)
- Lowercase; collapse whitespace
- Expand domain abbreviations from a committed dictionary:
  `pt` → `phòng trọ`, `cc` → `chung cư`, `nt` → `nội thất`, `dt` → `diện tích`,
  `wc` → `nhà vệ sinh`, `đc` → `địa chỉ`
- **PII stripping** — phone numbers, emails, Zalo/Facebook handles, replaced with
  `<PHONE>`, `<EMAIL>`, `<SOCIAL>` tokens. Applied at bronze→silver so PII never
  reaches an analysable layer.
- Price normalisation: `"7tr5"`, `"7.5 triệu"`, `"7,500,000"`, `"7tr500"` →
  `7500000`. Listings priced "thỏa thuận" (negotiable) are flagged and excluded from
  the rent model's training set but retained for descriptive analysis.

### 7.3 Survival labelling

```
first_seen  = min(obs_date) where is_present
last_seen   = max(obs_date) where is_present
gone_date   = first date with 2 consecutive absences
censor_date = 2026-10-12  (panel freeze date)

if gone_date is not null:
    duration_days  = gone_date - first_seen
    event_observed = true
else:
    duration_days  = censor_date - first_seen
    event_observed = false      # right-censored
```

**Left-truncation caveat:** listings that were already live before the crawl began
have unobserved earlier duration. Where `posted_date` is available it is used to
recover true age; where it is not, the listing is flagged `left_truncated` and a
sensitivity analysis is run both with and without those rows. This is discussed
explicitly in Report 3 rather than quietly ignored.

---

## 8. Verification Strategy

| Level | What | How |
|---|---|---|
| Unit | Parsers, normalisation, frontier logic | `pytest` against saved HTML fixtures — no network |
| Data quality | Schema, nulls, ranges, duplicates | Assertion job after each Spark stage; fails loudly and blocks downstream |
| Pipeline | Idempotency | Run the same job twice; assert byte-identical output row counts and checksums |
| Model | No leakage, no overfitting | **Temporal** train/test split — train on listings first seen before a cutoff, test after. A random split leaks, because duplicate re-posts of the same unit land on both sides. |
| Compliance | PII absence | Automated regex scan over silver/gold; any hit fails the build |
| Cost | Spend | AWS Budgets alerts at $50 / $100 / $200 |

**The temporal split is non-negotiable.** With near-duplicate re-posts in the data, a
random split produces a model that looks excellent and is worthless. If the reported
MAPE seems suspiciously good, this is the first thing to check.

---

## 9. Spark Cluster Topology (Oracle Always Free)

### 9.1 Instance layout

Oracle's Always Free Ampere A1 allocation is 4 OCPUs and 24 GB of RAM, which may be
split across up to four instances. Splitting it into three gives a genuinely
distributed cluster rather than a single node impersonating one.

| VM | Shape | Role | Spark config |
|---|---|---|---|
| `spark-master` | 2 OCPU / 12 GB / 80 GB | Master + driver + history server | `SPARK_WORKER_CORES=1`, `SPARK_WORKER_MEMORY=6g` |
| `spark-worker-1` | 1 OCPU / 6 GB / 60 GB | Worker | `SPARK_WORKER_CORES=1`, `SPARK_WORKER_MEMORY=4g` |
| `spark-worker-2` | 1 OCPU / 6 GB / 60 GB | Worker | `SPARK_WORKER_CORES=1`, `SPARK_WORKER_MEMORY=4g` |

Total block storage: 200 GB — exactly the Always Free ceiling. All three VMs sit in one
VCN subnet; port 7077 (master), 8080 (master UI), and 4040 (driver UI) are open within
the subnet only, never to the internet.

**Driver memory matters more than you would expect here.** The driver runs on the
master alongside the master daemon, so `spark.driver.memory` is capped at `4g`. Any job
that calls `.collect()` on a large result will OOM the driver and take the master with
it. Every job in the plan writes to S3 rather than collecting — this is why.

### 9.2 S3 access from outside AWS

Spark reads S3 through the `s3a://` connector, which needs two JARs matched to the
Hadoop version bundled with Spark 3.5 (Hadoop 3.3.4):

```bash
# infra/install_spark.sh (excerpt)
SPARK_HOME=/opt/spark
HADOOP_AWS=3.3.4
AWS_SDK=1.12.262

wget -P $SPARK_HOME/jars/ \
  https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_AWS}/hadoop-aws-${HADOOP_AWS}.jar
wget -P $SPARK_HOME/jars/ \
  https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK}/aws-java-sdk-bundle-${AWS_SDK}.jar
```

**Version mismatch between `hadoop-aws` and Spark's bundled Hadoop is the single most
common failure mode here**, and it surfaces as an opaque `NoSuchMethodError` rather
than anything mentioning versions. Check `$SPARK_HOME/jars/hadoop-common-*.jar` and
match exactly.

Required configuration in `spark-defaults.conf`:

```properties
spark.hadoop.fs.s3a.impl                 org.apache.hadoop.fs.s3a.S3AFileSystem
spark.hadoop.fs.s3a.aws.credentials.provider  org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider
spark.hadoop.fs.s3a.endpoint             s3.ap-southeast-1.amazonaws.com
spark.hadoop.fs.s3a.connection.maximum   32
spark.hadoop.fs.s3a.fast.upload          true
spark.hadoop.fs.s3a.block.size           64M
spark.sql.shuffle.partitions             24
spark.driver.memory                      4g
spark.executor.memory                    3g
spark.executor.cores                     1
```

`spark.sql.shuffle.partitions` drops from the default 200 to 24 — roughly 2–3× the
total core count. Leaving it at 200 on a 4-core cluster creates hundreds of tiny tasks
whose scheduling overhead exceeds their work.

Credentials come from an IAM user scoped to **this bucket only**, with `s3:GetObject`,
`s3:PutObject`, and `s3:ListBucket`. Keys live in `spark-env.sh` with mode `600`, never
in the repository.

### 9.3 Submitting jobs

```bash
# infra/submit.sh
#!/usr/bin/env bash
set -euo pipefail
JOB="$1"; shift
rsync -az --exclude '.git' ./ spark-master:/opt/vn-rental-dsp/
ssh spark-master "cd /opt/vn-rental-dsp && \
  /opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    --deploy-mode client \
    --py-files dist/jobs.zip \
    $JOB $*"
```

Compare with the EMR version it replaces: same script file, same arguments, same Python.
The portability was designed in from the start — `spark/` jobs take `--input`,
`--output`, and `--date` and read configuration from files, so nothing about them is
tied to a specific cluster.

### 9.4 ARM (aarch64) compatibility

The Ampere instances are ARM64. Verify in Week 1, not Week 3:

| Package | ARM64 wheel | Note |
|---|---|---|
| `pyspark` | Pure Python | Fine |
| `pyarrow` | Yes | Fine |
| `pandas`, `numpy` | Yes | Fine |
| `lxml` | Yes | Fine |
| `h3` | Yes | Verify the version resolves; older releases lacked aarch64 wheels |
| `scikit-learn` | Yes | Only needed for the optional single-node cross-check |

JDK 17 for ARM is available from the distro repositories. If any package has no ARM
wheel, it will attempt a source build — install `build-essential` and `python3-dev` on
the master beforehand so this fails gracefully rather than mysteriously.

### 9.5 Optional: HDFS/YARN demonstration cluster

If Report 2 needs explicit Hadoop evidence beyond Spark, run a small HDFS + YARN
cluster in Docker Compose on the master VM, load one province-month of silver data into
it, and run one job through YARN.

This produces the screenshots the "Big Data [Spark, Hadoop...]" criterion is asking
for — NameNode UI showing block distribution and replication, YARN ResourceManager
showing containers scheduled — without moving the real data lake off S3.

**Describe it accurately in the report:** it is a demonstration cluster processing a
sample, not the production data path. Claiming otherwise is the kind of overstatement
an examiner finds in about ninety seconds.

### 9.6 Fallback if Oracle capacity is unavailable

Oracle's free Ampere capacity is periodically exhausted in popular regions, and
instance creation fails with `Out of host capacity`. This is common and not an account
problem.

In order of preference:

1. **Retry in another region or availability domain.** Singapore, Osaka, and Seoul
   rotate availability. A retry loop against the API usually succeeds within a day.
2. **Google Cloud $300 / 90-day credits** — run Dataproc or a plain GCE VM. Covers the
   entire project window with room to spare, but expires.
3. **Databricks Free Edition** — real Spark and MLlib, zero setup, but reduced dataset
   size and no cluster topology to screenshot.
4. **Local machine** — Spark standalone on a personal computer with 16 GB RAM,
   processing province-by-province rather than the full corpus at once.

Decide this in Week 1. Discovering in Week 4 that there is nowhere to run Spark is a
project-level failure; discovering it in Week 1 costs an afternoon.
