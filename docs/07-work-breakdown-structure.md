# Work Breakdown Structure (WBS)

**Project:** Vietnam Urban Rental Market Intelligence
**Window:** 2026-09-14 → 2026-10-25 (6 weeks) · **Solo** · **Total effort: 165 h**
**Related:** `03-implementation-plan.md` (task detail) · `04-schedule-and-milestones.md`
(dates) · `05-risk-register.md`

This WBS is **deliverable-oriented**: each element names a *thing produced*, not an
activity. It satisfies the 100% rule — the eight Level-1 elements together account for
all project work, and anything not appearing here is out of scope by definition.

Level-3 work packages are the smallest managed unit. Each has an owner, an effort
estimate, a verifiable completion criterion, and a mapping to its task in the
implementation plan.

---

## 1. WBS Hierarchy

```
0.0  VN URBAN RENTAL MARKET INTELLIGENCE (165 h)
│
├── 1.0  PROJECT MANAGEMENT & GOVERNANCE .................... 10 h
│   ├── 1.1  Project Definition
│   │   ├── 1.1.1  Project charter
│   │   ├── 1.1.2  Technical design specification
│   │   └── 1.1.3  Work breakdown structure & schedule
│   ├── 1.2  Controls
│   │   ├── 1.2.1  Risk register (maintained weekly)
│   │   ├── 1.2.2  Cost guardrails & budget alarms
│   │   └── 1.2.3  Weekly progress log
│   └── 1.3  Compliance
│       ├── 1.3.1  Data governance & ethics policy
│       └── 1.3.2  robots.txt compliance matrix (re-verified weekly)
│
├── 2.0  DATA COLLECTION SYSTEM ............................. 27 h
│   ├── 2.1  Crawl Foundation
│   │   ├── 2.1.1  Repository skeleton & configuration loader
│   │   ├── 2.1.2  Source definitions (4 portals)
│   │   └── 2.1.3  Crawl frontier (SQLite, panel-aware)
│   ├── 2.2  Discovery Crawling
│   │   ├── 2.2.1  Sitemap poller with index recursion
│   │   ├── 2.2.2  Scrapy spider — batdongsan  ◀ CRITICAL PATH
│   │   ├── 2.2.3  Scrapy spiders — phongtro123, mogi, nhatot
│   │   └── 2.2.4  S3 bronze batch writer
│   ├── 2.3  Panel Collection
│   │   ├── 2.3.1  Provisional cohort freeze (Sep 20)
│   │   ├── 2.3.2  Full cohort freeze, stratified (Sep 25)
│   │   └── 2.3.3  Daily liveness probe scheduler
│   └── 2.4  Crawl Operations
│       ├── 2.4.1  EC2 deployment & cron scheduling
│       └── 2.4.2  Throughput validation & monitoring
│
├── 3.0  DATA PLATFORM ...................................... 12 h
│   ├── 3.1  Storage
│   │   ├── 3.1.1  S3 bucket, medallion prefixes, lifecycle rules
│   │   └── 3.1.2  Glue Data Catalog & Athena setup
│   ├── 3.2  Compute
│   │   ├── 3.2.1  Oracle Always Free VM provisioning (3 nodes)
│   │   ├── 3.2.2  Spark 3.5 standalone install + S3A connector
│   │   └── 3.2.3  Job submission tooling (submit.sh)
│   └── 3.3  Hadoop Evidence (optional)
│       └── 3.3.1  Docker HDFS/YARN demonstration cluster
│
├── 4.0  DATA PROCESSING & QUALITY .......................... 25 h
│   ├── 4.1  Parsing
│   │   ├── 4.1.1  Parser ABC & Vietnamese normalisation library
│   │   ├── 4.1.2  Four source-specific parsers + fixtures
│   │   └── 4.1.3  Spark bronze→silver parse job
│   ├── 4.2  Quality Gates
│   │   ├── 4.2.1  PII stripping & automated leak gate
│   │   └── 4.2.2  Schema, range & parse-rate assertions
│   ├── 4.3  Entity Resolution
│   │   └── 4.3.1  MinHash LSH deduplication & canonicalisation
│   └── 4.4  Panel Assembly
│       ├── 4.4.1  Panel fact table (listing × day)
│       └── 4.4.2  Right-censored survival labelling
│
├── 5.0  EXPLORATORY DATA ANALYSIS .......................... 15 h
│   ├── 5.1  Enrichment
│   │   ├── 5.1.1  OSM POI extraction (Overpass)
│   │   └── 5.1.2  H3 indexing & proximity features
│   ├── 5.2  Analysis
│   │   ├── 5.2.1  Market structure notebook
│   │   ├── 5.2.2  Geographic analysis & choropleth maps
│   │   └── 5.2.3  Temporal analysis & Kaplan–Meier curves
│   └── 5.3  Outputs
│       └── 5.3.1  Report-quality figure set (300 dpi)
│
├── 6.0  PREDICTIVE MODELS .................................. 25 h
│   ├── 6.1  Feature Pipeline
│   │   ├── 6.1.1  Feature engineering (numeric, geo, text, amenity)
│   │   └── 6.1.2  Leakage-safe temporal split
│   ├── 6.2  Stage 1 — Fair Rent
│   │   ├── 6.2.1  District-median baseline
│   │   ├── 6.2.2  GBT regressor training
│   │   ├── 6.2.3  Hyperparameter tuning (CrossValidator)
│   │   └── 6.2.4  price_gap scoring across all listings
│   └── 6.3  Stage 2 — Time-on-Market
│       ├── 6.3.1  AFT survival regression
│       ├── 6.3.2  Concordance evaluation
│       ├── 6.3.3  price_gap ablation study
│       └── 6.3.4  Left-truncation sensitivity analysis
│
├── 7.0  RESULTS & PRESCRIPTIVE LAYER ....................... 11 h
│   ├── 7.1  Prescriptive Engine
│   │   ├── 7.1.1  Price-vs-days curve generator
│   │   ├── 7.1.2  Optimal price selection (isotonic-smoothed)
│   │   └── 7.1.3  Mispricing / bargain alert table
│   └── 7.2  Interpretation
│       ├── 7.2.1  Feature importance & partial dependence
│       ├── 7.2.2  Residual failure-mode analysis
│       └── 7.2.3  Interactive district map artefact
│
├── 8.0  DELIVERABLES & DEFENCE ............................. 25 h
│   ├── 8.1  Reports
│   │   ├── 8.1.1  Report 1 — Project Proposal        ◀ Sep 20
│   │   ├── 8.1.2  Report 2 — Data Tasks              ◀ Oct 4
│   │   ├── 8.1.3  Report 3 — Model & Results         ◀ Oct 18
│   │   └── 8.1.4  Report 4 — Final Consolidated      ◀ Oct 25
│   ├── 8.2  Portfolio Assets
│   │   ├── 8.2.1  Public repository, README, architecture diagram
│   │   └── 8.2.2  Reproducibility verification (clean checkout)
│   └── 8.3  Oral Defence
│       ├── 8.3.1  Capstone presentation deck (~15 slides)
│       └── 8.3.2  Defence preparation & rehearsal
│
└── 9.0  MANAGEMENT RESERVE ................................. 15 h
        (unallocated buffer — 9% of total; see §5)
```

---

## 2. WBS Dictionary

Level-3 work packages with owner, effort, week, dependency, and the criterion that
closes them. **Plan** column maps to the task number in `03-implementation-plan.md`.

### 1.0 Project Management & Governance — 10 h

| ID | Work Package | h | Week | Depends on | Completion criterion | Plan |
|---|---|---|---|---|---|---|
| 1.1.1 | Project charter | 2 | 0 | — | Scope, problem, success criteria approved | — |
| 1.1.2 | Technical design spec | 2 | 0 | 1.1.1 | Architecture, schemas, algorithms documented | — |
| 1.1.3 | WBS & schedule | 1 | 0 | 1.1.2 | All work decomposed; critical path identified | — |
| 1.2.1 | Risk register maintenance | 2 | 1–6 | 1.1.3 | Reviewed and dated every Monday | — |
| 1.2.2 | Cost guardrails | 1 | 1 | 3.1.1 | Budget alerts at $10/$25/$50 firing; egress alarm set | T2 |
| 1.2.3 | Weekly progress log | 1 | 1–6 | — | 6 dated entries with cost, volume, event rate | — |
| 1.3.1 | Governance & ethics policy | 1 | 0 | — | Publishable without edits | — |
| 1.3.2 | robots.txt re-verification | — | 1–6 | 1.3.1 | Matrix re-checked weekly; changes logged | — |

### 2.0 Data Collection System — 27 h

| ID | Work Package | h | Week | Depends on | Completion criterion | Plan |
|---|---|---|---|---|---|---|
| 2.1.1 | Repo skeleton & config loader | 2 | 1 | — | `pytest tests/test_smoke.py` green | T1 |
| 2.1.2 | Source definitions | 1 | 1 | 2.1.1 | 4 sources with sitemaps and patterns | T1 |
| 2.1.3 | Crawl frontier (SQLite) | 4 | 1 | 2.1.1 | 5 frontier tests green; 2-failure rule verified | T4 |
| 2.1.4 | Sitemap poller | 2 | 1 | 2.1.3 | Returns ≥10k URLs for one live source | T5 |
| 2.2.2 | **Spider — batdongsan** | 6 | 1 | 2.1.3, 2.2.4 | **Bronze objects landing in S3** | T6 |
| 2.2.3 | Spiders — 3 remaining | 5 | 1 | 2.2.2 | 4 `source=` prefixes in today's partition | T7 |
| 2.2.4 | S3 bronze batch writer | 2 | 1 | 2.1.1 | Writer test green; ~128 MB parts | T6 |
| 2.3.1 | Provisional cohort freeze | 0.5 | 1 | 2.2.2 | ~50k listings flagged `in_panel` | T8 |
| 2.3.2 | Full cohort freeze | 1 | 2 | 2.2.3 | ~200k, stratified by source × province | T12 |
| 2.3.3 | Daily probe scheduler | 2.5 | 2 | 2.3.2 | Soft-delete detection tested; 2 days of probes landed | T12 |
| 2.4.1 | EC2 deploy & cron | 1 | 1 | 2.2.2 | Unattended run across a full day | T6 |
| 2.4.2 | Throughput validation | — | 1 | 2.4.1 | ≥2,000 pages/hour sustained | T6 |

### 3.0 Data Platform — 12 h

| ID | Work Package | h | Week | Depends on | Completion criterion | Plan |
|---|---|---|---|---|---|---|
| 3.1.1 | S3 bucket & lifecycle | 1.5 | 1 | — | Public access blocked; prefixes created | T2 |
| 3.1.2 | Glue Catalog & Athena | 1 | 3 | 4.1.3 | Silver queryable via Athena | T15 |
| 3.2.1 | Oracle VM provisioning | 2.5 | 2 | — | 3 VMs running; ports open in VCN only | T10 |
| 3.2.2 | Spark install + S3A | 5 | 2 | 3.2.1 | 2 workers ALIVE; `smoke_s3.py` returns a count | T10 |
| 3.2.3 | Job submission tooling | 1 | 2 | 3.2.2 | `submit.sh` runs a job end to end | T10 |
| 3.3.1 | HDFS/YARN demo (optional) | 1 | 3 | 3.2.2 | NameNode + ResourceManager screenshots captured | T10 |

### 4.0 Data Processing & Quality — 25 h

| ID | Work Package | h | Week | Depends on | Completion criterion | Plan |
|---|---|---|---|---|---|---|
| 4.1.1 | Parser ABC & normalisation | 4 | 1 | 2.1.1 | 13 normalisation tests green | T3 |
| 4.1.2 | Four parsers + fixtures | 5 | 1–2 | 4.1.1 | 8 parser tests green; property types mapped | T3, T7 |
| 4.1.3 | Spark parse job | 5 | 2 | 4.1.2, 3.2.3 | Idempotency test green; silver written | T9 |
| 4.2.1 | PII gate | 2 | 2 | 4.1.3 | Audit query returns 0 | T9 |
| 4.2.2 | Data quality assertions | 2 | 2 | 4.1.3 | Job fails loudly on violation; parse rate ≥95% | T9 |
| 4.3.1 | Deduplication (MinHash LSH) | 4 | 2 | 4.1.3 | 20 clusters manually validated; rate 10–25% | T11 |
| 4.4.1 | Panel fact table | 1.5 | 3 | 4.3.1, 2.3.3 | One row per listing × day | T13 |
| 4.4.2 | Survival labelling | 1.5 | 3 | 4.4.1 | Censoring test green; single absence ≠ event | T13 |

### 5.0 Exploratory Data Analysis — 15 h

| ID | Work Package | h | Week | Depends on | Completion criterion | Plan |
|---|---|---|---|---|---|---|
| 5.1.1 | OSM POI extraction | 2 | 3 | — | `osm_pois.parquet` cached | T14 |
| 5.1.2 | H3 indexing & proximity | 3 | 3 | 5.1.1, 4.1.3 | H3 tests green; null coords handled | T14 |
| 5.2.1 | Market structure notebook | 3 | 3 | 4.3.1 | Distributions, missingness, duplicate rates | T15 |
| 5.2.2 | Geographic analysis & maps | 3 | 3 | 5.1.2 | Choropleth + hexbin + distance-to-CBD fit | T15 |
| 5.2.3 | Temporal & KM curves | 2 | 3 | 4.4.2 | KM by price quintile — the thesis figure | T15 |
| 5.3.1 | Figure set export | 2 | 3 | 5.2.x | All figures 300 dpi in `reports/figures/` | T15 |

### 6.0 Predictive Models — 25 h

| ID | Work Package | h | Week | Depends on | Completion criterion | Plan |
|---|---|---|---|---|---|---|
| 6.1.1 | Feature engineering | 4 | 4 | 5.1.2, 4.3.1 | `features_rent` written | T17 |
| 6.1.2 | Leakage-safe temporal split | 2 | 4 | 6.1.1 | Cluster-leak test green | T17 |
| 6.2.1 | Baseline model | 1 | 4 | 6.1.2 | Baseline MAPE recorded | T18 |
| 6.2.2 | GBT training & evaluation | 5 | 4 | 6.2.1 | MAPE by province and price decile | T18 |
| 6.2.3 | Hyperparameter tuning | 4 | 4 | 6.2.2 | **MAPE ≤ 20%, ≥30% better than baseline** | T18 |
| 6.2.4 | price_gap scoring | 1 | 4 | 6.2.3 | `features_survival` carries `price_gap` | T18 |
| 6.3.1 | AFT survival regression | 3 | 5 | 6.2.4, 4.4.2 | Censoring convention verified; model fits | T19 |
| 6.3.2 | Concordance evaluation | 2 | 5 | 6.3.1 | **C-index ≥ 0.65** | T19 |
| 6.3.3 | price_gap ablation | 2 | 5 | 6.3.2 | Δ concordance recorded either way | T19 |
| 6.3.4 | Left-truncation sensitivity | 1 | 5 | 6.3.3 | Both fits reported; divergence discussed | T19 |

### 7.0 Results & Prescriptive Layer — 11 h

| ID | Work Package | h | Week | Depends on | Completion criterion | Plan |
|---|---|---|---|---|---|---|
| 7.1.1 | Price-vs-days curve | 2 | 5 | 6.3.1 | Monotonicity test green | T20 |
| 7.1.2 | Optimal price selection | 2 | 5 | 7.1.1 | 3 worked examples for the report | T20 |
| 7.1.3 | Bargain alert table | 1 | 5 | 7.1.2 | Top-100 underpriced, liquid listings | T20 |
| 7.2.1 | Importance & partial dependence | 2 | 5 | 6.2.3 | Top-10 rent drivers named and explained | T21 |
| 7.2.2 | Residual failure analysis | 2 | 5 | 6.2.3 | Failure modes by district, band, source | T21 |
| 7.2.3 | Interactive district map | 2 | 5 | 7.1.3 | Clickable folium artefact | T21 |

### 8.0 Deliverables & Defence — 25 h

| ID | Work Package | h | Week | Depends on | Completion criterion | Plan |
|---|---|---|---|---|---|---|
| 8.1.1 | Report 1 — Proposal | 4 | 1 | 1.1.1, 1.1.2 | Submitted Sep 20 | T8 |
| 8.1.2 | Report 2 — Data Tasks | 6 | 3 | 4.x, 5.x | Submitted Oct 4; Spark justification evidenced | T16 |
| 8.1.3 | Report 3 — Model & Results | 6 | 5 | 6.x, 7.x | Submitted Oct 18 | T22 |
| 8.1.4 | Report 4 — Final | 3 | 6 | 8.1.1–8.1.3 | Consolidated; design changes explained | T24 |
| 8.2.1 | Repository & README | 2 | 6 | all | Public; architecture diagram; no credentials | T23 |
| 8.2.2 | Reproducibility verification | 1 | 6 | 8.2.1 | Clean checkout runs from README alone | T23 |
| 8.3.1 | Presentation deck | 2 | 6 | 8.1.4 | ~15 slides | T24 |
| 8.3.2 | Defence preparation | 1 | 6 | 8.3.1 | 4 anticipated questions answerable; rehearsed twice | T24 |

---

## 3. Effort Roll-Up

| WBS | Element | Hours | % | Peak week |
|---|---|---|---|---|
| 1.0 | Project Management & Governance | 10 | 6% | continuous |
| 2.0 | Data Collection System | 27 | 16% | Week 1 |
| 3.0 | Data Platform | 12 | 7% | Week 2 |
| 4.0 | Data Processing & Quality | 25 | 15% | Week 2 |
| 5.0 | Exploratory Data Analysis | 15 | 9% | Week 3 |
| 6.0 | Predictive Models | 25 | 15% | Weeks 4–5 |
| 7.0 | Results & Prescriptive Layer | 11 | 7% | Week 5 |
| 8.0 | Deliverables & Defence | 25 | 15% | Weeks 3, 5, 6 |
| 9.0 | Management Reserve | 15 | 9% | — |
| | **Total** | **165** | **100%** | |

**Weekly load** (against ~27 h/week available), summed bottom-up from the work packages
above rather than estimated at the week level:

| Week | Planned h | Load | Note |
|---|---|---|---|
| 0 | 6 | — | Charter, spec, WBS, governance — already complete |
| 1 | 34 | 🔴 126% | Front-loaded by design — the Day 5 crawler deadline |
| 2 | 30 | 🟠 111% | Platform + processing in parallel |
| 3 | 26 | 🟢 96% | EDA plus Report 2 |
| 4 | 17 | 🟢 63% | Deliberate slack |
| 5 | 25 | 🟢 93% | Survival + prescriptive + Report 3 |
| 6 | 9 | 🟢 33% | Consolidation — deliberate slack |
| 1–6 | 3 | — | Continuous: risk review, progress log, robots re-check |
| — | 15 | — | Management reserve |
| | **150 + 15** | | **165 total** |

**Weeks 1 and 2 exceed capacity; Weeks 4 and 6 are well under.** That shape is
deliberate, not an estimating error. The crawler deadline forces work into Week 1, and
the two light weeks exist to absorb the overflow — Week 4 catches Week 1–2 slippage,
Week 6 catches Week 5's.

Two cautions on reading this. First, **Week 6 at 33% is the least trustworthy number
here**: 9 hours to consolidate Report 4, polish the repository, verify reproducibility,
build a deck and rehearse assumes everything upstream landed cleanly. Treat its slack as
reserve, not free time. Second, **Week 2 is the real pinch point** — it is over capacity
*and* sits between two hard dates, so it has nowhere to push work. If Week 1 overruns,
apply the cut list in `04-schedule-and-milestones.md` §6 rather than borrowing from
Week 2.

---

## 4. Critical Path

```
2.1.1 → 2.1.3 → 2.2.4 → 2.2.2 → 2.4.1 ═══ CRAWLER LIVE (Sep 18)
                                    │
                                    ▼
                              2.3.2 (Sep 25)  ═══ COHORT FROZEN
                                    │
                                    ▼
                              2.3.3 ──── 17 days of observation ────┐
                                                                    ▼
3.2.1 → 3.2.2 → 3.2.3 → 4.1.3 → 4.3.1 → 4.4.1 ─────────────────► 4.4.2 (Oct 12)
                                    │                                │
                                    ▼                                ▼
                              6.1.1 → 6.1.2 → 6.2.2 → 6.2.3 → 6.2.4 → 6.3.1
                                                                       │
                                                                       ▼
                                                                 6.3.2 → 7.1.1 → 8.1.3
```

**The binding constraint is 2.3.3 — elapsed observation time.** It consumes no effort
hours yet dominates the schedule, because calendar days cannot be compressed by working
harder. Every other path has float.

**Zero-float packages:** 2.2.2, 2.4.1, 2.3.2, 2.3.3, 4.4.2, 6.2.4, 6.3.1, 8.1.3.
A day lost on any of these moves the end date.

---

## 5. Management Reserve (9.0)

Fifteen hours, 9% of total, held against realised risk — **not** against scope growth.

| Trigger | Draw | Risk |
|---|---|---|
| Crawler blocked on anti-bot | up to 6 h | R-01, R-02 |
| Oracle capacity unavailable, fallback needed | up to 5 h | R-12 |
| Rent model misses MAPE target | up to 5 h | R-05 |
| Parser breakage from site redesign | up to 3 h | R-06 |

Reserve draws are logged in `docs/weekly-log.md` with the triggering risk ID. When the
reserve is exhausted, the response is the cut list — not unpaid overtime.

**9% is thin for a project with two critical risks.** It is what fits in six weeks, and
it is why the cut order was agreed in advance rather than improvised under pressure.

---

## 6. Exclusions (the 100% rule, stated negatively)

Work **not** in this WBS is out of scope and requires a documented change before it is
started:

- Image collection, storage, and CNN feature extraction
- Streamlit or hosted dashboard beyond the folium artefact (7.2.3)
- Supabase or any serving-layer API
- Real-time streaming ingestion (Kafka, Spark Structured Streaming)
- Sale-price listings; any market outside the six named provinces
- Facebook, Zalo, or any social-platform source
- Airflow or managed orchestration
- PhoBERT or transformer-based text embeddings
