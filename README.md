# Vietnam Urban Rental Market Intelligence

End-to-end big data pipeline: crawls Vietnam's urban rental listing market from four
portals, maintains a 30+ day daily panel of listings, and trains a fair-rent model plus
a time-on-market survival model on Spark.

**Question answered:** *what is this unit actually worth, and how long will it take to
rent at that price?*

## Layout

| Path | Purpose |
|---|---|
| `config/` | YAML config + `config.load(name)` loader. Bucket names, source definitions, Spark settings. |
| `crawler/` | Scrapy project: sitemap poller, SQLite frontier, spiders, S3 bronze writer, panel probe classifier. |
| `parsers/` | Pure functions `html → dict`. No network. Unit-tested against saved fixtures. |
| `spark/` | PySpark jobs: bronze→silver parse (`parse_bronze`), near-duplicate detection (`dedupe`), panel fact table with survival labels (`build_panel`), features, models. |
| `analysis/` | EDA, visualisation, prescriptive layer. |
| `infra/` | AWS / Oracle Cloud setup scripts. |
| `tests/` | pytest suite; `tests/fixtures/` holds saved HTML. |
| `docs/` | Charter, technical design spec, implementation plan, schedule, risk register. |
| `docs/notes/` | Obsidian working vault: per-task and per-risk notes, portal research, daily run log. |

## Setup

```bash
python -m venv .venv
.venv/Scripts/activate          # Windows;  source .venv/bin/activate on Linux
pip install -e ".[spark,viz,js]"
playwright install chromium      # nhatot renders client-side; behind a proxy set HTTPS_PROXY first
pytest
```

Requires Python 3.11+. PySpark 3.5.1 must match the Spark version on the cluster.
Note: PySpark is not yet available on Python 3.14; use 3.12 for Spark development.

## Crawling

The spiders, the sitemap poller and `run_discovery.sh` all share one frontier file:
`$FRONTIER_DB`, or `./frontier.db` when unset. Set it once on the crawl host.

```bash
export FRONTIER_DB=/var/data/frontier.db
python -m crawler.sitemap_poller --all                      # fill the frontier from sitemaps
scrapy crawl phongtro123 -s CLOSESPIDER_PAGECOUNT=100       # smoke test one source
./run_discovery.sh                                          # one pass over all four sources (cron)
```

Bronze `dt=` partitions and the parse job's `--date` are **UTC** days, the same clock
`crawl_ts` uses; a run that crosses 00:00 UTC keeps writing under the day it started on,
so the tail of that run is parsed by the next day's job.

## Spark pipeline

All jobs read from / write to `s3://vn-rental-dsp/` and are idempotent (re-running with the
same arguments produces identical output).

```bash
# Bronze → Silver: parse HTML, normalise fields, strip PII, run quality gates
spark-submit spark/parse_bronze.py --date 2026-09-18

# Near-duplicate detection: MinHash LSH with character 3-gram shingling
spark-submit spark/dedupe.py --date 2026-09-18

# Panel fact table: daily observation matrix + survival labels (two-consecutive-absences rule)
spark-submit spark/build_panel.py --censor-date 2026-10-12
```

## Panel monitoring

`crawler/panel_scheduler.py` classifies probe responses for panel tracking:

- Detects soft-deletes (HTTP 200 with Vietnamese removal phrases like "tin đăng không tồn tại")
- HTTP 404/410 → absent; HTTP 5xx → inconclusive (does not count as absence)
- Used by the panel probe spider to maintain `is_present` observations

## Documents

- [Project charter](docs/01-project-charter.md)
- [Technical design spec](docs/02-technical-design-spec.md)
- [Implementation plan](docs/03-implementation-plan.md)
- [Work breakdown structure](docs/07-work-breakdown-structure.md)
- [Risk register](docs/05-risk-register.md)
- [Working notes vault](docs/notes/README.md) — open the repo root in Obsidian; see
  [Dashboard](docs/notes/Dashboard.md)
