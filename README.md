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
| `crawler/` | Scrapy project: sitemap poller, SQLite frontier, spiders, S3 bronze writer. |
| `parsers/` | Pure functions `html -> dict`. No network. Unit-tested against saved fixtures. |
| `spark/` | PySpark jobs: bronze→silver parse, dedup, panel fact table, features, models. |
| `analysis/` | EDA, visualisation, prescriptive layer. |
| `infra/` | AWS / Oracle Cloud setup scripts. |
| `tests/` | pytest suite; `tests/fixtures/` holds saved HTML. |
| `docs/` | Charter, technical design spec, implementation plan, schedule, risk register. |
| `docs/notes/` | Obsidian working vault: per-task and per-risk notes, portal research, daily run log. |

## Setup

```bash
python -m venv .venv
.venv/Scripts/activate          # Windows;  source .venv/bin/activate on Linux
pip install -e ".[spark,viz]"
pytest
```

Requires Python 3.11+. PySpark 3.5.1 must match the Spark version on the cluster.

## Documents

- [Project charter](docs/01-project-charter.md)
- [Technical design spec](docs/02-technical-design-spec.md)
- [Implementation plan](docs/03-implementation-plan.md)
- [Work breakdown structure](docs/07-work-breakdown-structure.md)
- [Risk register](docs/05-risk-register.md)
- [Working notes vault](docs/notes/README.md) — open the repo root in Obsidian; see
  [Dashboard](docs/notes/Dashboard.md)
