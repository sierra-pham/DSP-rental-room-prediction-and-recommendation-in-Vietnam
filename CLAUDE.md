# VN Rental DSP — working notes for Claude

Vietnam urban rental market pipeline: four Scrapy spiders → immutable S3 bronze → Spark silver →
panel → models. The README covers layout, setup and crawling. This file covers what the
README does not: the rules that break things, the data contracts already decided, how to
verify on this machine, and where the truth about status lives.

Everything here was measured on this checkout on 2026-09-18. When a number disagrees with
the code, the code is right and this file is stale — fix it.

---

## Where status lives

- **Per-task status is the YAML front matter of `docs/notes/tasks/WBS-*.md`** (`status: todo |
  in-progress | done | blocked`) plus a dated `## Log`. Update it when you finish or defer a step.
- `docs/03-implementation-plan.md` is the plan and its checkboxes are **not** maintained. Do not
  read `[ ]` there as "not done"; read the WBS note.
- `docs/notes/daily/YYYY-MM-DD.md` is the run log. One dated bullet per piece of work.
- `docs/02-technical-design-spec.md` is the binding authority when plan and spec disagree.
- A deferred step is written down as deferred, with the reason. Never mark a WBS note `done`
  because the code exists; the completion criterion in the note is what has to be true.

---

## Rules that break something if broken

1. **Bronze is immutable.** Nothing deletes, rewrites or edits a bronze object. Silver is
   regenerable from bronze; bronze is not regenerable from anything.
2. **Parsers are pure.** `parsers/` takes an HTML string and a URL, returns a dict, makes no
   network calls, and is tested only against `tests/fixtures/*.html`. Field normalisation lives in
   `parsers/normalise.py`; applying it to silver lives in `spark/parse_bronze.py`.
   **Scrapy 2.19 API:** `start_requests()` was removed; use `async def start()` (async generator).
3. **Politeness ceiling is 1 request/second/domain.** `DOWNLOAD_DELAY >= 1.0` on every spider;
   `tests/test_spiders.py` enforces it. Only nhatot uses Playwright, capped at 20,000 pages/run.
4. **Every Spark job is idempotent.** `--date X` twice gives identical output. Silver writes are
   `partitionBy("province", "dt")` with `partitionOverwriteMode=dynamic`, so a rerun touches only
   that day's partitions.
5. **No PII past bronze.** `strip_pii` runs on `title`, `address` and `description_clean`;
   `pii_gate` scans every non-identifier string column of the silver schema and fails the write.
6. **Bronze `dt=` and `--date` are UTC days.** `utc_today()` in `crawler/spiders/base.py`. A run
   crossing 00:00 UTC files under its start date.
7. **PySpark 3.5.1 must equal the cluster's Spark version.** Driver/cluster mismatch fails at
   submit time.
8. **Never hand-edit a number in docs that you did not measure.** Run it.

---

## Data contracts already decided (do not re-decide)

| Contract | Decision | Why |
|---|---|---|
| `listing_id` in silver | the **bronze** id `<source>:<url last path segment>`, not the parser's | frontier, probes and panel all share the bronze key |
| `province` | one of `HCM HN DN BD DNA CT` via `normalise_province`; unknown → null | spec partitions on codes; charter §4.2 names the six markets |
| `district` | `normalise_district`: diacritics stripped, lowercase, prefix `quan/huyen/tp/q.` removed (`"Quận Gò Vấp"` → `"go vap"`, `"Q.7"` → `"7"`) | spec §3.2: "normalised, diacritic-stripped" |
| `furnishing` | `none/basic/full/unknown` via `config/furnishing_map.yaml` + `normalise_furnishing`; never null | spec §3.5 enum; unmapped → `unknown` with a logged warning |
| `property_type` | `config/property_type_map.yaml`; unmapped → `other` with a logged warning | spec §3.5 |
| `parse_ok` | `True` whenever the parser returned, even all-nulls | the `core_fields_null` gate catches the all-null day instead |
| non-200 bronze row | `is_active=False`, `parse_ok=False`, `parse_error="http_status: N"`, parser not called | expired listings are what `is_active` records |
| parse-rate gate | ≥ 95 % over **fetched** rows only; non-200 share is logged, never fails | a normal delisting rate must not abort the day |
| silver columns | spec §3.2 in order, plus `parse_ok`, `parse_error`, `dt`; explicit `SILVER_SCHEMA`, never inferred | |

Quality-gate rules, each tested red and green in `tests/test_spark_jobs.py`: `row_count`,
`asking_rent_vnd` in [300k, 500M] or null, `area_sqm` in [5, 1000] or null, `province` in the six
codes or null, `listing_id` unique within `(dt, source)`, `parse_rate`, `core_fields_null` (≤ 5 %
of parsed rows with rent, area and title all null), `furnishing` in the enum. `crawl_date != dt`
is warned, not failed.

---

## Verifying on this machine (Windows, corporate proxy)

```bash
export JAVA_HOME="$USERPROFILE/tools/jdk4py/jdk4py/java-runtime" PYTHONIOENCODING=utf-8
.venv/Scripts/python -m pytest tests/ -q -p no:cacheprovider      # 140 tests, ~13 s (no Spark)
.venv/Scripts/python -m pytest tests/test_spark_jobs.py -k <name>  # needs PySpark; skipped on 3.14
.venv/Scripts/python -m scrapy list                                # must print exactly 4 spiders
```

- `.venv` is Python 3.14.3 with Scrapy 2.19. **PySpark 3.5.1 is not available on Python 3.14**;
  the 37 Spark tests in `test_spark_jobs.py` are not collected. Use a Python 3.12 venv or the
  cluster for Spark work. Java 17 comes from the PyPI package `jdk4py` installed under
  `%USERPROFILE%\tools\jdk4py`.
- **The proxy blocks github.com** (raw downloads, releases, `git ls-remote`) and needs
  `curl --ssl-no-revoke`. PyPI and Maven Central are open. Hadoop `winutils.exe`/`hadoop.dll` are
  only on GitHub, so they cannot be fetched.
- **Consequence: Spark cannot read or write local files here.** `spark.read.json(<path>)` and
  local parquet writes crash with `UnsatisfiedLinkError: NativeIO$Windows.access0`. Spark tests
  build DataFrames in memory (`spark.read.json(sc.parallelize(lines), schema=BRONZE_SCHEMA)`).
  The real read/write path is exercised only on the Linux cluster.
- `tests/conftest.py` installs a Windows-only `sitecustomize` for PySpark worker processes that
  flushes the result socket at exit. Without it every Python task dies with "Python worker exited
  unexpectedly" — verified A/B on 2026-09-13. It is inert on Linux. Do not remove it because
  "it looks like a hack"; remove it only after reproducing a green run without it.
- Benign stderr on every Spark run: `HADOOP_HOME and hadoop.home.dir are unset`,
  `Missing Python executable`. Anything else new is a finding.
- A probe that fails because Java, Hadoop or a binary is missing proves nothing about the code.
  Check the tool ran before reading its failure as a defect.

---

## How work gets verified here

- **Test every gate in both directions.** A rule nobody has watched go red is a rule nobody knows
  works. Break it on purpose, confirm the failure, restore.
- **Measure before asserting.** Run the parser over the fixture and read what it returns before
  writing `assert out["district"] == ...`. Never assert `None` as the expected value of a field
  that should be populated.
- **Real Spark, real parsers, real fixtures.** No mocks in `tests/test_spark_jobs.py`.
- **Check a column's vocabulary, not just its presence.** Silver shipped raw per-site strings in
  `furnishing` and `district` once because the review checked §3.2 column names only.
- **Verbatim copies are a defect.** Four spiders share `crawler/spiders/base.py::BronzeSpider`;
  a new source is a name-only subclass.
- Commit only the files of the task. `.claude/settings.local.json`, `.superpowers/`, `*.db`,
  logs are ignored and must stay ignored.
- The repo is LF (`git ls-files --eol` shows `i/lf w/lf`); write files with `newline="\n"`.

---

## What is deferred to live environments (as of 2026-09-18)

| Step | Blocked on | Where it is logged |
|---|---|---|
| Full cron schedule for all four spiders on crawl-host; batdongsan and nhatot sitemaps return 403 behind proxy | EC2 host (no corporate proxy) | `WBS-2.2.3`, `WBS-2.4.1` |
| Run `spark/parse_bronze.py --date …` on the cluster; the parquet write path has **never executed** | Oracle cluster; `infra/submit.sh` does not exist yet | `WBS-4.1.3` |
| Run `spark/dedupe.py --date …` and `spark/build_panel.py --censor-date …` on the cluster | Oracle cluster | `WBS-4.3.1`, `WBS-4.4.1` |
| Executors import `parsers.*` inside `mapPartitions`; `submit.sh` must `pip install -e .` on every node or pass `--py-files` | Task 10 | `WBS-4.1.3` |
| Report 1 screenshot of live bronze; provisional cohort freeze | live data | plan Task 8 |

**Crawler deadline met 2026-09-18.** First bronze crawl: phongtro123 (10.6 MB) and mogi (10.2 MB)
landed in `s3://vn-rental-dsp/bronze/listings/dt=2026-09-18/`. Batdongsan and nhatot pending
deployment from a non-proxied host.

---

## Known follow-ups, none blocking

- `_SOCIAL_RE` in `parsers/normalise.py` has no word boundary: `"FBS Tower"` → `"<SOCIAL> Tower"`.
- `_PHONE_RE` eats four-group billion figures: `"10.000.000.000"` → `"1<PHONE>"`.
- `ward` is written raw and is scanned by `pii_gate`; a real phone number there fails the day and
  no code path strips it.
- `normalise_district("q7")` (no separator) is left as `"q7"`; `"Q.7"` → `"7"`.
- `amenities` is an array and is outside the PII scan; add an `exists()` clause when a parser fills it.
- `quality_gate` runs one `.count()` per rule; fold into a single `agg` when the cluster timing says so.
- Bronze `.jsonl.gz` parts are not splittable; parse parallelism is bounded by part count.
- `panel_scheduler.py` soft-delete detection covers 8 Vietnamese phrases; new sites may need more.
- `dedupe.py` union-find runs on the driver; will not scale past ~1 M edges without a distributed
  connected-components approach.
- `build_panel.py` `left_truncated` defaults to `True`; needs refinement when joined with silver
  `first_seen` data.
- `test_crawl_launch.py` has 3 pre-existing failures: playwright not installed (Python 3.14),
  `run_discovery.sh` path-with-spaces on Windows.
