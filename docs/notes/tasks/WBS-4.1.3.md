---
id: "4.1.3"
type: task
title: "Spark parse job"
element: "4.0 Data Processing & Quality"
hours: 5
week: "2"
week_num: 2
status: in-progress
critical_path: false
depends_on: ["4.1.2", "3.2.3"]
plan_task: ["T9"]
---

# 4.1.3 — Spark parse job

**Element:** 4.0 Data Processing & Quality · **Effort:** 5 h · **Week:** 2 · **Plan:** T9


**Completion criterion:** Idempotency test green; silver written

**Depends on:** [4.1.2](WBS-4.1.2.md), [3.2.3](WBS-3.2.3.md)

## Log

- 2026-09-12 - Job written as a distributed `mapPartitions` over bronze: gunzip
  `html_gz_b64`, dispatch on `source`, emit one silver row per bronze row under an
  explicit `SILVER_SCHEMA` (spec 3.2 plus `parse_ok` / `parse_error` / `dt`). The
  bronze `listing_id` passes through; `province` is stored as one of the six codes via
  the new `parsers.normalise.normalise_province`. Output is
  `silver/listings/province=*/dt=*` with `partitionOverwriteMode=dynamic`, so
  `--date X` twice rewrites only that day's partitions. Tested locally against the four
  parser fixtures (`tests/test_spark_jobs.py`, 19 tests green). The cluster run (plan
  step 7) is deferred until the Spark cluster exists ([3.2.1](WBS-3.2.1.md)) - there is
  nothing to submit to from this machine yet.

## References

- [Work breakdown structure](../../07-work-breakdown-structure.md)
- [Implementation plan](../../03-implementation-plan.md)
