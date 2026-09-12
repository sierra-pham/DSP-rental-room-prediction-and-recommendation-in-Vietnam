---
id: "4.2.2"
type: task
title: "Data quality assertions"
element: "4.0 Data Processing & Quality"
hours: 2
week: "2"
week_num: 2
status: in-progress
critical_path: false
depends_on: ["4.1.3"]
plan_task: ["T9"]
---

# 4.2.2 — Data quality assertions

**Element:** 4.0 Data Processing & Quality · **Effort:** 2 h · **Week:** 2 · **Plan:** T9


**Completion criterion:** Job fails loudly on violation; parse rate ≥95%

**Depends on:** [4.1.3](WBS-4.1.3.md)

## Log

- 2026-09-12 - `spark.parse_bronze.quality_gate` implemented. Rules, each failing with
  its own name and row count: `row_count` (zero rows), `asking_rent_vnd` outside
  300,000-500,000,000, `area_sqm` outside 5-1,000, `province` outside the six codes,
  `listing_id` repeated within `(dt, source)`, `parse_rate` below 95%. A null province
  is allowed and logged, not failed. Every rule is tested red on purpose and green on
  clean data. Verification against real silver is deferred with the cluster run
  ([4.1.3](WBS-4.1.3.md)).

## References

- [Work breakdown structure](../../07-work-breakdown-structure.md)
- [Implementation plan](../../03-implementation-plan.md)
