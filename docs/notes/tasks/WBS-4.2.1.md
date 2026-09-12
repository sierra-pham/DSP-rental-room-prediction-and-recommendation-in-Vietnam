---
id: "4.2.1"
type: task
title: "PII gate"
element: "4.0 Data Processing & Quality"
hours: 2
week: "2"
week_num: 2
status: in-progress
critical_path: false
depends_on: ["4.1.3"]
plan_task: ["T9"]
---

# 4.2.1 — PII gate

**Element:** 4.0 Data Processing & Quality · **Effort:** 2 h · **Week:** 2 · **Plan:** T9


**Completion criterion:** Audit query returns 0

**Depends on:** [4.1.3](WBS-4.1.3.md)

## Log

- 2026-09-12 - `spark.parse_bronze.pii_gate` implemented: `description_clean` is
  matched against `(?:\+?84|0)[0-9]{9,10}` and the job raises `RuntimeError` naming the
  offending row count. Tested both ways - green on clean rows, red on a planted phone
  number. The audit-on-real-data half of the completion criterion is deferred with the
  cluster run ([4.1.3](WBS-4.1.3.md)).

## References

- [Work breakdown structure](../../07-work-breakdown-structure.md)
- [Implementation plan](../../03-implementation-plan.md)
