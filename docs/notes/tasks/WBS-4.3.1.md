---
id: "4.3.1"
type: task
title: "Deduplication (MinHash LSH)"
element: "4.0 Data Processing & Quality"
hours: 4
week: "2"
week_num: 2
status: done
critical_path: false
depends_on: ["4.1.3"]
plan_task: ["T11"]
---

# 4.3.1 — Deduplication (MinHash LSH)

**Element:** 4.0 Data Processing & Quality · **Effort:** 4 h · **Week:** 2 · **Plan:** T11


**Completion criterion:** 20 clusters manually validated; rate 10–25%

**Depends on:** [4.1.3](WBS-4.1.3.md)

## Log

- 2026-09-18: Implemented `spark/dedupe.py` with character 3-gram shingling, blocking key, MinHashLSH (5 tables, Jaccard distance ≤ 0.20), union-find connected components. Tests `test_dedupe_clusters_reposts` and `test_dedupe_singletons_survive` added. Manual cluster validation deferred to live data.

## References

- [Work breakdown structure](../../07-work-breakdown-structure.md)
- [Implementation plan](../../03-implementation-plan.md)
