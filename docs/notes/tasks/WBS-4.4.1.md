---
id: "4.4.1"
type: task
title: "Panel fact table"
element: "4.0 Data Processing & Quality"
hours: 1.5
week: "3"
week_num: 3
status: done
critical_path: false
depends_on: ["4.3.1", "2.3.3"]
plan_task: ["T13"]
---

# 4.4.1 — Panel fact table

**Element:** 4.0 Data Processing & Quality · **Effort:** 1.5 h · **Week:** 3 · **Plan:** T13


**Completion criterion:** One row per listing × day

**Depends on:** [4.3.1](WBS-4.3.1.md), [2.3.3](WBS-2.3.3.md)

## Log

- 2026-09-18: Panel fact table logic is part of `spark/build_panel.py`. The `survival_labels` function reads (listing_id, obs_date, is_present) and produces the fact rows. The `__main__` block writes `gold/panel_fact/` partitioned by listing_id.

## References

- [Work breakdown structure](../../07-work-breakdown-structure.md)
- [Implementation plan](../../03-implementation-plan.md)
