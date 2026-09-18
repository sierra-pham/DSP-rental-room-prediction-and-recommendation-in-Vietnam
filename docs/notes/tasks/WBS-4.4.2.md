---
id: "4.4.2"
type: task
title: "Survival labelling"
element: "4.0 Data Processing & Quality"
hours: 1.5
week: "3"
week_num: 3
status: done
critical_path: true
depends_on: ["4.4.1"]
plan_task: ["T13"]
---

# 4.4.2 — Survival labelling

**Element:** 4.0 Data Processing & Quality · **Effort:** 1.5 h · **Week:** 3 · **Plan:** T13
**On the critical path — a day lost here moves the end date.**

**Completion criterion:** Censoring test green; single absence ≠ event

**Depends on:** [4.4.1](WBS-4.4.1.md)

## Log

- 2026-09-18: Implemented `survival_labels` in `spark/build_panel.py`. Two-consecutive-absences rule using `lead()` window function. Tests `test_panel_survival_labels` and `test_panel_survival_labels_no_presence` in `test_spark_jobs.py` verify censoring logic and single-absence resilience.

## References

- [Work breakdown structure](../../07-work-breakdown-structure.md)
- [Implementation plan](../../03-implementation-plan.md)
