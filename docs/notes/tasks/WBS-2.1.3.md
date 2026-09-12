---
id: "2.1.3"
type: task
title: "Crawl frontier (SQLite)"
element: "2.0 Data Collection System"
hours: 4
week: "1"
week_num: 1
status: done
critical_path: false
depends_on: ["2.1.1"]
plan_task: ["T4"]
---

# 2.1.3 — Crawl frontier (SQLite)

**Element:** 2.0 Data Collection System · **Effort:** 4 h · **Week:** 1 · **Plan:** T4


**Completion criterion:** 5 frontier tests green; 2-failure rule verified

**Depends on:** [2.1.1](WBS-2.1.1.md)

## Log

- 2026-09-12 — `crawler/frontier.py` implemented TDD-first. SQLite with WAL mode,
  URL deduplication, 2-failure gone rule, panel cohort freeze with random sampling.
  `pytest tests/test_frontier.py` → **5 passed**, criterion met.

## References

- [Work breakdown structure](../../07-work-breakdown-structure.md)
- [Implementation plan](../../03-implementation-plan.md)
