---
id: "4.1.2"
type: task
title: "Four parsers + fixtures"
element: "4.0 Data Processing & Quality"
hours: 5
week: "1–2"
week_num: 1
status: done
critical_path: false
depends_on: ["4.1.1"]
plan_task: ["T3", "T7"]
---

# 4.1.2 — Four parsers + fixtures

**Element:** 4.0 Data Processing & Quality · **Effort:** 5 h · **Week:** 1–2 · **Plan:** T3, T7


**Completion criterion:** 8 parser tests green; property types mapped

**Depends on:** [4.1.1](WBS-4.1.1.md)

## Log

- 2026-09-12 — **Blocked on fixtures.** Plan task T3 step 1 requires three
  batdongsan listing pages saved by hand from a browser into `tests/fixtures/`
  (full-field apartment, sparse room, `thỏa thuận` price). Selectors for
  `parsers/batdongsan.py` have to be read off those files, so the parser and its
  8 tests cannot start until they exist.

- 2026-09-12 — All four parsers (batdongsan, phongtro123, mogi, nhatot) and
  their fixtures are committed, with `config/property_type_map.yaml` mapping
  each source's raw category strings to the controlled vocabulary.
  `pytest tests/test_parsers.py` is 20 green tests (5 tests x 4 sources; the
  "8 parser tests" in the completion criterion below is the plan's stale
  count from before the fourth source and its extra checks were added).

## References

- [Work breakdown structure](../../07-work-breakdown-structure.md)
- [Implementation plan](../../03-implementation-plan.md)
