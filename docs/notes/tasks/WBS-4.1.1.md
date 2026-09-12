---
id: "4.1.1"
type: task
title: "Parser ABC & normalisation"
element: "4.0 Data Processing & Quality"
hours: 4
week: "1"
week_num: 1
status: done
critical_path: false
depends_on: ["2.1.1"]
plan_task: ["T3"]
---

# 4.1.1 — Parser ABC & normalisation

**Element:** 4.0 Data Processing & Quality · **Effort:** 4 h · **Week:** 1 · **Plan:** T3


**Completion criterion:** 13 normalisation tests green

**Depends on:** [2.1.1](WBS-2.1.1.md)

## Log

- 2026-09-12 — `parsers/normalise.py` and `parsers/base.py` written TDD-first.
  `pytest tests/test_normalise.py` → **13 passed**, criterion met.
- The implementation plan's sample `parse_price_vnd` failed its own `"7.5 triệu"`
  case: the first regex `(\d+)\s*tr(?:iệu)?` skipped the `7.` and matched `5 triệu`,
  returning 5,000,000. Fixed by allowing a decimal separator in the whole part
  (`_MILLION_RE`); a decimal whole part suppresses the trailing fraction group.

## References

- [Work breakdown structure](../../07-work-breakdown-structure.md)
- [Implementation plan](../../03-implementation-plan.md)
