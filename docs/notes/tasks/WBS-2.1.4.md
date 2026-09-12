---
id: "2.1.4"
type: task
title: "Sitemap poller"
element: "2.0 Data Collection System"
hours: 2
week: "1"
week_num: 1
status: done
critical_path: false
depends_on: ["2.1.3"]
plan_task: ["T5"]
---

# 2.1.4 — Sitemap poller

**Element:** 2.0 Data Collection System · **Effort:** 2 h · **Week:** 1 · **Plan:** T5


**Completion criterion:** Returns ≥10k URLs for one live source

**Depends on:** [2.1.3](WBS-2.1.3.md)

## Log

- 2026-09-12 — `crawler/sitemap_poller.py` implemented. Uses `lxml.etree` with
  namespace-agnostic XPath (`local-name()`). Handles both `<urlset>` and
  `<sitemapindex>` with one-level recursion. `pytest tests/test_sitemap_poller.py`
  → **2 passed**. Live validation (Step 4) deferred to EC2 deployment — corporate
  proxy blocks outbound fetches from this machine.

## References

- [Work breakdown structure](../../07-work-breakdown-structure.md)
- [Implementation plan](../../03-implementation-plan.md)
