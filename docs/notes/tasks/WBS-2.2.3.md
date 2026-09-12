---
id: "2.2.3"
type: task
title: "Spiders — 3 remaining"
element: "2.0 Data Collection System"
hours: 5
week: "1"
week_num: 1
status: in-progress
critical_path: false
depends_on: ["2.2.2"]
plan_task: ["T7"]
---

# 2.2.3 — Spiders — 3 remaining

**Element:** 2.0 Data Collection System · **Effort:** 5 h · **Week:** 1 · **Plan:** T7


**Completion criterion:** 4 `source=` prefixes in today's partition

**Depends on:** [2.2.2](WBS-2.2.2.md)

## Log

<!-- dated notes as this package progresses -->

- 2026-09-12 — mogi, phongtro123 and nhatot spiders are written on a shared
  `crawler/spiders/base.py` (`BronzeSpider`) alongside batdongsan, with
  `tests/test_spiders.py` green. nhatot's Playwright fallback (download
  handlers, asyncio reactor, `CLOSESPIDER_PAGECOUNT: 20000`, per-request
  `playwright` meta) is in place. Step 7 (starting all four on the EC2 cron
  and confirming four `source=` prefixes in today's S3 partition) is
  deferred: the crawler host is not reachable from this machine. Completion
  criterion still needs that live run.

## References

- [Work breakdown structure](../../07-work-breakdown-structure.md)
- [Implementation plan](../../03-implementation-plan.md)
