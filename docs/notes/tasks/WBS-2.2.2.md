---
id: "2.2.2"
type: task
title: "Spider — batdongsan"
element: "2.0 Data Collection System"
hours: 6
week: "1"
week_num: 1
status: in-progress
critical_path: true
depends_on: ["2.1.3", "2.2.4"]
plan_task: ["T6"]
---

# 2.2.2 — Spider — batdongsan

**Element:** 2.0 Data Collection System · **Effort:** 6 h · **Week:** 1 · **Plan:** T6
**On the critical path — a day lost here moves the end date.**

**Completion criterion:** Bronze objects landing in S3

**Depends on:** [2.1.3](WBS-2.1.3.md), [2.2.4](WBS-2.2.4.md)

## Log

- 2026-09-12 — `crawler/spiders/batdongsan.py` and `crawler/settings.py` written.
  Spider reads frontier batch, fetches raw HTML, gzip+base64 encodes it, writes via
  S3BatchWriter, and marks URLs fetched. Settings enforce ROBOTSTXT_OBEY,
  AUTOTHROTTLE_TARGET_CONCURRENCY=1.0, DOWNLOAD_DELAY=1.0. Smoke test (Steps 5-6)
  and EC2 deployment (Step 7) require live environment.

## References

- [Work breakdown structure](../../07-work-breakdown-structure.md)
- [Implementation plan](../../03-implementation-plan.md)
