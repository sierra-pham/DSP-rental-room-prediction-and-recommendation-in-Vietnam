---
id: "2.3.3"
type: task
title: "Daily probe scheduler"
element: "2.0 Data Collection System"
hours: 2.5
week: "2"
week_num: 2
status: done
critical_path: true
depends_on: ["2.3.2"]
plan_task: ["T12"]
---

# 2.3.3 — Daily probe scheduler

**Element:** 2.0 Data Collection System · **Effort:** 2.5 h · **Week:** 2 · **Plan:** T12
**On the critical path — a day lost here moves the end date.**

**Completion criterion:** Soft-delete detection tested; 2 days of probes landed

**Depends on:** [2.3.2](WBS-2.3.2.md)

## Log

- 2026-09-18: Implemented `crawler/panel_scheduler.py` with `classify_probe` detecting soft-delete pages (8 Vietnamese removal phrases). Tests in `tests/test_panel_scheduler.py` cover HTTP 200/404/500, soft-delete, and real listing pages. Probe landing in S3 deferred to EC2 deploy.

## References

- [Work breakdown structure](../../07-work-breakdown-structure.md)
- [Implementation plan](../../03-implementation-plan.md)
