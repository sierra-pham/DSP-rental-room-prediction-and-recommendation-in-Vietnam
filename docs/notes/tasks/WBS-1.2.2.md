---
id: "1.2.2"
type: task
title: "Cost guardrails"
element: "1.0 Project Management & Governance"
hours: 1
week: "1"
week_num: 1
status: done
critical_path: false
depends_on: ["3.1.1"]
plan_task: ["T2"]
---

# 1.2.2 — Cost guardrails

**Element:** 1.0 Project Management & Governance · **Effort:** 1 h · **Week:** 1 · **Plan:** T2


**Completion criterion:** Budget alerts at $10/$25/$50 firing; egress alarm set

**Depends on:** [3.1.1](WBS-3.1.1.md)

## Log

<!-- dated notes as this package progresses -->

- 2026-09-12 — Budget `vn-rental-dsp-monthly` ($50/month, ACTUAL alerts at 20/50/100%
  = $10/$25/$50 -> nguyenpnt4@fpt.com) and CloudWatch alarm `s3-egress-high`
  (BytesDownloaded > 80 GB/day, state `OK`) created via `infra/budgets.sh`.
  Required root to enable IAM access to billing information first.

## References

- [Work breakdown structure](../../07-work-breakdown-structure.md)
- [Implementation plan](../../03-implementation-plan.md)
