# Risk Register

**Project:** Vietnam Urban Rental Market Intelligence
**Reviewed:** every Monday, 30 minutes, logged in `docs/weekly-log.md`

**Scoring:** Likelihood and Impact each 1–5. Score = L × I.
🔴 15–25 critical · 🟠 8–14 significant · 🟡 4–7 moderate · 🟢 1–3 low

---

## The Two That Can Actually Kill This Project

Most risks below cost you quality. **R-01 and R-04 cost you the project**, because
both destroy something that cannot be recovered later by working harder: elapsed
observation time. Read these two first and check them weekly.

---

## R-01 🔴 — Crawler not live by Sep 18

| | |
|---|---|
| **Likelihood** | 3 |
| **Impact** | 5 |
| **Score** | **15 — CRITICAL** |

**Description.** The crawler slips past Day 5. Because the observation window cannot
be back-filled, every day of delay permanently removes a day of survival labels. A
crawler live on Oct 1 means there is no Report 3 as designed.

**Early warning signals.** Wednesday of Week 1 arrives with no successful fetch; time
being spent on parser elegance rather than getting bytes to S3; anti-bot friction on
the first source attempted.

**Mitigation (do these proactively).**
- Build the crawler *before* the parsers are complete — bronze stores raw HTML, so
  parsing can be fixed any time afterwards. This ordering exists specifically to
  de-risk R-01.
- Target one source first (batdongsan), not four.
- Timebox: any single blocker gets 4 hours, then you route around it.
- Run Checkpoint A on Friday Sep 18 against a number, not a feeling.

**Contingency (if triggered).**
1. Drop to the single easiest source — `phongtro123` has the simplest HTML and the
   most permissive robots.txt.
2. Strip the crawler to `requests` + `lxml` in a loop. It is worse in every way except
   the one that matters: it works today.
3. Be live by Saturday Sep 19 at the absolute latest.
4. If Sep 22 arrives with nothing flowing, **change the project**: drop the survival
   stage, make it a pure rent-prediction project with a one-shot crawl, and rewrite
   Report 1's scope accordingly. Taking this decision in Week 2 is recoverable;
   discovering it in Week 5 is not.

**Owner:** Nguyen · **Review:** daily during Week 1

---

## R-02 🟠 — A source deploys anti-bot protection mid-project

| | |
|---|---|
| **Likelihood** | 3 |
| **Impact** | 4 |
| **Score** | **12 — SIGNIFICANT** |

**Description.** Cloudflare, rate-limit blocking, or a login wall appears on a source
you are already depending on. Most damaging if it hits a source in the frozen panel
cohort, because those listings then look as though they disappeared — producing a mass
of **false survival events** on a single date.

**Early warning.** A rising 403/429 rate; response times climbing; AutoThrottle's delay
drifting upward in the logs.

**Mitigation.**
- Four sources, so no single one is load-bearing.
- Stay conservative on rate: ≤ 1 req/s, honest User-Agent, `ROBOTSTXT_OBEY = True`.
  Most blocking is triggered by aggression, and this crawl is not aggressive.
- Monitor the daily 403/429 rate; alert if it exceeds 5%.
- **Critical safeguard:** if a source's probe failure rate exceeds 20% on a single
  day, mark that day's probes `invalid` for that source rather than recording
  disappearance events. Encode this as an automated check in `build_panel.py` — a
  site-wide block must never be read as 200,000 units renting simultaneously.

**Contingency.** Drop the affected source from the panel cohort, re-weight the
remaining sources, and document the coverage change in Report 2. Keep the already-
collected bronze data — it remains valid for descriptive analysis.

**Owner:** Nguyen · **Review:** weekly

---

## R-03 🟡 — AWS cost overrun (egress, not compute)

| | |
|---|---|
| **Likelihood** | 2 |
| **Impact** | 3 |
| **Score** | **6 — MODERATE** *(was 12; reduced by moving compute off EMR)* |

**Description.** With Spark running free on Oracle, compute cost is gone and projected
spend drops from ~$150 to ~$20. The remaining exposure is **S3 data transfer out**:
reading the bucket from outside AWS is billed at ~$0.09/GB beyond 100 GB/month. A full
re-parse reads ~25–30 GB, so repeated full passes during a debugging session are the
realistic way to overspend. Unpartitioned Athena queries are the secondary risk.

**Early warning.** Monthly `DataTransfer-Out-Bytes` passing 80 GB; running more than
three full re-parses in a month.

**Mitigation.**
- Budget alerts at **$10 / $25 / $50** — low enough that an anomaly is visible
  immediately rather than lost inside expected spend.
- CloudWatch alarm on `DataTransfer-Out-Bytes` at 80 GB.
- Daily processing is incremental — only the new day's bronze partition is parsed.
- Cache `silver/` and `gold/` on the Oracle VMs' free 200 GB block storage and read
  locally during iterative modelling; only bronze re-parses need to hit S3.
- Partition-pruned Athena queries only; always filter on `province` and `dt`.
- Weekly cost review as part of the Monday ritual.

**Contingency.** Pull the whole silver layer to the Oracle VMs once and work entirely
locally for the remainder of the project, syncing only final outputs back to S3.
Silver is ~8 GB, so this is a single transfer well inside the free allowance.

**Owner:** Nguyen · **Review:** weekly (Monday cost check)

---

## R-12 🟠 — No free Oracle capacity, or the free cluster underperforms

| | |
|---|---|
| **Likelihood** | 3 |
| **Impact** | 4 |
| **Score** | **12 — SIGNIFICANT** |

**Description.** Two distinct failures with one owner. First: Oracle's Always Free
Ampere capacity is frequently exhausted in popular regions and instance creation fails
with `Out of host capacity` — common, and not an account problem. Second: four ARM
cores may prove too slow for full-corpus passes, making the modelling loop painful.

**Early warning.** Instance creation failing on Week 2 Monday; a single day's parse
taking over 30 minutes; full re-parses exceeding 4 hours.

**Mitigation.**
- **Provision on Week 2 Monday, not later** — this gives a full week to work through
  fallbacks before the pipeline is genuinely needed.
- Retry across availability domains and regions; capacity rotates, and a retry loop
  usually succeeds within a day.
- Design for incremental processing from the start, so full passes are rare.
- Tune `spark.sql.shuffle.partitions` to 24. The default of 200 is the most common
  cause of a small cluster appearing catastrophically slow.

**Contingency**, in order (full detail in Spec §9.6):
1. Different region or availability domain.
2. **GCP $300 / 90-day credits** — covers the whole project window comfortably.
3. **Databricks Free Edition** — real Spark and MLlib, but a reduced dataset.
4. **Local machine** — Spark standalone, processing province-by-province.

Note that options 2–4 all still cost nothing. The project is not at risk from this;
only the schedule is.

**Owner:** Nguyen · **Review:** at Task 10, then weekly

---

## R-13 🟡 — Oracle reclaims idle Always Free instances

| | |
|---|---|
| **Likelihood** | 2 |
| **Impact** | 3 |
| **Score** | **6 — MODERATE** |

**Description.** Oracle reclaims Always Free compute that has been genuinely idle for
7 days. Losing `spark-master` mid-project means rebuilding the cluster.

**Why likelihood is low.** Nightly Spark jobs keep all three VMs active well above the
idle threshold. The risk is real only if processing pauses for a week.

**Mitigation.** Confirm all three instances are running as part of the Monday review.
Keep `infra/install_spark.sh` fully scripted so a rebuild is one command per node
rather than an afternoon of remembering what was installed. Back up
`spark-defaults.conf` and `spark-env.sh` to the repository (keys excluded).

**Contingency.** Re-provision and re-run the install script. Because all data lives in
S3 and nothing of value is stored on the VMs, a lost node costs roughly 30 minutes.
This is a deliberate property of the design, not luck.

**Owner:** Nguyen · **Review:** weekly

---

## R-04 🔴 — Insufficient survival events

| | |
|---|---|
| **Likelihood** | 3 |
| **Impact** | 5 |
| **Score** | **15 — CRITICAL** |

**Description.** Too few panel listings disappear within the observation window, so the
AFT model has almost no uncensored observations to learn from. With a 17-day window
this is a live concern, not a theoretical one: if typical time-on-market is 45 days,
almost everything is censored.

**Early warning.** Checkpoint B on Sep 29 shows an event rate below 12%.

**Mitigation.**
- Freeze a **provisional cohort on Sep 20** as well as the full cohort on Sep 25.
  The provisional cohort buys 5 extra days of observation for a subset, at near-zero
  cost. Do this.
- Over-weight `phongtro123` in the cohort — the informal room segment turns over
  fastest, so it yields events soonest.
- Include listings already live before the crawl began, using `posted_date` to
  reconstruct age (flagged `left_truncated`, with a sensitivity analysis).
- Consider extending the censor date to Oct 14 if Week 4 modelling is ahead of plan.

**Contingency.** Restructure Report 3's headline: lead with the rent model as the
primary predictive result, and present survival analysis **descriptively** —
Kaplan–Meier curves by price quartile, no AFT regression. KM curves are valid with
heavy censoring and still demonstrate the core hypothesis visually. State the
limitation explicitly; a defensible descriptive result beats an AFT model fitted on
40 events.

**Owner:** Nguyen · **Review:** weekly from Sep 29

---

## R-05 🟠 — Duplicate contamination corrupts the labels

| | |
|---|---|
| **Likelihood** | 4 |
| **Impact** | 3 |
| **Score** | **12 — SIGNIFICANT** |

**Description.** Re-posted listings are not merged, so the original appears to have
rented when the landlord simply reposted it. Survival labels become noise, and a random
train/test split leaks duplicates across both sides — producing an excellent-looking and
entirely worthless rent model.

**Early warning.** A duplicate rate under 3% (the blocking key is too strict and the
join is finding nothing) or over 40% (over-merging distinct units). A rent model MAPE
suspiciously below 10%.

**Mitigation.**
- MinHash LSH with a manually validated threshold (Task 11, Step 4 — read 20 clusters
  yourself; do not trust the number).
- Cluster-aware temporal split, enforced by an explicit unit test (Task 17, Step 1).
- Compare the duplicate rate across sources; a wild outlier indicates a parser bug in
  ID extraction rather than genuine duplication.

**Contingency.** Tighten blocking to exact `(district, area, price)` matching. Less
recall, but high precision — and for label correctness, precision is what matters.

**Owner:** Nguyen · **Review:** weekly

---

## R-06 🟡 — Site HTML structure changes mid-crawl

| | |
|---|---|
| **Likelihood** | 3 |
| **Impact** | 2 |
| **Score** | **6 — MODERATE** |

**Description.** A source redesigns its listing page; selectors break; parsed fields
go null.

**Why the impact is only 2.** Bronze stores raw HTML. A parser fix plus a re-run
recovers everything. This is precisely the failure mode the medallion architecture was
adopted to neutralise — and it is worth saying so in Report 2, because it demonstrates
that the architecture choice was made for a reason.

**Mitigation.** The daily QA job asserts parse success ≥ 95% and fails loudly.
Fixtures are saved from multiple dates so regressions are visible.

**Contingency.** Fix selectors, re-run `parse_bronze.py` over the affected date range.
Cost: one cluster run — free, plus the S3 egress for that date range.

**Owner:** Nguyen · **Review:** weekly

---

## R-07 🟡 — Poor geographic coverage (missing coordinates)

| | |
|---|---|
| **Likelihood** | 4 |
| **Impact** | 2 |
| **Score** | **8 — SIGNIFICANT** |

**Description.** 30–50% of listings lack latitude/longitude, weakening exactly the
location features that matter most for rent prediction.

**Mitigation.** District-centroid fallback with an explicit `geo_precision` flag
carried as a model feature. H3 indexing at resolution 8 is coarse enough to remain
meaningful for centroid-derived points.

**Contingency.** Rely on district and ward as categorical features with target
encoding. Report MAPE separately for exact-coordinate and centroid-only listings —
the comparison is itself an interesting finding about the value of precise location
data.

**Owner:** Nguyen · **Review:** at Task 14

---

## R-08 🟡 — EC2 instance cannot sustain crawl throughput

| | |
|---|---|
| **Likelihood** | 2 |
| **Impact** | 3 |
| **Score** | **6 — MODERATE** |

**Description.** `t3.micro` burstable CPU credits are exhausted; throughput collapses
to baseline and the daily page target is missed.

**Early warning.** CloudWatch `CPUCreditBalance` trending toward zero; the Task 6
throughput test returning under 1,500 pages/hour.

**Mitigation.** Validate throughput in Week 1 (Task 6, Step 6) rather than assuming it.
Scrapy is I/O-bound, which favours a small instance, but the gzip and base64 work in
the S3 writer is CPU-bound and is the likely culprit if credits drain.

**Contingency.** Upgrade to `t3.small` (~$15/month, comfortably within budget) or run
a second instance splitting sources. Both are cheap; neither is worth a day of
optimisation.

**Owner:** Nguyen · **Review:** at Task 6, then weekly

---

## R-09 🟡 — Disappearance ≠ rented (measurement validity)

| | |
|---|---|
| **Likelihood** | 5 |
| **Impact** | 2 |
| **Score** | **10 — SIGNIFICANT** |

**Description.** Listings vanish for reasons other than renting: expiry, withdrawal,
the landlord deleting to repost fresh. The survival model's target is therefore a
proxy, not the quantity of interest.

**Why likelihood is 5.** This is not a risk of something going wrong — it is
*certainly* true. The risk is failing to handle it well.

**Mitigation.**
- State the assumption explicitly in Report 1, not defensively in Report 3.
- Deduplication catches delete-and-repost, which is the largest contaminant.
- Where a source exposes listing expiry terms, model expiry separately.
- Present a sensitivity analysis: restrict to listings that vanished well before any
  plausible expiry date, and check whether conclusions hold.

**Contingency.** None needed. This is managed through honest framing rather than
elimination. Handled well, it becomes a strength in the oral defence — examiners
respond better to a candidate who identified the validity threat themselves than to
one who claims a proxy is a ground truth.

**Owner:** Nguyen · **Review:** at Tasks 13, 19, 22

---

## R-10 🟡 — Scope creep

| | |
|---|---|
| **Likelihood** | 4 |
| **Impact** | 2 |
| **Score** | **8 — SIGNIFICANT** |

**Description.** Image models, a Streamlit dashboard, PhoBERT embeddings, Airflow,
Iceberg — each individually attractive, collectively fatal to a 6-week timeline.

**Mitigation.** The explicit out-of-scope list in Charter §6 and the pre-agreed cut
order in Schedule §6. Both were written while calm, specifically so that the decision
under pressure is a lookup rather than a judgement call.

**Contingency.** Apply the cut list in order. An "advanced" feature added in Week 5 at
the cost of a rushed report is a net loss on every grading criterion.

**Owner:** Nguyen · **Review:** weekly

---

## R-11 🟢 — Legal or ethical challenge to crawling

| | |
|---|---|
| **Likelihood** | 1 |
| **Impact** | 3 |
| **Score** | **3 — LOW** |

**Description.** A source objects to the crawl, or the course supervisor challenges its
legitimacy.

**Why likelihood is low.** `ROBOTSTXT_OBEY = True`, ≤ 1 req/s, an honest User-Agent
with a contact address, no personal data retained, no login walls circumvented, and no
content republished. This is a textbook-compliant academic crawl.

**Mitigation.** The governance document (`06-data-governance-ethics.md`) exists
precisely to be handed over if asked. A contactable User-Agent means a site operator
can email rather than block.

**Contingency.** If a source requests it, stop crawling that source immediately and
delete its data. Document the request and the response in Report 4 — handling it
correctly is itself a credit to the project.

**Owner:** Nguyen · **Review:** monthly

---

## Summary

| ID | Risk | Score | Status |
|---|---|---|---|
| R-01 | Crawler not live by Sep 18 | 🔴 15 | Open |
| R-04 | Insufficient survival events | 🔴 15 | Open |
| R-02 | Anti-bot deployed mid-project | 🟠 12 | Open |
| R-05 | Duplicate contamination | 🟠 12 | Open |
| R-12 | No free Oracle capacity / cluster too slow | 🟠 12 | Open |
| R-09 | Disappearance ≠ rented | 🟡 10 | Accepted, managed by framing |
| R-07 | Missing coordinates | 🟡 8 | Open |
| R-10 | Scope creep | 🟡 8 | Open |
| R-03 | AWS cost overrun (egress) | 🟡 6 | Reduced — compute now free |
| R-06 | HTML structure changes | 🟡 6 | Mitigated by architecture |
| R-08 | EC2 throughput | 🟡 6 | Open |
| R-13 | Oracle reclaims idle instances | 🟡 6 | Open |
| R-11 | Legal/ethical challenge | 🟢 3 | Mitigated by policy |

**If you check only two things each week: is the crawler still collecting (R-01), and
is the event rate climbing (R-04).** Everything else is recoverable with effort. Those
two are recoverable only with time you do not have.
