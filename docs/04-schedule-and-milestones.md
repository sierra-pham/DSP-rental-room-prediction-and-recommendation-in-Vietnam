# Schedule & Milestones

**Project window:** Mon 2026-09-14 → Sun 2026-10-25 (6 weeks)
**Execution:** Solo
**Plan:** `03-implementation-plan.md`

---

## 1. The Critical Path

```
                    Sep 14   Sep 21   Sep 28   Oct 5    Oct 12   Oct 19   Oct 25
                      │        │        │        │        │        │        │
 Crawler build    ████│        │        │        │        │        │        │
 CRAWL LIVE ──────────►Sep 18  │        │        │        │        │        │
 Discovery crawl      ████████████████████████████████████████████ │        │
 Panel freeze ─────────────────►Sep 25  │        │        │        │        │
 Panel observation             ████████████████████████████►Oct 12 │        │
                               │◄──── 17 days ────►│       │       │        │
 Silver/gold pipeline     ██████████████│        │        │        │        │
 EDA                           │   ████████      │        │        │        │
 Feature eng + rent model      │        │   ██████████    │        │        │
 Survival + prescriptive       │        │        │   ██████████    │        │
 Reports                    R1 │        │     R2 │        │     R3 │     R4 │
```

**The one irreversible constraint:** panel observation cannot be compressed or
back-filled. Every other task can be shortened, reordered, or partially descoped.
This one cannot. It is why the crawler ships on Day 5 in a rough state rather than on
Day 12 in a polished one.

**Note on the observation window:** freezing the cohort on Sep 25 and censoring on
Oct 12 gives **17 days** of panel observation, not the 30 in the ideal case. This is
sufficient for the survival model provided the event rate holds up — short-let rooms
turn over fast — but it is tighter than comfortable. Two ways to improve it, in
preference order:

1. **Freeze a provisional cohort on Sep 20** from whatever has been crawled by then,
   even if it is only 60k listings, and freeze the full 200k cohort on Sep 25 as
   planned. The provisional cohort gets 22 days of observation and acts as insurance.
2. **Push the censor date to Oct 14** and compress modelling into 4 days. Only do this
   if the event rate at the Week 3 checkpoint is below target.

Option 1 costs almost nothing and is strongly recommended.

---

## 2. Week-by-Week

### Week 1 — Sep 14–20: Get data flowing

| Day | Focus | Tasks |
|---|---|---|
| Mon 14 | Repo, config, AWS setup, budget alerts | 1, 2 |
| Tue 15 | Normalisation + first parser, with fixtures | 3 |
| Wed 16 | Frontier + sitemap poller | 4, 5 |
| Thu 17 | Spider + S3 writer, throughput validation | 6 |
| **Fri 18** | **🔴 CRAWLER LIVE — hard deadline** | 6 |
| Sat 19 | Remaining three spiders and parsers | 7 |
| Sun 20 | Report 1; provisional cohort freeze | 8 |

**Exit criteria:** crawler running on cron; ≥ 50k listings in bronze; Report 1 submitted.

**If Friday arrives and the crawler is not live:** stop adding sources. Ship the one
spider that works, on cron, tonight. Three sources crawled from Sep 25 is a worse
project than one source crawled from Sep 18.

---

### Week 2 — Sep 21–27: Bronze → silver, freeze the panel

| Day | Focus | Tasks |
|---|---|---|
| Mon 21 | Spark parse job + local tests | 9 |
| Tue 22 | Provision Oracle free cluster; first real Spark run | 10 |
| Wed 23 | Deduplication (MinHash LSH) | 11 |
| Thu 24 | Dedup validation — manually read 20 clusters | 11 |
| **Fri 25** | **🔴 PANEL COHORT FREEZE + daily probing live** | 12 |
| Sat 26 | Verify two consecutive probe days landed | 12 |
| Sun 27 | Buffer / catch-up | — |

**Exit criteria:** ≥ 300k listings; silver layer building on the free Spark cluster;
panel probing daily
and verified for two consecutive days.

---

### Week 3 — Sep 28 – Oct 4: Panel, enrichment, EDA, Report 2

| Day | Focus | Tasks |
|---|---|---|
| Mon 28 | Panel fact table + survival labels | 13 |
| Tue 29 | **Event-rate checkpoint** — see §4 | 13 |
| Wed 30 | OSM fetch + H3 enrichment | 14 |
| Thu 1 | EDA: market structure | 15 |
| Fri 2 | EDA: geography, maps, KM curves | 15 |
| Sat 3 | Write Report 2 | 16 |
| **Sun 4** | **🔴 Report 2 due** | 16 |

**Exit criteria:** ≥ 500k listings; gold layer complete; ≥ 10 days panel; Report 2
submitted.

---

### Week 4 — Oct 5–11: Features and the rent model

| Day | Focus | Tasks |
|---|---|---|
| Mon 5 | Feature engineering + leakage guard tests | 17 |
| Tue 6 | Baseline model; record the number | 18 |
| Wed 7 | GBT training and evaluation | 18 |
| Thu 8 | Hyperparameter tuning | 18 |
| Fri 9 | Score `price_gap` across all listings | 18 |
| Sat 10 | Buffer — reserved for the rent model missing its criterion | 18 |
| Sun 11 | Buffer | — |

**Exit criteria:** rent model at MAPE ≤ 20%, beating baseline by ≥ 30% relative;
`price_gap` written to `gold/features_survival/`.

---

### Week 5 — Oct 12–18: Survival, prescriptive, Report 3

| Day | Focus | Tasks |
|---|---|---|
| **Sun 12** | **🔴 PANEL CENSOR DATE — data frozen for modelling** | 13 |
| Mon 13 | AFT survival model | 19 |
| Tue 14 | `price_gap` ablation + left-truncation sensitivity | 19 |
| Wed 15 | Prescriptive layer + worked examples | 20 |
| Thu 16 | Interpretation notebooks and figures | 21 |
| Fri 17 | Write Report 3 | 22 |
| **Sat 18** | **🔴 Report 3 due** | 22 |
| Sun 19 | Buffer | — |

**Note:** the crawler keeps running past the censor date. Later data is not used for
modelling but strengthens the descriptive story and demonstrates the pipeline's
stability over six weeks.

**Exit criteria:** survival model at concordance ≥ 0.65; ablation result recorded
either way; Report 3 submitted.

---

### Week 6 — Oct 19–25: Consolidation and defence

| Day | Focus | Tasks |
|---|---|---|
| Mon 19 | Repo polish, README, reproducibility check | 23 |
| Tue 20 | Clean-checkout verification; full test suite | 23 |
| Wed 21 | Write Report 4 | 24 |
| Thu 22 | Build the deck | 24 |
| Fri 23 | Rehearse; prepare the four defence questions | 24 |
| Sat 24 | Rehearse again; final polish | 24 |
| **Sun 25** | **🔴 Report 4 + oral presentation** | 24 |

**Exit criteria:** all four reports submitted; presentation delivered; repository public
and reproducible; `v1.0-capstone` tagged.

---

## 3. Milestone Register

| ID | Milestone | Date | Hard? | Verification |
|---|---|---|---|---|
| M1 | AWS account with budget alerts | Sep 14 | No | Alert email received from a test threshold |
| M2 | **Crawler live** | **Sep 18** | **YES** | Bronze objects present for today's `dt=` |
| M3 | Report 1 submitted | Sep 20 | Yes | Submission confirmation |
| M4 | All four spiders running | Sep 21 | No | Four `source=` prefixes in today's partition |
| M5 | **Panel cohort frozen** | **Sep 25** | **YES** | `SELECT COUNT(*) WHERE in_panel=1` ≈ 200k |
| M6 | Spark cluster live, silver layer building | Sep 23 | No | 2 workers ALIVE at `:8080`; `silver/listings/` non-empty; QA job green |
| M7 | Event rate ≥ 8% | Sep 29 | Checkpoint | Athena query on `features_survival` |
| M8 | Report 2 submitted | Oct 4 | Yes | Submission confirmation |
| M9 | Rent model meets criteria | Oct 9 | No | `models/metrics/rent_*.json` |
| M10 | **Panel censored** | **Oct 12** | **YES** | `build_panel.py --censor-date 2026-10-12` run |
| M11 | Survival model meets criteria | Oct 14 | No | `models/metrics/survival_*.json` |
| M12 | Report 3 submitted | Oct 18 | Yes | Submission confirmation |
| M13 | Repo reproducible | Oct 20 | No | Clean-checkout run succeeds |
| M14 | Report 4 + oral | Oct 25 | Yes | Presentation delivered |

---

## 4. Go / No-Go Checkpoints

Three moments where you stop, measure, and decide. Each has a defined trigger and a
defined fallback, so the decision is made against a number rather than against how
tired you are that day.

### Checkpoint A — Fri Sep 18, end of day: "Is data flowing?"

```sql
SELECT COUNT(*) FROM urls WHERE last_fetch_ts IS NOT NULL;
```

| Result | Decision |
|---|---|
| ≥ 20,000 fetched | **GO** — proceed to Week 2 as planned |
| 5,000–20,000 | **GO with caution** — spend Saturday on throughput, not new sources |
| < 5,000 | **NO-GO** → escalate to Risk R-01. Drop to the single easiest source (phongtro123), strip everything non-essential, and be live by Saturday |

### Checkpoint B — Tue Sep 29: "Will there be enough events?"

```sql
SELECT SUM(CASE WHEN event_observed THEN 1 ELSE 0 END) * 1.0 / COUNT(*)
FROM gold.features_survival;
```

| Result | Decision |
|---|---|
| ≥ 12% | **GO** — on track for ≥ 25% by the censor date |
| 8–12% | **GO with mitigation** — extend the censor date to Oct 14; over-sample short-let sources in the cohort |
| < 8% | **NO-GO** → escalate to Risk R-04. Pivot Report 3's primary result to the rent model plus a *descriptive* survival analysis (KM curves only, no AFT regression) |

### Checkpoint C — Fri Oct 9: "Is the rent model good enough to build on?"

| Result | Decision |
|---|---|
| MAPE ≤ 20% | **GO** — proceed to the survival stage |
| MAPE 20–30% | **GO** — use the model as-is; discuss the gap honestly in Report 3. A 25% MAPE with a sound method beats a 15% MAPE with hidden leakage |
| MAPE > 30% | **Investigate before proceeding**, in this order: duplicate leakage → `thỏa thuận` rows in training → missing coordinates → outlier rents. Budget one day; if unresolved, proceed anyway and make the failure analysis a section of the report |

---

## 5. Time Budget

Solo, six weeks, assuming ~25–30 hours/week (~165 hours total):

| Phase | Estimate | % |
|---|---|---|
| Crawler development | 30 h | 18% |
| Pipeline (parse, dedup, panel, enrich) | 35 h | 21% |
| EDA and visualisation | 20 h | 12% |
| Modelling (both stages + prescriptive) | 35 h | 21% |
| Reports and presentation | 30 h | 18% |
| Buffer / debugging | 15 h | 9% |

**9% buffer is thin.** The realistic protections are: the crawler runs unattended
while you work on other things, and Weeks 4 and 5 each carry a deliberately empty
buffer day. Use them for slippage, not for scope.

**Weekly rhythm:** 30 minutes every Monday reviewing the AWS cost dashboard, crawl
volume, and event rate. Logged in `docs/weekly-log.md`. Three data points make a trend
visible; noticing a problem in Week 4 that started in Week 2 is the expensive failure
mode.

---

## 6. What to Cut, In Order

When time runs short — and it will — cut in this order. Decided now, calmly, rather
than at 2am in Week 5.

1. **Image collection and CNN features** — stretch goal, never on the critical path
2. **The `nhatot` source** — the hardest to crawl and the only one with an
   `ai-train=no` signal to argue about
3. **The Streamlit dashboard** — notebook figures are sufficient for every report
4. **Hyperparameter tuning depth** — a single sensible configuration is acceptable;
   say so in the report
5. **Can Tho and Da Nang** — reduce to four markets
6. **The prescriptive layer** — reluctantly. It is the project's differentiator, so
   cut it only if Report 3 is otherwise at risk

**Never cut:** the panel (it is the whole project), the temporal split (without it the
results are fiction), the PII stripping (it is an ethics obligation, not a feature),
or bronze immutability (it is your only insurance against parser bugs).
