# Project Charter — Vietnam Urban Rental Market Intelligence

**Document owner:** Nguyen (nguyenpnt4@fpt.com)
**Status:** Approved framing, pending technical sign-off
**Version:** 1.0
**Date:** 2026-09-11
**Project window:** 2026-09-14 → 2026-10-25 (6 weeks)
**Execution model:** Solo

---

## 1. Executive Summary

This project builds an end-to-end big data system that crawls Vietnam's urban rental
listing market from source websites, processes it on a distributed Spark stack on AWS,
and produces a two-stage predictive model that answers a question landlords and renters
both care about: **what is this unit actually worth, and how long will it take to rent
at that price?**

The distinguishing feature is not the model — it is the **longitudinal panel**. By
re-crawling a fixed cohort of listings every day for 30+ days, the project observes
listings enter and leave the market, producing a genuine time-to-event label that
cannot be obtained from any static dataset. This is what makes the dataset original,
the problem non-trivial, and the "big data" claim defensible on volume, velocity, and
variety simultaneously.

---

## 2. Context: Why This Is a Big Data Problem

Big data status is claimed on three of the five V's, each with a concrete justification.
This section exists because "my data is big" is the single most commonly challenged claim
in a capstone defence.

### 2.1 Volume

| Component | Estimate | Raw size |
|---|---|---|
| Unique listings crawled (4 portals, 6 cities) | 600,000 – 1,000,000 | ~120 GB raw HTML |
| Daily panel re-crawl (200k cohort × 35 days) | 7,000,000 listing-day observations | ~1,050 GB raw HTML |
| Listing photos (sampled, 100k listings × ~6) | ~600,000 images | ~120 GB |
| **Total raw-equivalent** | | **~1.3 TB** |
| **Stored (gzip + Parquet)** | | **~300–350 GB** |

The panel dimension is what creates the volume. A one-shot scrape of 800k listings is
roughly 20 GB compressed — comfortably a laptop problem. Thirty-five daily snapshots of
that market is not.

### 2.2 Velocity

A sustained ingest of **~200,000 pages/day** (~2.3 pages/second aggregate, ~0.6 req/s
per domain) running continuously for five weeks. New listings appear and existing ones
vanish hourly; the pipeline must detect both within a 24-hour window or the survival
labels degrade.

### 2.3 Variety

Four heterogeneous sources with incompatible schemas, plus three data modalities:

- **Structured** — area, bedrooms, price, posting date, coordinates
- **Unstructured text** — Vietnamese free-text descriptions, heavily abbreviated,
  inconsistent diacritics, embedded contact details
- **Binary** — listing photographs
- **External enrichment** — OpenStreetMap POIs and administrative boundaries

### 2.4 Veracity (acknowledged, not claimed as a strength)

Listing data is user-generated and adversarial: duplicate re-posts, bait pricing,
stale listings left active, and deliberately vague addresses. Near-duplicate detection
and outlier handling are treated as first-class pipeline stages, not afterthoughts.

---

## 3. Problem Statement

### 3.1 The business problem

Vietnam's urban rental market is informationally inefficient. There is no published
price index at district or ward granularity, no public benchmark for what a given unit
should rent for, and no visibility into how long units actually take to rent. Landlords
price by imitation and guesswork; renters cannot tell a bargain from a bait listing.

### 3.2 The analytical problem, in two stages

**Stage 1 — Fair rent estimation (Predictive)**

> Given a unit's location, physical attributes, amenities, description text, and
> photographs, estimate its fair monthly rent.

Output: `fair_rent_vnd` per listing, and therefore
`price_gap = (asking_rent − fair_rent) / fair_rent`.

**Stage 2 — Time-on-market / liquidity (Predictive, conditioned on Stage 1)**

> Given the same features **plus `price_gap`**, estimate how long the listing will
> remain on the market before it disappears.

This is a **time-to-event problem with right-censoring**: listings still active at the
end of the observation window have not yet "failed", and treating their observed
duration as the true duration would bias every estimate downward. Handled with
Accelerated Failure Time survival regression, not ordinary regression.

**Stage 3 — Pricing recommendation (Prescriptive)**

Invert Stage 2 over a price grid to produce the deliverable statement:

> *"Price this unit at 7.2M VND and expect ~11 days to rent. At 8.0M VND, expect ~26
> days. Your revenue-maximising price given a 1-month vacancy cost is 7.5M VND."*

### 3.3 Coverage of the four analytics types

| Type | Where it appears | Example output |
|---|---|---|
| **Descriptive** | Report 2 EDA | Rent distribution by district, supply by property type, market composition, seasonality of postings |
| **Diagnostic** | Report 2 + Report 3 interpretation | Feature importance and SHAP-style attribution: *why* is rent high here, *why* do some units sit unrented |
| **Predictive** | Report 3 models | Fair-rent regressor; time-on-market survival model |
| **Prescriptive** | Report 3 conclusions | Optimal price recommendation; mispricing alerts; district-level investment ranking |

The project's declared **primary approach is Predictive**, with a Prescriptive
extension. Descriptive and diagnostic work is foundational rather than the headline.

---

## 4. Data Requirements

### 4.1 Sources and their role

| Source | Segment | Role | robots.txt status |
|---|---|---|---|
| `batdongsan.com.vn` | Formal apartments, houses | Primary — highest quality structured fields | Green — 3 internal router paths blocked only |
| `phongtro123.com` | Informal rooms (phòng trọ) | Primary — covers the student/worker segment | Green — `/admincp`, `/api` blocked only |
| `mogi.vn` | Formal, mid-market | Secondary — cross-source validation | Green — `/api/`, `/Property/`, `/MarketPrice/` blocked |
| `nhatot.com` | Mixed, high volume | Secondary — **sitemap-only crawl** | Amber — search/filter params disallowed; declares `ai-train=no` |
| OpenStreetMap (Overpass API) | POIs, boundaries | Enrichment — distance to universities, industrial parks, transit, markets | Open data, ODbL |
| GSO / provincial statistics portals | Population, income | Enrichment — district-level context | Public statistics |

### 4.2 Geographic scope

Six markets, chosen to give genuine heterogeneity rather than one city's quirks:

1. **Ho Chi Minh City** — largest market, all segments
2. **Hanoi** — second market, different price structure
3. **Da Nang** — mid-size, tourism-influenced
4. **Binh Duong** — industrial belt, worker housing
5. **Dong Nai** — industrial belt, worker housing
6. **Can Tho** — regional centre, thin market (deliberate contrast case)

### 4.3 Required fields

**Must have** (a listing missing these is unusable for modelling):
`listing_id`, `source`, `url`, `asking_rent_vnd`, `area_sqm`, `province`, `district`,
`property_type`, `first_seen_date`, `last_seen_date`, `crawl_ts`

**Should have:** `ward`, `street`, `latitude`, `longitude`, `bedrooms`, `bathrooms`,
`floor`, `furnishing_level`, `description_text`, `amenities[]`, `posted_date`,
`image_urls[]`

**Nice to have:** `landlord_type`, `deposit_terms`, `utilities_included`, `view_count`

**Deliberately excluded at parse time:** contact names, phone numbers, and any personal
identifiers. See the Data Governance document — these are stripped in the bronze→silver
transition and never persist to an analysable layer.

### 4.4 Collection method

- **Frontier discovery:** daily sitemap polling. All four sources publish sitemaps,
  which provides a complete and cheap URL inventory and makes new-listing detection a
  set difference rather than a paginated crawl.
- **Fetching:** Scrapy with AutoThrottle, per-domain concurrency of 2, courtesy delay
  targeting ≤1 req/s per domain, honest descriptive User-Agent with contact address,
  conditional requests via `ETag`/`If-Modified-Since` where supported.
- **JS-rendered pages:** Playwright fallback, used only for sources that require it,
  with a hard budget cap because it is ~20× the cost per page.
- **Panel design:** a fixed cohort of ~200,000 listings selected at end of Week 2 is
  re-fetched every 24 hours until Week 6. Cohort membership is frozen; new listings
  discovered after cohort freeze are tracked in a separate incremental set.
- **Disappearance detection:** a listing is marked `gone` after **two consecutive**
  daily failures returning 404/410 or a "no longer available" page state. Two
  consecutive failures, not one, to avoid transient errors creating false events.

---

## 5. Success Criteria

### 5.1 Data criteria (gate for Report 2)

- [ ] ≥ 500,000 unique listings collected across ≥ 3 sources
- [ ] ≥ 250 GB stored across bronze/silver layers
- [ ] ≥ 17 consecutive days of panel observation on the full cohort (≥ 22 days on the
      provisional cohort frozen Sep 20)
- [ ] ≥ 25% of panel cohort has observed a disappearance event (uncensored)
- [ ] Silver layer parse success rate ≥ 95% of bronze documents
- [ ] Zero personal identifiers present in silver or gold layers (automated check)

### 5.2 Model criteria (gate for Report 3)

- [ ] Fair-rent model: **MAPE ≤ 20%** on a held-out temporal split, beating a
      district-median baseline by ≥ 30% relative MAPE reduction
- [ ] Survival model: **concordance index ≥ 0.65**
- [ ] Demonstrated that `price_gap` carries statistically meaningful signal in the
      survival model (not merely included)
- [ ] All modelling executed in Spark MLlib on a multi-node cluster, with runtime
      evidence captured for the report

### 5.3 Engineering criteria

- [ ] Pipeline is re-runnable end to end from a documented command sequence
- [ ] Total cloud spend ≤ $50 (Spark compute is free on Oracle Always Free; remaining
      cost is S3 storage, requests, and egress)
- [ ] Crawl activity provably compliant with each source's robots.txt

### 5.4 Portfolio criteria

- [ ] Public GitHub repository with README, architecture diagram, and reproducible setup
- [ ] At least one interactive artefact (map or dashboard) a non-technical viewer can use
- [ ] Four written reports plus an oral presentation deck

---

## 6. Scope Boundaries

### In scope

Crawling, storage, cleaning, deduplication, EDA, feature engineering, the two models,
the prescriptive layer, visualisation, and the four reports.

### Explicitly out of scope (YAGNI)

These are named so they can be defended as deliberate choices rather than omissions:

- **Real-time streaming.** Daily batch matches the rate at which the rental market
  actually changes. Kafka here would be architecture theatre.
- **A production web application.** A dashboard artefact, yes; a deployed multi-user
  service, no.
- **Sale-price listings.** Rental only. Sale transactions do not resolve within a
  6-week observation window.
- **Deep learning on images.** A sampled CNN-embedding experiment is a stretch goal
  only. It is not on the critical path.
- **Facebook and other social sources.** Excluded on terms-of-service, personal-data,
  and reproducibility grounds. The resulting coverage bias is documented as a
  limitation rather than concealed.
- **Sub-daily crawl frequency.** No analytical benefit; multiplies cost and load on
  the sources.

---

## 7. Deliverable Map

| Report | Due | Content | Depends on |
|---|---|---|---|
| **Report 1 — Proposal** | End Week 1 (Sep 20) | Sections 2, 3, 4 of this charter, plus the technical design summary | This document |
| **Report 2 — Data Tasks** | End Week 3 (Oct 4) | Collection architecture, storage design, cleaning methodology, EDA findings, Spark/Hadoop justification | Crawler live by Day 5; silver layer complete |
| **Report 3 — Model & Results** | End Week 5 (Oct 18) | Both models, evaluation, tuning, interpretation, visualisation, recommendations | ≥28 days panel data |
| **Report 4 — Final + Oral** | End Week 6 (Oct 25) | Consolidated report and capstone presentation | All prior reports |

---

## 8. The Critical Path — Read This Twice

The survival model needs as many days of daily observation as the calendar allows.
Report 3 is due at the end of Week 5. Counting backwards:

```
Report 3 due ................................. Oct 18  (end of Week 5)
Modelling needs data frozen by ............... Oct 12  (panel censor date)
Full cohort frozen and probing from .......... Sep 25  → 17 days observed
Provisional cohort frozen from ............... Sep 20  → 22 days observed
Crawler must therefore be LIVE by ............ Sep 18  (Day 5)
```

**Seventeen days is tighter than ideal**, which is why the provisional cohort exists:
freezing a smaller cohort five days early costs almost nothing and buys five extra days
of observation as insurance. If the event rate at the Week 3 checkpoint is weak, the
censor date moves to Oct 14 and modelling compresses. Both contingencies are in the
Risk Register under R-04.

**There is no slack in this.** Every day the crawler is late is a day of survival
labels that cannot be recovered later, because the observation window cannot be
back-filled. If the crawler is not collecting by Day 5, escalate to the fallback in
the Risk Register (R-01) rather than continuing to debug.

Corollary: Week 1 builds a crawler that is *running*, not one that is *elegant*.
Refactoring happens in Week 2 while data accumulates in the background.

---

## 9. Assumptions

1. Oracle Cloud Always Free provides the Spark cluster at no cost, and AWS free tier
   covers the crawler instance. Residual S3 cost (~$20/month) is acceptable.
2. Target sites remain reachable and do not deploy new anti-bot measures mid-project.
3. A single always-on `t3.micro` instance can sustain 200k pages/day. *(Validated in
   Week 1, Task 6 — if not, scale to two instances.)*
4. Approximately 25–40% of the panel cohort will turn over within the observation
   window, giving sufficient uncensored events. *(Validated at the Week 3 checkpoint.)*
5. Listing disappearance is a usable proxy for "rented". This is an assumption, not a
   fact, and is treated as a stated limitation throughout.

---

## 10. Sign-off

| Role | Name | Status |
|---|---|---|
| Project owner | Nguyen | — |
| Course supervisor | — | Pending Report 1 approval |
