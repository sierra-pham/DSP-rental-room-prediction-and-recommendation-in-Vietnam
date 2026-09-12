---
type: source
source: nhatot
domain: nhatot.com
needs_js: true
sitemap_only: true
rental_url_pattern: "/thue-"
robots_checked: 
robots_ok: 
status: not-started
tags: [source]
---

# nhatot

**Domain:** `nhatot.com` · **JS required:** yes — Playwright/Splash path needed · **Sitemap-only** (robots.txt disallows search/filter params)

Defined in [config/sources.yaml](../../../config/sources.yaml); crawled at
`max_rps_per_domain: 1.0` under UA `VN-Rental-Research/1.0 (academic project; nguyenpnt4@fpt.com)`.

## robots.txt

Re-verified weekly — WBS [1.3.2](../tasks/WBS-1.3.2.md).

| Checked | Verdict | Crawl-delay | Disallowed paths that matter |
|---|---|---|---|
|  |  |  |  |

## Sitemaps

- `https://www.nhatot.com/sitemap.xml`

Rental URLs match `/thue-`.

## URL shape

| Kind | Pattern | Example |
|---|---|---|
| Rental detail |  |  |

## Selectors

| Field | Selector | Notes |
|---|---|---|
| price |  |  |
| area |  |  |
| bedrooms |  |  |
| district |  |  |
| posted date |  |  |

## Quirks

<!-- unit conventions, "Liên hệ" prices, agent duplicate posting patterns -->

## Breakage history

Linked risk: [R-06 — HTML structure changes](../risks/R-06.md)

| Date | What broke | Fix | Fixture refreshed |
|---|---|---|---|
