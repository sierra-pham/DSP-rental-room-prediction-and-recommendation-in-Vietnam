# Data Governance & Ethics

**Project:** Vietnam Urban Rental Market Intelligence
**Purpose:** Academic capstone, non-commercial
**Contact:** nguyenpnt4@fpt.com

This document exists to be handed to a supervisor, a site operator, or an examiner
without modification. It states what is collected, on what basis, and what is not
collected — and it is also the source for the "Data Ethics" section of Reports 1 and 4.

---

## 1. Principles

1. **Collect the minimum necessary.** Fields not required by a stated research question
   are not retained.
2. **Never collect personal data into an analysable layer.** Names, phone numbers,
   emails, and social handles are removed before any analysis or modelling.
3. **Impose no meaningful burden on the source.** The crawl is designed to be a
   rounding error in each site's traffic.
4. **Be identifiable and reachable.** The User-Agent carries a project name and a
   working contact address. A site operator who objects can email rather than block.
5. **Respect stated preferences.** `robots.txt` is obeyed mechanically, not just in
   spirit.
6. **Do not republish.** No listing content, image, or description is reproduced
   publicly. Only aggregates and model outputs appear in reports and the repository.
7. **Be honest about bias.** Sources excluded for ethical reasons create coverage gaps.
   Those gaps are documented, not concealed.

---

## 2. robots.txt Compliance Matrix

Verified 2026-09-11. **Re-verify at the start of each week** — robots.txt can change,
and a rule added in Week 3 applies from Week 3.

| Source | `User-agent: *` | Relevant Disallow rules | Crawl-delay | Our handling |
|---|---|---|---|---|
| `batdongsan.com.vn` | `Allow: /` | `/microservice-architecture-router/`, `/microservice-architecture-router-mobile`, `/HandlerWeb/UserHandler.ashx` | none declared | Listing pages only. Self-imposed 1 req/s. |
| `phongtro123.com` | `Allow: /` | `/admincp`, `/api`, plus various tracking/pagination params (`s`, `paged`, `cat`, `ref`, `trk`) | none declared | Sitemap-driven; no parameterised URLs fetched. Self-imposed 1 req/s. |
| `mogi.vn` | `Allow: /` | `/trang-ca-nhan/`, `/api/`, `/template/`, `/MarketPrice/`, `/Property/`, `*gclid=`, `*wbraid=` | none declared | Listing detail pages only; personal-profile pages never fetched. Self-imposed 1 req/s. |
| `nhatot.com` | `Allow: /` | `/user/`, `/nhan/`, `*chatroom*`, `*q=`, `*f=`, `*sort=`, `*page=`, `/dashboard`, `/feed` | none declared | **Sitemap-only.** No search, filter, or pagination URLs. No user or chat pages. Self-imposed 1 req/s. |

**Mechanical enforcement:** `ROBOTSTXT_OBEY = True` in `crawler/settings.py`. Scrapy
fetches and honours each site's robots.txt automatically; this is not left to
developer discipline.

**No crawl-delay is declared by any source.** In its absence a crawler may legitimately
proceed at its own pace. This project nonetheless imposes a ceiling of **1 request per
second per domain** — well below what any of these sites would notice. See §4.

---

## 3. The nhatot.com `ai-train=no` Signal

`nhatot.com` declares a Content-Signal header (`search=yes, ai-input=yes, ai-train=no`)
and blocks a list of AI crawler user-agents including `GPTBot`, `CCBot`, `ClaudeBot`,
and `anthropic-ai` from all paths.

**Assessment.** Those `Disallow` directives are scoped to named AI-training crawler
user-agents. This project's crawler is not one of them, and the `User-agent: *` rules
permit listing pages. So the crawl is technically permitted.

However, the `ai-train=no` signal expresses an intent that deserves a direct answer
rather than a technicality.

**Position taken.** The signal addresses the use of site content as training corpus for
general-purpose generative models — reproducing or substituting for the content itself.
This project trains a statistical regression on *structured attributes* (area, district,
room count, price) to estimate market values. It does not reproduce listing text, does
not generate content resembling the source, and does not build a product competing with
the site. The declared `ai-input=yes` signal indicates that using content as input to
analysis is acceptable to the operator.

**Nonetheless, additional restrictions are applied to this source specifically:**

- Sitemap-only crawling; no search, filter, or pagination URLs
- A hard cap of 20,000 pages/day
- Description text from nhatot is used for TF-IDF features only; **no raw nhatot
  description text is reproduced in any report or repository artefact**
- If nhatot's operators object, the source is dropped and its data deleted

**If in doubt, drop the source.** The remaining three carry sufficient volume, and the
project is not worth an argument with a site operator. This position is stated in
Report 1 so the examiner sees the reasoning was made in advance, not constructed
afterwards.

---

## 4. Crawl Politeness Policy

| Control | Setting | Rationale |
|---|---|---|
| Rate limit | ≤ 1 req/s per domain | ~0.6 req/s in steady state at the 200k/day target |
| Concurrency | 2 per domain | Low enough to avoid connection-pool pressure |
| AutoThrottle | Enabled, target concurrency 1.0 | Backs off automatically as the site slows — the crawl yields to real users |
| Retry | Max 2, exponential backoff | No hammering on failure |
| 429 handling | Back off, do not retry aggressively | An explicit "slow down" is honoured |
| Crawl window | 01:00–06:00 ICT for bulk discovery | Off-peak for Vietnamese traffic |
| User-Agent | `VN-Rental-Research/1.0 (academic project; nguyenpnt4@fpt.com)` | Identifiable and contactable |
| Caching | Conditional requests where ETag/Last-Modified supported | Avoids re-downloading unchanged pages |
| No circumvention | No CAPTCHA solving, no login walls, no IP rotation to evade blocks | A block is a decision to be respected, not an obstacle to route around |

**Proportionality.** At ~50,000 requests/day against a site serving millions of
pageviews, the crawl is a rounding error in traffic terms. This is a deliberate design
target, not an accident of implementation.

---

## 5. Personal Data

### 5.1 Legal framing

Vietnam's **Decree 13/2023/NĐ-CP on Personal Data Protection** governs the processing
of personal data. Rental listings routinely contain personal data: contact names, phone
numbers, and sometimes photographs of identifiable people.

The project's position is to avoid the question entirely rather than to seek a basis
for processing: **personal data is not retained in any analysable form.**

### 5.2 What is stripped, and where

Stripping occurs in the **bronze → silver** transition, inside `parse_bronze.py`, before
any data reaches a queryable table.

| Data type | Treatment |
|---|---|
| Phone numbers (all Vietnamese formats) | Replaced with `<PHONE>` |
| Email addresses | Replaced with `<EMAIL>` |
| Zalo / Facebook / Viber handles | Replaced with `<SOCIAL>` |
| Contact names | Not extracted by any parser |
| Agent/landlord profile pages | Never fetched (`/trang-ca-nhan/` is disallowed anyway) |
| Listing photographs | Sampled only; faces not processed; not republished |

### 5.3 Why bronze still contains personal data

Bronze holds raw HTML exactly as served, which includes contact details. This is a
deliberate trade-off:

- Bronze is **private** — the S3 bucket blocks all public access, and no bronze object
  is ever published, shared, or committed to the repository.
- Bronze immutability is the project's insurance against parser bugs. Sanitising at
  crawl time would mean a parser error becomes an unrecoverable data loss, because the
  original pages will have changed.
- Nothing derived from bronze retains personal data.

**Controls on bronze:** public access blocked at the bucket level; access restricted to
a single IAM principal; no bronze sample ever appears in a report, notebook, or
repository; bronze is deleted at project completion (see §7).

### 5.4 Automated verification

`parse_bronze.py` runs a regex scan over `description_clean` before writing silver and
**raises and fails the job** if any phone-number pattern survives. A pipeline that
leaks personal data does not complete; it errors.

An independent audit query is run before each report submission:

```sql
SELECT COUNT(*) FROM silver.listings
WHERE regexp_like(description_clean, '(\+?84|0)[0-9]{9,10}')
   OR regexp_like(description_clean, '[\w.+-]+@[\w-]+\.[\w.]+');
-- Expected: 0
```

---

## 6. Excluded Sources and the Resulting Bias

### 6.1 Facebook and social platforms — excluded

**Grounds for exclusion:**

1. **Terms of service.** Meta's terms prohibit automated data collection, and this is
   actively enforced.
2. **Personal data.** Group posts contain names, phone numbers, faces, and
   addresses — with no meaningful separation between listing content and personal data,
   and no reasonable expectation by posters that their posts would be collected into a
   research dataset.
3. **Reproducibility.** Content behind a login cannot be independently verified by an
   examiner, which undermines the integrity of the research.
4. **Consent context.** A person posting a room in a neighbourhood group is addressing
   that group, not the public.

### 6.2 The resulting bias, stated plainly

A substantial share of Vietnam's informal rental market — particularly low-cost phòng
trọ, short-term sublets, and direct landlord-to-tenant arrangements — is transacted
through Facebook groups and Zalo rather than listing portals.

**Consequences for the findings, which must be stated in every report:**

- The dataset over-represents the **formal, agent-mediated, higher-value** segment.
- Rent estimates at the low end are based on the portal-listed subset, which likely
  skews toward professionally-managed properties.
- Time-on-market estimates reflect portal dynamics, not the informal market, where
  units may transact faster through personal networks.
- `phongtro123.com` partially compensates by covering the informal room segment, and
  including it is a deliberate mitigation — but it does not close the gap.

**This limitation is a finding, not a flaw.** Quantifying where a data source is blind
is a legitimate research contribution, and it is a far stronger position in an oral
defence than claiming a coverage the data does not have.

---

## 7. Data Retention and Disposal

| Layer | Retention | Disposal |
|---|---|---|
| Bronze (raw HTML, contains PII) | Duration of the project | **Deleted at project completion (target: 2026-11-30)** |
| Silver / gold (PII-stripped) | Retained for portfolio purposes | May be retained indefinitely |
| Images | Duration of the project | Deleted with bronze |
| Models and metrics | Retained | Contain no personal data |
| A published sample dataset | — | Only aggregated, PII-free, and only if a source's terms permit |

**Disposal command, to be run at completion:**

```bash
aws s3 rm s3://vn-rental-dsp/bronze/ --recursive
aws s3 rm s3://vn-rental-dsp/images/ --recursive
```

Record the date this was executed in `docs/weekly-log.md`.

---

## 8. Publication Policy

**What may be published** (repository, reports, presentation):

- Aggregated statistics: medians, distributions, counts by district
- Model artefacts, coefficients, and evaluation metrics
- All source code
- Maps and charts derived from aggregates
- Anonymised, paraphrased examples where a concrete illustration is needed

**What may not be published:**

- Any individual listing's full record
- Any listing description text verbatim
- Any listing photograph
- Any contact information whatsoever
- Raw bronze or silver data, in whole or in part

**Repository note.** The public GitHub repository contains code and aggregate outputs
only. `.gitignore` excludes `tests/fixtures/*.html` from public commits — the saved HTML
fixtures contain real listing content including contact details. Parser tests in CI run
against **synthetic** fixtures with the same structure and fabricated contact details.

---

## 9. Responding to an Objection

If a site operator, a listing poster, or any other party objects to this project's data
collection:

1. **Stop crawling that source immediately.** Do not negotiate first.
2. Acknowledge within 24 hours from the contact address in the User-Agent.
3. Delete that source's data from all layers.
4. Document the request, the response, and the timeline in Report 4.
5. Do not resume without explicit written permission.

The cost of losing one source is a paragraph in the limitations section. The cost of
handling an objection badly is the credibility of the entire project.

---

## 10. Ethics Statement for Reports

*Short version, for inclusion in Reports 1 and 4:*

> This project collects publicly accessible rental listing data from four Vietnamese
> property portals for non-commercial academic research. All crawling obeys each site's
> robots.txt, is rate-limited to at most one request per second per domain, and
> identifies itself with a contactable User-Agent string. Personal data — contact
> names, phone numbers, email addresses, and social media handles — is removed before
> any analysis, verified by an automated check that fails the pipeline if any such data
> is detected downstream. No listing content, description text, or photograph is
> republished; only aggregate statistics and model outputs appear in this work. Social
> media sources were deliberately excluded on terms-of-service, personal-data, and
> reproducibility grounds; the resulting under-representation of Vietnam's informal
> rental market is documented as a limitation of these findings.
