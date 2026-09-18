# EC2 Crawl Deployment — Design Spec

**Date:** 2026-09-18
**Status:** Approved
**Implements:** Implementation Plan Tasks 6–7 (spider deployment), Task 12 (panel probing)
**Spec reference:** `02-technical-design-spec.md` §4 (collection), §7.3 (panel design)

---

## 1. Goal

Deploy the four Scrapy spiders (batdongsan, phongtro123, mogi, nhatot) to a single
always-on EC2 instance so that bronze data accumulates daily in S3 without manual
intervention. Every day without crawling is permanently lost observation data — survival
labels cannot be back-filled.

### Success criteria

- All four `source=` prefixes present in `bronze/listings/dt=<today>/` by end of Day 1
- Throughput >= 2,000 pages/hour per source, across a sequential four-source pass completing in <= 6 hours
- Panel probe cron runs daily after cohort freeze
- Frontier state backed up to S3 hourly
- Instance is disposable — a fresh EC2 can be restored from S3 state backup

---

## 2. Infrastructure

### 2.1 EC2 Instance

| Parameter | Value | Rationale |
|---|---|---|
| Instance type | `t3.micro` | Free tier eligible, 2 vCPU / 1 GB, sufficient for sequential crawling |
| AMI | Ubuntu 24.04 LTS (amd64) | Stable, Python 3.12 in apt |
| Region | `ap-southeast-1` (Singapore) | Same region as S3 bucket, zero cross-region transfer |
| Storage | 20 GB gp3 (EBS) | Free tier covers 30 GB; frontier.db < 500 MB, repo < 100 MB |
| Key pair | Ed25519 SSH key | Create or reuse existing |

### 2.2 Security Group

| Rule | Port | Source | Purpose |
|---|---|---|---|
| Inbound SSH | 22 | Your IP CIDR (e.g. `x.x.x.x/32`) | Administration |
| Outbound all | All | 0.0.0.0/0 | Crawling + S3 uploads |

No web server ports needed. The instance does not serve traffic.

**Warning:** Do not use `0.0.0.0/0` for SSH. A public SSH port receives thousands of
brute-force attempts per day. If your IP is dynamic, use AWS Systems Manager Session
Manager (no inbound port required) or restrict to your ISP's CIDR range.

### 2.3 IAM

**Preferred: Instance profile with IAM role** (no credentials on disk).

Policy:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": ["s3:PutObject", "s3:GetObject", "s3:ListBucket"],
      "Resource": [
        "arn:aws:s3:::vn-rental-dsp",
        "arn:aws:s3:::vn-rental-dsp/*"
      ]
    }
  ]
}
```

**Fallback:** If instance profiles are not available (e.g., lab account restrictions),
use `aws configure` with an IAM user access key. The key must NOT be committed to git.

---

## 3. Software Setup

### 3.1 System packages

```bash
sudo apt-get update
sudo apt-get install -y python3.12 python3.12-venv python3-pip git
```

### 3.2 Project deployment

```bash
cd /opt
sudo git clone https://github.com/sierra-pham/DSP-rental-room-prediction-and-recommendation-in-Vietnam.git vn-rental-dsp
sudo chown -R ubuntu:ubuntu /opt/vn-rental-dsp
cd /opt/vn-rental-dsp
python3.12 -m venv .venv
source .venv/bin/activate
pip install -e "."
```

Nhatot requires Playwright (JS rendering). Install separately:

```bash
pip install -e ".[js]"
playwright install --with-deps chromium
```

### 3.3 AWS credentials

With instance profile (preferred — no setup needed, boto3 auto-discovers the role).

With access keys (fallback):

```bash
aws configure
# Enter: Access Key ID, Secret Access Key, region: ap-southeast-1, output: json
```

### 3.4 Verify

```bash
scrapy list                    # must print: batdongsan mogi nhatot phongtro123
aws s3 ls s3://vn-rental-dsp/  # must list bronze/ silver/ etc.
```

---

## 4. Frontier Seeding

The sitemap poller discovers listing URLs from each source's sitemap and inserts them
into the SQLite frontier. This is the first step and must complete before any crawl.

```bash
cd /opt/vn-rental-dsp
source .venv/bin/activate
export FRONTIER_DB=/var/data/frontier.db
sudo mkdir -p /var/data && sudo chown ubuntu:ubuntu /var/data

python -m crawler.sitemap_poller --all
```

**Expected output:** 4 sources seeded, total URLs in the hundreds of thousands.

Verify:

```bash
sqlite3 $FRONTIER_DB "SELECT source, COUNT(*) FROM urls GROUP BY source;"
```

Expected: each source has > 10,000 URLs (batdongsan and mogi typically > 100k).

---

## 5. Smoke Test

Before enabling cron, validate each spider individually with a small crawl:

```bash
export FRONTIER_DB=/var/data/frontier.db

# Test each source — 100 pages each
for src in phongtro123 mogi batdongsan nhatot; do
  echo "=== Testing $src ==="
  scrapy crawl "$src" -a db_path=$FRONTIER_DB -s CLOSESPIDER_PAGECOUNT=100
done

# Verify bronze data landed
aws s3 ls s3://vn-rental-dsp/bronze/listings/dt=$(date -u +%F)/ --recursive
```

**Expected:** 4 `source=` prefixes in today's bronze partition. Each part file is a
gzipped JSONL >= 100 KB.

**If a source fails with 403/Cloudflare:** That source's sitemap or listing pages are
blocked. Log the issue, proceed with the working sources, and investigate Cloudflare
bypass (e.g., rotating User-Agent, request headers).

**If nhatot fails with Playwright errors:** Chromium may need additional system
dependencies. Run `playwright install --with-deps chromium` again.

---

## 6. Cron Schedule

All times are UTC. The schedule runs sequentially to stay within the t3.micro's 1 GB
memory and CPU credit budget.

```bash
# /opt/vn-rental-dsp/crontab.txt

# 01:00 UTC — Refresh the frontier from sitemaps (new listings discovered daily)
0 1 * * *  cd /opt/vn-rental-dsp && FRONTIER_DB=/var/data/frontier.db .venv/bin/python -m crawler.sitemap_poller --all >> /opt/vn-rental-dsp/logs/poll.log 2>&1

# 02:00 UTC — Discovery crawl: one pass over all four sources
0 2 * * *  cd /opt/vn-rental-dsp && SCRAPY=.venv/bin/scrapy PYTHON=.venv/bin/python FRONTIER_DB=/var/data/frontier.db ./run_discovery.sh >> /opt/vn-rental-dsp/logs/crawl.log 2>&1

# Hourly — Checkpoint WAL then back up frontier state to S3
0 * * * *  sqlite3 /var/data/frontier.db "PRAGMA wal_checkpoint(FULL);" && aws s3 cp /var/data/frontier.db s3://vn-rental-dsp/state/frontier.db 2>/dev/null

# Weekly — Rotate logs (compress, keep 4 weeks)
0 0 * * 0  for f in /opt/vn-rental-dsp/logs/poll.log /opt/vn-rental-dsp/logs/crawl.log /opt/vn-rental-dsp/logs/panel.log; do [ -f "$f" ] && mv "$f" "$f.$(date +\%F)" && gzip "$f.$(date +\%F)"; done; find /var/log -name '*.log.*.gz' -mtime +28 -delete
```

Install:

```bash
crontab /opt/vn-rental-dsp/crontab.txt
crontab -l   # verify
```

### 6.1 Crawl budget per source

At 1 request/second with `CONCURRENT_REQUESTS_PER_DOMAIN=2` and AutoThrottle, each
source achieves roughly 2,000–3,000 pages/hour. With 4 sources running sequentially
in `run_discovery.sh`, a full pass takes 4–6 hours (assuming 10,000 pages per source
per day as the frontier serves unfetched URLs).

The `CLOSESPIDER_PAGECOUNT` settings:
- batdongsan, phongtro123, mogi: no cap (default 10,000 from `next_batch` limit)
- nhatot: capped at 20,000 pages/run (Playwright is slower)

### 6.2 Panel probing (added after cohort freeze)

Once the panel cohort is frozen (WBS-2.3.1 / WBS-2.3.2), add:

```bash
# 04:00 UTC — Daily panel liveness probes (matches implementation plan Task 12)
0 4 * * *  cd /opt/vn-rental-dsp && FRONTIER_DB=/var/data/frontier.db .venv/bin/python -m crawler.panel_scheduler --run >> /opt/vn-rental-dsp/logs/panel.log 2>&1
```

This probes every URL in the panel cohort and writes results to
`bronze/probes/dt=<date>/`. The panel scheduler uses `classify_probe` from
`crawler/panel_scheduler.py` to detect soft-deletes (HTTP 200 with removal text).

---

## 7. Throughput Validation

After the first full cron run completes, validate throughput:

```bash
# Count pages fetched in the last hour
sqlite3 /var/data/frontier.db \
  "SELECT source, COUNT(*) FROM urls WHERE last_fetch_ts > datetime('now','-1 hour') GROUP BY source;"
```

**Gate:** >= 2,000 pages/hour per source. If below 1,500/hour:

1. Check CPU credit balance: `aws cloudwatch get-metric-statistics --namespace AWS/EC2 --metric-name CPUCreditBalance --dimensions Name=InstanceId,Value=<id> --statistics Average --period 300 --start-time <1h-ago> --end-time now`
2. Check if AutoThrottle backed off: look for `AutoThrottle` messages in `/opt/vn-rental-dsp/logs/crawl.log`
3. If credits exhausted: reduce `CONCURRENT_REQUESTS_PER_DOMAIN` to 1, or accept slower crawling. **t3.small is NOT free-tier eligible** (~$15/month) — only t2.micro and t3.micro qualify

Also verify S3 data volume:

```bash
aws s3 ls s3://vn-rental-dsp/bronze/listings/dt=$(date -u +%F)/ --recursive --summarize | tail -3
```

**Expected:** 4 source prefixes, total size > 50 MB for a full day's crawl.

---

## 8. State Backup and Recovery

### 8.1 What is backed up

| Asset | Location | Backup target | Frequency |
|---|---|---|---|
| `frontier.db` | `/var/data/frontier.db` | `s3://vn-rental-dsp/state/frontier.db` | Hourly |
| Crawl logs | `/opt/vn-rental-dsp/logs/crawl.log` | Not backed up (disposable) | — |

### 8.2 Recovery procedure

If the EC2 instance dies:

```bash
# On a new instance, after software setup (§3):
aws s3 cp s3://vn-rental-dsp/state/frontier.db /var/data/frontier.db
# Resume cron — the frontier has all URL state, crawling picks up where it left off
```

Bronze data is in S3 and unaffected by instance loss. The frontier contains all URL
discovery and fetch state. Recovery time: ~30 minutes.

---

## 9. Monitoring

### 9.1 Daily health check (manual, first week)

```bash
# SSH into the instance
ssh ubuntu@<ec2-ip>

# Check last crawl ran
tail -20 /opt/vn-rental-dsp/logs/crawl.log

# Check today's bronze
aws s3 ls s3://vn-rental-dsp/bronze/listings/dt=$(date -u +%F)/ --recursive --summarize

# Check frontier progress
sqlite3 /var/data/frontier.db "SELECT source, COUNT(*) FROM urls WHERE last_fetch_ts > datetime('now','-24 hours') GROUP BY source;"
```

### 9.2 Automated alerting (optional, after first week is stable)

CloudWatch alarm on the instance's `StatusCheckFailed` metric — emails you if the
instance goes down:

```bash
aws cloudwatch put-metric-alarm \
  --alarm-name crawl-instance-down \
  --namespace AWS/EC2 --metric-name StatusCheckFailed \
  --dimensions Name=InstanceId,Value=<instance-id> \
  --statistic Maximum --period 300 --evaluation-periods 2 \
  --threshold 1 --comparison-operator GreaterThanOrEqualToThreshold \
  --alarm-actions <sns-topic-arn>
```

---

## 10. Constraints and Risks

| Risk | Mitigation |
|---|---|
| t3.micro runs out of CPU credits during long crawls | AutoThrottle reduces load; monitor CPUCreditBalance; reduce concurrency to 1 if needed |
| A site blocks the crawler IP | Contact site operator via email in User-Agent; reduce crawl rate; respect robots.txt. Do NOT rotate User-Agent — the fixed descriptive UA is an ethical requirement |
| Playwright (nhatot) exceeds memory on t3.micro | `CLOSESPIDER_PAGECOUNT=20000` caps nhatot; Playwright runs headless |
| frontier.db grows too large | WAL mode, weekly VACUUM; at 600k URLs the DB is ~50 MB |
| Instance terminated (spot/reboot) | Hourly S3 backup; recovery in 30 minutes |

---

## 11. Sequence Diagram

```
Day 0 (today):
  1. Launch EC2 t3.micro
  2. Install Python, clone repo, pip install
  3. Configure AWS credentials
  4. Seed frontier (sitemap poller --all)
  5. Smoke test (100 pages × 4 sources)
  6. Install cron
  7. Throughput validation (1-hour run)

Day 1:
  - First full cron run completes
  - Verify 4 source prefixes in bronze
  - Manual health check

Day 2-3:
  - Monitor logs, verify daily accumulation
  - Freeze provisional panel cohort (50k listings)
  - Add panel probe cron

Day 7 (end of week):
  - >= 50k listings in bronze
  - Panel probing daily
  - Cohort frozen
```
