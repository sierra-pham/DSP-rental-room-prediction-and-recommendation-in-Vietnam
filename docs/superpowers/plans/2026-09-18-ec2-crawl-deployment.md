# EC2 Crawl Deployment — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Deploy four Scrapy spiders to an always-on EC2 t3.micro so that bronze data accumulates daily in `s3://vn-rental-dsp/bronze/` without manual intervention.

**Architecture:** Single EC2 t3.micro (free tier, ap-southeast-1) runs sequential daily cron jobs: sitemap poller seeds the SQLite frontier, `run_discovery.sh` crawls all four sources writing gzipped JSONL to S3, and a panel probe spider monitors cohort liveness. The frontier is backed up hourly to S3 so the instance is disposable.

**Tech Stack:** Ubuntu 24.04, Python 3.12, Scrapy 2.19, Playwright (nhatot only), boto3, SQLite WAL, AWS S3, cron.

**Design spec:** `docs/superpowers/specs/2026-09-18-ec2-crawl-deployment-design.md`

---

## File Structure

| File | Action | Purpose |
|---|---|---|
| `infra/crontab.txt` | Create | Cron schedule for EC2: poller, crawl, backup, log rotation |
| `infra/deploy_ec2.sh` | Create | One-shot setup script: apt packages, venv, pip install, directories |
| `infra/iam-ec2-crawl-role.json` | Create | IAM policy for the EC2 instance profile (S3 read/write only) |
| `run_discovery.sh` | No change | Already correct; cron passes `SCRAPY`/`PYTHON`/`FRONTIER_DB` overrides |
| `crawler/sitemap_poller.py` | No change | Already supports `--all` and `--db` |
| `crawler/spiders/*.py` | No change | All four spiders use `async def start()` (Scrapy 2.19) |

---

## Task 1: Create the IAM policy for the EC2 instance profile

The EC2 instance needs S3 access only — no bucket admin, no budgets.
This is a subset of the existing `infra/iam-policy-setup.json`.

**Files:**
- Create: `infra/iam-ec2-crawl-role.json`

- [ ] **Step 1: Write the IAM policy document**

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "CrawlerS3Access",
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

Save to `infra/iam-ec2-crawl-role.json`.

- [ ] **Step 2: Create the IAM role and instance profile in AWS**

```bash
# Create the IAM role
aws iam create-role \
  --role-name vn-rental-crawler \
  --assume-role-policy-document '{
    "Version": "2012-10-17",
    "Statement": [{
      "Effect": "Allow",
      "Principal": {"Service": "ec2.amazonaws.com"},
      "Action": "sts:AssumeRole"
    }]
  }'

# Attach the S3 policy
aws iam put-role-policy \
  --role-name vn-rental-crawler \
  --policy-name CrawlerS3Access \
  --policy-document file://infra/iam-ec2-crawl-role.json

# Create instance profile and attach role
aws iam create-instance-profile --instance-profile-name vn-rental-crawler
aws iam add-role-to-instance-profile \
  --instance-profile-name vn-rental-crawler \
  --role-name vn-rental-crawler
```

- [ ] **Step 3: Verify the role exists**

```bash
aws iam get-instance-profile --instance-profile-name vn-rental-crawler
```

Expected: JSON output showing the role ARN. Wait ~10 seconds after creation before
using it — IAM propagation is eventually consistent.

- [ ] **Step 4: Commit**

```bash
git add infra/iam-ec2-crawl-role.json
git commit -m "IAM policy for EC2 crawler instance profile"
```

---

## Task 2: Launch the EC2 instance

**Files:** None (AWS console / CLI operations only)

- [ ] **Step 1: Create a key pair (if you don't have one)**

```bash
aws ec2 create-key-pair \
  --key-name vn-rental-key \
  --key-type ed25519 \
  --query 'KeyMaterial' \
  --output text > ~/.ssh/vn-rental-key.pem
chmod 400 ~/.ssh/vn-rental-key.pem
```

- [ ] **Step 2: Create the security group**

```bash
# Get default VPC ID
VPC_ID=$(aws ec2 describe-vpcs --filters Name=isDefault,Values=true --query 'Vpcs[0].VpcId' --output text)

aws ec2 create-security-group \
  --group-name vn-rental-crawler-sg \
  --description "SSH only for crawler instance" \
  --vpc-id "$VPC_ID"

# Get your current public IP
MY_IP=$(curl -s https://checkip.amazonaws.com)

# Allow SSH from your IP only
aws ec2 authorize-security-group-ingress \
  --group-name vn-rental-crawler-sg \
  --protocol tcp --port 22 \
  --cidr "${MY_IP}/32"
```

- [ ] **Step 3: Find the Ubuntu 24.04 AMI**

```bash
AMI_ID=$(aws ec2 describe-images \
  --owners 099720109477 \
  --filters "Name=name,Values=ubuntu/images/hvm-ssd-gp3/ubuntu-noble-24.04-amd64-server-*" \
  --query 'sort_by(Images, &CreationDate)[-1].ImageId' \
  --output text)
echo "AMI: $AMI_ID"
```

- [ ] **Step 4: Launch the instance**

```bash
SG_ID=$(aws ec2 describe-security-groups --group-names vn-rental-crawler-sg --query 'SecurityGroups[0].GroupId' --output text)

INSTANCE_ID=$(aws ec2 run-instances \
  --image-id "$AMI_ID" \
  --instance-type t3.micro \
  --key-name vn-rental-key \
  --security-group-ids "$SG_ID" \
  --iam-instance-profile Name=vn-rental-crawler \
  --block-device-mappings '[{"DeviceName":"/dev/sda1","Ebs":{"VolumeSize":20,"VolumeType":"gp3"}}]' \
  --tag-specifications 'ResourceType=instance,Tags=[{Key=Name,Value=vn-rental-crawler}]' \
  --query 'Instances[0].InstanceId' --output text)
echo "Instance: $INSTANCE_ID"
```

- [ ] **Step 5: Wait for the instance to be running and get the public IP**

```bash
aws ec2 wait instance-running --instance-ids "$INSTANCE_ID"
EC2_IP=$(aws ec2 describe-instances --instance-ids "$INSTANCE_ID" \
  --query 'Reservations[0].Instances[0].PublicIpAddress' --output text)
echo "IP: $EC2_IP"
```

- [ ] **Step 6: Verify SSH access**

```bash
ssh -i ~/.ssh/vn-rental-key.pem -o StrictHostKeyChecking=accept-new ubuntu@$EC2_IP "echo ok"
```

Expected: `ok`

If it hangs: the security group is wrong, or the instance is in a private subnet.
Check `aws ec2 describe-instances --instance-ids $INSTANCE_ID` for `SubnetId` and
whether `PublicIpAddress` is present.

---

## Task 3: Create the deploy script and install software on EC2

**Files:**
- Create: `infra/deploy_ec2.sh`

- [ ] **Step 1: Write the deploy script**

Save to `infra/deploy_ec2.sh`:

```bash
#!/usr/bin/env bash
# One-shot setup for the crawl EC2 instance.
# Run on the EC2 instance: bash /opt/vn-rental-dsp/infra/deploy_ec2.sh
set -euo pipefail

export AWS_DEFAULT_REGION=ap-southeast-1

echo ">> Installing system packages"
sudo apt-get update -qq
sudo apt-get install -y -qq python3.12 python3.12-venv python3-pip git sqlite3 awscli

echo ">> Cloning repo (skipped if already present)"
sudo mkdir -p /opt/vn-rental-dsp
sudo chown ubuntu:ubuntu /opt/vn-rental-dsp
if [ ! -d /opt/vn-rental-dsp/.git ]; then
  git clone https://github.com/sierra-pham/DSP-rental-room-prediction-and-recommendation-in-Vietnam.git /opt/vn-rental-dsp
fi

echo ">> Creating venv and installing dependencies"
cd /opt/vn-rental-dsp
python3.12 -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install -e "."

echo ">> Installing Playwright for nhatot spider"
pip install -e ".[js]"
playwright install --with-deps chromium

echo ">> Creating data and log directories"
sudo mkdir -p /var/data
sudo chown ubuntu:ubuntu /var/data
mkdir -p /opt/vn-rental-dsp/logs

echo ">> Verifying installation"
.venv/bin/python -m scrapy list
aws s3 ls s3://vn-rental-dsp/ --max-items 1

echo ">> Done. Next: seed the frontier, smoke test, install cron."
```

- [ ] **Step 2: Commit the deploy script locally**

```bash
git add infra/deploy_ec2.sh
git commit -m "EC2 deploy script: apt, venv, pip, playwright, directories"
git push origin main
```

- [ ] **Step 3: SSH into EC2 and run the deploy script**

```bash
ssh -i ~/.ssh/vn-rental-key.pem ubuntu@$EC2_IP
```

On the EC2 instance:

```bash
sudo mkdir -p /opt/vn-rental-dsp && sudo chown ubuntu:ubuntu /opt/vn-rental-dsp
git clone https://github.com/sierra-pham/DSP-rental-room-prediction-and-recommendation-in-Vietnam.git /opt/vn-rental-dsp
cd /opt/vn-rental-dsp
bash infra/deploy_ec2.sh
```

The deploy script skips `git clone` if `/opt/vn-rental-dsp/.git` already exists.

- [ ] **Step 4: Verify the installation**

On the EC2 instance:

```bash
cd /opt/vn-rental-dsp
source .venv/bin/activate
scrapy list
```

Expected output (exactly 4 lines):

```
batdongsan
mogi
nhatot
phongtro123
```

```bash
aws s3 ls s3://vn-rental-dsp/
```

Expected: lists `bronze/`, `silver/`, `gold/`, etc.

If `scrapy list` fails: check `pip install -e "."` succeeded.
If `aws s3 ls` fails: check the instance profile is attached (`curl -s http://169.254.169.254/latest/meta-data/iam/security-credentials/`).

---

## Task 4: Seed the frontier from all four sitemaps

**Files:** None (runtime operation)

- [ ] **Step 1: Create the data directory and set FRONTIER_DB**

On the EC2 instance:

```bash
sudo mkdir -p /var/data && sudo chown ubuntu:ubuntu /var/data
export FRONTIER_DB=/var/data/frontier.db
```

- [ ] **Step 2: Run the sitemap poller for all sources**

```bash
cd /opt/vn-rental-dsp
source .venv/bin/activate
.venv/bin/python -m crawler.sitemap_poller --all --db $FRONTIER_DB
```

Expected: each source prints a count of new URLs. Total should be in the hundreds of
thousands. This may take 5-15 minutes depending on sitemap sizes and network speed.

- [ ] **Step 3: Verify the frontier was seeded**

```bash
sqlite3 $FRONTIER_DB "SELECT source, COUNT(*) FROM urls GROUP BY source;"
```

Expected output (counts will vary):

```
batdongsan|150000+
mogi|400000+
nhatot|50000+
phongtro123|140000+
```

If any source shows 0: the sitemap URL in `config/sources.yaml` may be wrong, or the
site returned a 403. Check with:

```bash
curl -sI -A "VN-Rental-Research/1.0 (academic project; nguyenpnt4@fpt.com)" \
  "https://batdongsan.com.vn/sitemap.xml"
```

Expected: HTTP 200. If 403: the site blocks automated requests even from EC2.

---

## Task 5: Smoke test each spider (100 pages each)

**Files:** None (runtime operation)

- [ ] **Step 1: Run phongtro123 (simplest, most reliable)**

```bash
cd /opt/vn-rental-dsp
source .venv/bin/activate
export FRONTIER_DB=/var/data/frontier.db

scrapy crawl phongtro123 -a db_path=$FRONTIER_DB -s CLOSESPIDER_PAGECOUNT=100
```

Expected: completes in ~2 minutes, prints stats with `response_received_count: 100+`.

- [ ] **Step 2: Verify phongtro123 data in S3**

```bash
aws s3 ls "s3://vn-rental-dsp/bronze/listings/dt=$(date -u +%F)/source=phongtro123/" --recursive
```

Expected: at least one `part-NNNN.jsonl.gz` file, >= 100 KB.

- [ ] **Step 3: Run mogi**

```bash
scrapy crawl mogi -a db_path=$FRONTIER_DB -s CLOSESPIDER_PAGECOUNT=100
```

Expected: completes in ~2 minutes, `response_received_count: 100+`.

- [ ] **Step 4: Run batdongsan**

```bash
scrapy crawl batdongsan -a db_path=$FRONTIER_DB -s CLOSESPIDER_PAGECOUNT=100
```

Expected: completes in ~2 minutes, `response_received_count: 100+`.

If 403 errors: batdongsan may require additional request headers. Check the logs for
`Filtered offsite request` or `Ignoring response` messages.

- [ ] **Step 5: Run nhatot (Playwright — slowest)**

```bash
scrapy crawl nhatot -a db_path=$FRONTIER_DB -s CLOSESPIDER_PAGECOUNT=50
```

Expected: completes in ~5 minutes (Playwright is slower). Uses 50 pages because
headless Chromium consumes more memory on t3.micro.

If Playwright fails with `BrowserType.launch: Executable doesn't exist`:

```bash
playwright install --with-deps chromium
```

- [ ] **Step 6: Verify all four sources landed in S3**

```bash
aws s3 ls "s3://vn-rental-dsp/bronze/listings/dt=$(date -u +%F)/" --recursive --summarize
```

Expected: 4 `source=` prefixes, total >= 4 files. This is the critical gate — if all
four sources are in S3, the crawl system works end to end.

---

## Task 6: Create and install the cron schedule

**Files:**
- Create: `infra/crontab.txt`

- [ ] **Step 1: Write the crontab file**

Save to `infra/crontab.txt`:

```bash
# VN Rental DSP — crawl cron schedule (all times UTC)
# Install: crontab /opt/vn-rental-dsp/infra/crontab.txt

# 01:00 UTC — Refresh the frontier from sitemaps (new listings discovered daily)
0 1 * * *  cd /opt/vn-rental-dsp && FRONTIER_DB=/var/data/frontier.db .venv/bin/python -m crawler.sitemap_poller --all >> /opt/vn-rental-dsp/logs/poll.log 2>&1

# 02:00 UTC — Discovery crawl: one pass over all four sources
0 2 * * *  cd /opt/vn-rental-dsp && SCRAPY=.venv/bin/scrapy PYTHON=.venv/bin/python FRONTIER_DB=/var/data/frontier.db ./run_discovery.sh >> /opt/vn-rental-dsp/logs/crawl.log 2>&1

# Hourly — Checkpoint WAL then back up frontier state to S3
0 * * * *  sqlite3 /var/data/frontier.db "PRAGMA wal_checkpoint(FULL);" && aws s3 cp /var/data/frontier.db s3://vn-rental-dsp/state/frontier.db 2>/dev/null

# Weekly — Rotate logs (compress, keep 4 weeks)
0 0 * * 0  for f in /opt/vn-rental-dsp/logs/*.log; do [ -f "$f" ] && mv "$f" "$f.$(date +\%F)" && gzip "$f.$(date +\%F)"; done; find /opt/vn-rental-dsp/logs -name '*.log.*.gz' -mtime +28 -delete
```

- [ ] **Step 2: Commit and push**

```bash
git add infra/crontab.txt
git commit -m "Cron schedule for EC2 crawl deployment"
git push origin main
```

- [ ] **Step 3: Pull on EC2 and install the crontab**

On the EC2 instance:

```bash
cd /opt/vn-rental-dsp
git pull origin main
crontab infra/crontab.txt
crontab -l
```

Expected: prints the 4 cron entries.

- [ ] **Step 4: Verify the first cron run**

Wait until the next scheduled run, or trigger manually:

```bash
# Trigger sitemap poller manually
cd /opt/vn-rental-dsp && FRONTIER_DB=/var/data/frontier.db .venv/bin/python -m crawler.sitemap_poller --all

# Trigger discovery crawl manually (will take 4-6 hours for a full pass)
cd /opt/vn-rental-dsp && SCRAPY=.venv/bin/scrapy PYTHON=.venv/bin/python FRONTIER_DB=/var/data/frontier.db ./run_discovery.sh
```

Check the logs:

```bash
tail -20 /opt/vn-rental-dsp/logs/crawl.log
```

Expected: `>> <timestamp> discovery pass done rc=0`

---

## Task 7: Throughput validation

**Files:** None (runtime verification)

- [ ] **Step 1: After the first full crawl pass completes, count pages fetched**

```bash
sqlite3 /var/data/frontier.db \
  "SELECT source, COUNT(*) FROM urls
   WHERE last_fetch_ts > datetime('now','-6 hours')
   GROUP BY source;"
```

Expected: each source shows >= 2,000 pages fetched (varies by source size and
response time). Divide the count by elapsed hours to get pages/hour.

- [ ] **Step 2: Verify S3 data volume**

```bash
aws s3 ls "s3://vn-rental-dsp/bronze/listings/dt=$(date -u +%F)/" --recursive --summarize | tail -3
```

Expected: 4 source prefixes, total size > 50 MB for a full day.

- [ ] **Step 3: Check CPU credit balance**

```bash
INSTANCE_ID=$(curl -s http://169.254.169.254/latest/meta-data/instance-id)
aws cloudwatch get-metric-statistics \
  --namespace AWS/EC2 --metric-name CPUCreditBalance \
  --dimensions Name=InstanceId,Value=$INSTANCE_ID \
  --statistics Average --period 300 \
  --start-time $(date -u -d '1 hour ago' +%FT%TZ) \
  --end-time $(date -u +%FT%TZ)
```

Expected: `Average` > 10. If below 5, crawling is exhausting CPU credits. Reduce
`CONCURRENT_REQUESTS_PER_DOMAIN` to 1 in `crawler/settings.py`.

- [ ] **Step 4: Log the throughput result**

Record in `docs/notes/daily/2026-09-18.md` (or the actual date):

```markdown
- EC2 crawl deployment: throughput validated at X pages/hour per source
  - phongtro123: Xk pages
  - mogi: Xk pages
  - batdongsan: Xk pages
  - nhatot: Xk pages
  - CPU credit balance: X
```

---

## Task 8: Freeze the panel cohort and add the panel probe cron

This task is done 2-3 days after the first crawl, once >= 50k listings have been
crawled. Do not run it on Day 0.

**Prerequisite:** `crawler/panel_scheduler.py` currently implements `classify_probe`
but the `--run` CLI path only logs the panel-due count — it does not yet run a probe
spider or write to S3. Before Step 6 can be verified, the panel probe spider must be
implemented (WBS-2.3.3). If the probe spider is not yet built, install the cron now
(Steps 1–5) and defer Step 6 verification until the spider is complete.

**Files:**
- Modify: `infra/crontab.txt` (add panel probe line)

- [ ] **Step 1: Verify enough listings have been crawled**

```bash
sqlite3 /var/data/frontier.db \
  "SELECT COUNT(*) FROM urls WHERE last_fetch_ts IS NOT NULL;"
```

Expected: >= 50,000. If less, wait another day.

- [ ] **Step 2: Freeze the provisional panel cohort**

```bash
cd /opt/vn-rental-dsp
source .venv/bin/activate
.venv/bin/python -c "
from crawler.frontier import Frontier
f = Frontier('/var/data/frontier.db')
n = f.freeze_panel_cohort(size=50000)
print(f'Cohort frozen: {n} listings')
"
```

Expected: `Cohort frozen: 50000 listings`

- [ ] **Step 3: Verify the cohort is stratified across sources**

```bash
sqlite3 /var/data/frontier.db \
  "SELECT source, COUNT(*) FROM urls WHERE in_panel=1 GROUP BY source;"
```

Expected: all four sources represented, roughly proportional to their crawled volumes.

- [ ] **Step 4: Add the panel probe cron line**

Edit `infra/crontab.txt` and add after the discovery crawl line:

```bash
# 04:00 UTC — Daily panel liveness probes (after discovery crawl finishes)
0 4 * * *  cd /opt/vn-rental-dsp && FRONTIER_DB=/var/data/frontier.db .venv/bin/python -m crawler.panel_scheduler --run >> /opt/vn-rental-dsp/logs/panel.log 2>&1
```

- [ ] **Step 5: Reinstall the crontab on EC2**

```bash
git add infra/crontab.txt
git commit -m "Add panel probe cron after cohort freeze"
git push origin main

# On EC2:
cd /opt/vn-rental-dsp && git pull && crontab infra/crontab.txt
crontab -l
```

Expected: 5 cron entries now listed.

- [ ] **Step 6: Verify probes land in S3 the next day**

```bash
aws s3 ls "s3://vn-rental-dsp/bronze/probes/dt=$(date -u +%F)/" --recursive
```

Expected: probe data files present. Do not proceed to Week 3 until two consecutive
days of probes are confirmed.

---

## Task 9: Set up monitoring and update WBS notes

**Files:**
- Modify: `docs/notes/tasks/WBS-2.2.2.md` — mark done
- Modify: `docs/notes/tasks/WBS-2.2.3.md` — mark done
- Modify: `docs/notes/tasks/WBS-2.4.1.md` — mark done
- Modify: `docs/notes/tasks/WBS-2.4.2.md` — mark done
- Modify: `docs/notes/tasks/WBS-2.3.1.md` — mark done (after cohort freeze)

- [ ] **Step 1: Set up CloudWatch instance status alarm**

```bash
INSTANCE_ID=$(aws ec2 describe-instances \
  --filters "Name=tag:Name,Values=vn-rental-crawler" \
  --query 'Reservations[0].Instances[0].InstanceId' --output text)

aws cloudwatch put-metric-alarm \
  --alarm-name crawl-instance-down \
  --namespace AWS/EC2 --metric-name StatusCheckFailed \
  --dimensions Name=InstanceId,Value=$INSTANCE_ID \
  --statistic Maximum --period 300 --evaluation-periods 2 \
  --threshold 1 --comparison-operator GreaterThanOrEqualToThreshold \
  --actions-enabled
```

Note: to receive email alerts, create an SNS topic and add `--alarm-actions <arn>`.

- [ ] **Step 2: Run the daily health check**

SSH into EC2 and run:

```bash
# Last crawl status
tail -5 /opt/vn-rental-dsp/logs/crawl.log

# Today's bronze
aws s3 ls "s3://vn-rental-dsp/bronze/listings/dt=$(date -u +%F)/" --recursive --summarize

# Pages fetched in last 24h
sqlite3 /var/data/frontier.db \
  "SELECT source, COUNT(*) FROM urls
   WHERE last_fetch_ts > datetime('now','-24 hours')
   GROUP BY source;"

# Frontier backup age
aws s3 ls s3://vn-rental-dsp/state/frontier.db
```

- [ ] **Step 3: Verify the recovery procedure (spec §8.2)**

Test that the instance is truly disposable by restoring the frontier on-box:

```bash
# On EC2: simulate recovery by restoring from S3 backup
mv /var/data/frontier.db /var/data/frontier.db.bak
aws s3 cp s3://vn-rental-dsp/state/frontier.db /var/data/frontier.db

# Verify the restored frontier has data
sqlite3 /var/data/frontier.db "SELECT COUNT(*) FROM urls WHERE last_fetch_ts IS NOT NULL;"

# Verify a spider can still crawl with the restored frontier
cd /opt/vn-rental-dsp && source .venv/bin/activate
scrapy crawl phongtro123 -a db_path=/var/data/frontier.db -s CLOSESPIDER_PAGECOUNT=10

# Restore the original
mv /var/data/frontier.db.bak /var/data/frontier.db
```

Expected: restored frontier has the same URL count, spider completes 10 pages.

- [ ] **Step 4: Update WBS task notes**

Update each task's YAML front matter from `status: todo` or `status: in-progress`
to `status: done`, and add a log entry with the date and what was measured:

- `WBS-2.2.2.md` (Spider — batdongsan): done, smoke test passed, X pages
- `WBS-2.2.3.md` (Spiders — 3 remaining): done, all 4 live on EC2
- `WBS-2.4.1.md` (EC2 deploy & cron): done, crontab installed
- `WBS-2.4.2.md` (Throughput validation): done, X pages/hour measured
- `WBS-2.3.1.md` (Provisional cohort freeze): done, 50k frozen

- [ ] **Step 5: Commit**

```bash
git add docs/notes/tasks/WBS-2.2.2.md docs/notes/tasks/WBS-2.2.3.md \
        docs/notes/tasks/WBS-2.4.1.md docs/notes/tasks/WBS-2.4.2.md \
        docs/notes/tasks/WBS-2.3.1.md
git commit -m "EC2 crawl deployment complete: all 4 spiders live, throughput validated"
git push origin main
```

---

## Summary: Day-by-day execution

| Day | Tasks | Gate |
|---|---|---|
| Day 0 | Tasks 1–6: EC2 up, software installed, frontier seeded, smoke test passed, cron installed | All 4 sources in S3 bronze |
| Day 1 | Task 7: First full cron run, throughput validation | >= 2,000 pages/hour/source |
| Day 2-3 | Task 8: Cohort freeze + panel probe cron (when >= 50k crawled) | Panel probes landing in S3 |
| Day 3+ | Task 9: Monitoring, WBS updates | Two consecutive days of probes confirmed |
