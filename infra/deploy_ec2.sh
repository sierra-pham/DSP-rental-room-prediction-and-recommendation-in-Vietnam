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
