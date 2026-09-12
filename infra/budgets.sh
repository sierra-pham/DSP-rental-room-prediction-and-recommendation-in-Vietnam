#!/usr/bin/env bash
# Cost guardrails: a $50 monthly cost budget with alerts at 20% / 50% / 100%
# ($10 / $25 / $50) emailed to the project owner, plus a CloudWatch alarm on S3 egress
# at 80 GB/day (the 100 GB/month free allowance is the real cost cliff, because the
# Spark cluster reads S3 from Oracle Cloud, i.e. from outside AWS).
# Idempotent for the alarm; the budget create fails harmlessly if it already exists.
set -euo pipefail
cd "$(dirname "$0")/.."

BUCKET=$(python -c "import config; print(config.load('aws')['bucket'])")
EMAIL="${BUDGET_EMAIL:-nguyenpnt4@fpt.com}"
ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
echo ">> account=$ACCOUNT_ID email=$EMAIL"

notification() {  # $1 = threshold percent
  cat <<JSON
{"Notification": {"NotificationType": "ACTUAL", "ComparisonOperator": "GREATER_THAN",
  "Threshold": $1, "ThresholdType": "PERCENTAGE"},
 "Subscribers": [{"SubscriptionType": "EMAIL", "Address": "$EMAIL"}]}
JSON
}

if aws budgets describe-budget --account-id "$ACCOUNT_ID" --budget-name vn-rental-dsp-monthly >/dev/null 2>&1; then
  echo ">> budget already exists, skipping"
else
  aws budgets create-budget --account-id "$ACCOUNT_ID" \
    --budget '{"BudgetName": "vn-rental-dsp-monthly", "BudgetType": "COST",
               "TimeUnit": "MONTHLY", "BudgetLimit": {"Amount": "50", "Unit": "USD"}}' \
    --notifications-with-subscribers "[$(notification 20), $(notification 50), $(notification 100)]"
  echo ">> budget created: \$50/month, alerts at \$10 / \$25 / \$50"
fi

# 80 GB in bytes = 85899345920. Dimensions are required or the alarm never receives data.
aws cloudwatch put-metric-alarm \
  --alarm-name s3-egress-high \
  --alarm-description "S3 BytesDownloaded > 80 GB in a day (100 GB/month free egress)" \
  --namespace AWS/S3 --metric-name BytesDownloaded \
  --dimensions Name=BucketName,Value="$BUCKET" Name=FilterId,Value=EntireBucket \
  --statistic Sum --period 86400 --evaluation-periods 1 \
  --threshold 85899345920 --comparison-operator GreaterThanThreshold \
  --treat-missing-data notBreaching
echo ">> alarm s3-egress-high set"
echo ">> done. Budgets console: https://console.aws.amazon.com/billing/home#/budgets"
