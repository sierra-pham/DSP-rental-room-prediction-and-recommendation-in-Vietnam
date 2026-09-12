#!/usr/bin/env bash
# Create the vn-rental-dsp bucket with the medallion prefix layout, block all public
# access, and add the bronze -> Intelligent-Tiering lifecycle rule.
# Idempotent: safe to re-run. Requires: aws CLI v2 with a configured profile.
set -euo pipefail
cd "$(dirname "$0")/.."

# Bucket and region come from config/aws.yaml — never hardcoded (Global Constraints).
BUCKET=$(python -c "import config; print(config.load('aws')['bucket'])")
REGION=$(python -c "import config; print(config.load('aws')['region'])")
echo ">> bucket=$BUCKET region=$REGION"

if aws s3api head-bucket --bucket "$BUCKET" 2>/dev/null; then
  echo ">> bucket already exists, skipping create"
else
  aws s3api create-bucket --bucket "$BUCKET" --region "$REGION" \
    --create-bucket-configuration LocationConstraint="$REGION"
  echo ">> bucket created"
fi

aws s3api put-public-access-block --bucket "$BUCKET" \
  --public-access-block-configuration \
  "BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true"
echo ">> public access blocked"

for p in bronze silver gold images models; do
  aws s3api put-object --bucket "$BUCKET" --key "$p/" >/dev/null
done
echo ">> prefixes: bronze/ silver/ gold/ images/ models/"

# Bronze is written once and read rarely after parsing; tier it down after 30 days.
aws s3api put-bucket-lifecycle-configuration --bucket "$BUCKET" \
  --lifecycle-configuration '{
    "Rules": [{
      "ID": "bronze-to-intelligent-tiering",
      "Status": "Enabled",
      "Filter": {"Prefix": "bronze/"},
      "Transitions": [{"Days": 30, "StorageClass": "INTELLIGENT_TIERING"}]
    }]
  }'
echo ">> lifecycle rule applied"

# Request metrics must be enabled for AWS/S3 BytesDownloaded to emit at all;
# without this the egress alarm in budgets.sh stays in INSUFFICIENT_DATA forever.
aws s3api put-bucket-metrics-configuration --bucket "$BUCKET" --id EntireBucket \
  --metrics-configuration '{"Id": "EntireBucket"}'
echo ">> request metrics enabled (filter id: EntireBucket)"

echo ">> done. verify with: aws s3 ls s3://$BUCKET/"
