# AWS Setup Log

Record of the AWS account configuration for the project. Report 2 (storage design)
and Report 4 (cost evidence) both cite this. Fill in each field when the step is done.

| Item | Value |
|---|---|
| Account ID | _pending_ |
| Region | `ap-southeast-1` (Singapore) |
| IAM principal used | _pending_ (user/role name; least-privilege: S3 on `vn-rental-dsp` only, plus Budgets/CloudWatch for setup) |
| Bucket | `s3://vn-rental-dsp/` |
| Bucket created | _pending_ (date, via `infra/setup_s3.sh`) |
| Public access block | all four flags `true` |
| Prefixes | `bronze/ silver/ gold/ images/ models/` |
| Lifecycle | `bronze/` → Intelligent-Tiering after 30 days |
| Request metrics | filter `EntireBucket` (feeds the egress alarm) |
| Budget | `vn-rental-dsp-monthly`, $50/month, alerts at $10 / $25 / $50 → nguyenpnt4@fpt.com |
| Budget created | _pending_ (date, via `infra/budgets.sh`) |
| Egress alarm | `s3-egress-high`: BytesDownloaded > 80 GB/day |
| Verified from Python | _pending_ (`pytest tests/test_aws.py` PASS, date) |

## Notes

- Spark compute runs on Oracle Cloud Always Free, so the only AWS costs are S3 storage
  (~$20/month at ~350 GB), requests, and cross-cloud egress above 100 GB/month.
- Cluster VMs access S3 via an IAM user access key scoped to this bucket (see
  `02-technical-design-spec.md` §9.2). Rotate it at project end.
