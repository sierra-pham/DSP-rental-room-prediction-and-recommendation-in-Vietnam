#!/usr/bin/env bash
# One discovery pass over every source in config/sources.yaml, all against the same
# frontier DB. Cron entry (docs/03-implementation-plan.md Task 6 step 7):
#   0 2 * * *  cd /opt/vn-rental-dsp && ./run_discovery.sh >> /var/log/crawl.log 2>&1
# A failing spider does not stop the others; the exit code is non-zero if any failed.
# Overrides for tests / other hosts: SCRAPY, PYTHON, FRONTIER_DB.
set -uo pipefail
cd "$(dirname "$0")"

SCRAPY="${SCRAPY:-scrapy}"
PYTHON="${PYTHON:-python}"
export FRONTIER_DB="${FRONTIER_DB:-frontier.db}"

SOURCES=$("$PYTHON" -c "import config; print(' '.join(config.load('sources')['sources']))")
rc=0
for src in $SOURCES; do
  echo ">> $(date -u +%FT%TZ) crawl $src db=$FRONTIER_DB"
  if ! "$SCRAPY" crawl "$src" -a db_path="$FRONTIER_DB"; then
    echo ">> $(date -u +%FT%TZ) $src FAILED"
    rc=1
  fi
done
echo ">> $(date -u +%FT%TZ) discovery pass done rc=$rc"
exit $rc
