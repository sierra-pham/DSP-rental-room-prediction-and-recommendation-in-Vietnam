"""Build the panel fact table and compute survival labels.

The panel is the core analytical asset: one row per (listing, day) with a
liveness flag. Survival labels are derived from the panel using the
two-consecutive-absences rule from spec 7.3:

    gone_date   = first date where both the row and the next row are absent
    first_seen  = min(obs_date) where is_present
    duration    = gone_date - first_seen  (or censor_date - first_seen)
    event       = gone_date is not null

A single absence followed by a return is treated as a transient failure
(probe timeout, CDN glitch), not as a rental event.

Usage:
    spark-submit spark/build_panel.py --censor-date 2026-10-12
"""

import argparse
import datetime
import logging

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

_log = logging.getLogger(__name__)


def survival_labels(
    df: DataFrame, censor_date: datetime.date
) -> DataFrame:
    """Compute survival labels from a panel of (listing_id, obs_date, is_present).

    Returns a DataFrame with:
        listing_id, first_seen, duration_days, event_observed, left_truncated
    """
    w = Window.partitionBy("listing_id").orderBy("obs_date")

    labeled = df.withColumn("next_present", F.lead("is_present", 1).over(w))

    gone_candidates = labeled.filter(
        (~F.col("is_present")) & (F.col("next_present") == False)  # noqa: E712
    )

    gone_dates = (
        gone_candidates.groupBy("listing_id")
        .agg(F.min("obs_date").alias("gone_date"))
    )

    first_seen = (
        df.filter(F.col("is_present"))
        .groupBy("listing_id")
        .agg(F.min("obs_date").alias("first_seen"))
    )

    censor_lit = F.lit(censor_date)

    result = (
        first_seen
        .join(gone_dates, "listing_id", "left")
        .withColumn(
            "event_observed",
            F.col("gone_date").isNotNull(),
        )
        .withColumn(
            "duration_days",
            F.when(F.col("event_observed"), F.datediff("gone_date", "first_seen"))
            .otherwise(F.datediff(censor_lit, "first_seen")),
        )
        .withColumn("left_truncated", F.lit(True))
        .select(
            "listing_id", "first_seen", "duration_days",
            "event_observed", "left_truncated",
        )
    )

    return result


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s %(message)s")

    ap = argparse.ArgumentParser(description="Build panel fact table and survival labels.")
    ap.add_argument("--censor-date", required=True,
                    help="right-censoring date, YYYY-MM-DD")
    ap.add_argument("--input", default=None, help="override the probes input path")
    ap.add_argument("--output", default=None, help="override the panel_fact output path")
    ap.add_argument("--output-survival", default=None,
                    help="override the features_survival output path")
    args = ap.parse_args()

    from spark.common import get_spark, s3_path

    spark = get_spark("build_panel")
    input_path = args.input or s3_path("bronze", "probes")
    output_path = args.output or s3_path("gold", "panel_fact")
    survival_path = args.output_survival or s3_path("gold", "features_survival")

    censor = datetime.date.fromisoformat(args.censor_date)

    probes = spark.read.parquet(input_path)

    panel = probes.select("listing_id", "obs_date", "is_present")

    labels = survival_labels(panel, censor_date=censor)

    (panel
     .repartition(1)
     .write
     .partitionBy("listing_id")
     .mode("overwrite")
     .parquet(output_path))

    (labels
     .repartition(1)
     .write
     .mode("overwrite")
     .parquet(survival_path))

    total = labels.count()
    events = labels.filter(F.col("event_observed")).count()
    _log.info(
        "survival: %d listings, %d events (%.1f%% uncensored)",
        total, events, 100 * events / total if total else 0,
    )
    spark.stop()
