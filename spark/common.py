"""Session builder and S3 path helpers shared by the Spark jobs."""

from pyspark.sql import SparkSession

import config


def get_spark(app_name: str) -> SparkSession:
    """One session per job, sized from config/aws.yaml.

    `partitionOverwriteMode=dynamic` is what makes the jobs idempotent: a
    write in `overwrite` mode then replaces only the partitions the job
    actually produced, so re-running `--date X` leaves every other day alone.
    """
    cfg = config.load("aws")
    shuffle_partitions = cfg["spark"]["shuffle_partitions"]
    return (SparkSession.builder
            .appName(app_name)
            .config("spark.sql.shuffle.partitions", str(shuffle_partitions))
            .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
            .getOrCreate())


def s3_path(layer: str, *parts: str) -> str:
    """s3a:// URI for a layer, e.g. s3_path("bronze", "listings", "dt=2026-09-20")."""
    cfg = config.load("aws")
    bucket = cfg["bucket"]
    suffix = "/".join(parts)
    return f"s3a://{bucket}/{layer}/{suffix}" if suffix else f"s3a://{bucket}/{layer}"
