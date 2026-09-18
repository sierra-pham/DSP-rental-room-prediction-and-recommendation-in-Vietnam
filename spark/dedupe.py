"""Near-duplicate detection via MinHash LSH on character 3-grams.

Re-posted listings corrupt the panel: a re-post looks like a new listing and
the original looks like it rented. This job clusters near-duplicates so
downstream survival labels track the *cluster*, not the individual post.

Pipeline:
    1. Blocking key (province, district, rounded area/rent) — prunes pairs
    2. Character 3-gram shingling of description_clean
    3. HashingTF → MinHashLSH (5 hash tables)
    4. approxSimilarityJoin (Jaccard distance ≤ 0.20 = similarity ≥ 0.80)
    5. Connected components via union-find on the driver
    6. canonical_id = min(listing_id) per component

Usage:
    spark-submit spark/dedupe.py --date 2026-09-20
"""

import argparse
import logging

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, StringType

from pyspark.ml.feature import HashingTF, MinHashLSH

_log = logging.getLogger(__name__)

JACCARD_DISTANCE_THRESHOLD = 0.20
NUM_HASH_TABLES = 5
SHINGLE_SIZE = 3
HASHING_FEATURES = 2**14


def _char_shingles(text: str, n: int = SHINGLE_SIZE) -> list[str]:
    """Overlapping character n-grams from a string."""
    if not text or len(text) < n:
        return []
    return [text[i:i + n] for i in range(len(text) - n + 1)]


def _union_find(edges: list[tuple[str, str]], all_ids: list[str]) -> dict[str, str]:
    """Connected components via union-find.

    The number of edges is O(duplicates), not O(listings), so collecting
    them to the driver is safe at this scale (~30k pairs for 200k listings).
    """
    parent: dict[str, str] = {id_: id_ for id_ in all_ids}

    def find(x: str) -> str:
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x

    def union(a: str, b: str) -> None:
        ra, rb = find(a), find(b)
        if ra != rb:
            if ra < rb:
                parent[rb] = ra
            else:
                parent[ra] = rb

    for a, b in edges:
        if a in parent and b in parent:
            union(a, b)

    return {id_: find(id_) for id_ in all_ids}


def build_clusters(df: DataFrame) -> DataFrame:
    """Cluster near-duplicate listings by description similarity.

    Input must have at minimum: listing_id, province, district, area_sqm,
    asking_rent_vnd, description_clean.

    Returns a DataFrame with (listing_id, canonical_id, cluster_size,
    is_canonical) — one row per listing in the input.
    """
    spark = df.sparkSession

    all_ids = df.select("listing_id").distinct()

    blocked = df.withColumn(
        "_block_key",
        F.concat_ws(
            "|",
            F.coalesce(F.col("province"), F.lit("")),
            F.coalesce(F.col("district"), F.lit("")),
            F.round(F.col("area_sqm"), 0).cast("string"),
            F.round(F.col("asking_rent_vnd"), -5).cast("string"),
        ),
    )

    shingle_udf = F.udf(_char_shingles, ArrayType(StringType()))
    shingled = blocked.withColumn(
        "_shingles",
        shingle_udf(F.coalesce(F.col("description_clean"), F.lit(""))),
    ).filter(F.size("_shingles") > 0)

    ht = HashingTF(
        numFeatures=HASHING_FEATURES, inputCol="_shingles", outputCol="_features"
    )
    featured = ht.transform(shingled)

    mh = MinHashLSH(
        numHashTables=NUM_HASH_TABLES, inputCol="_features", outputCol="_hashes"
    )
    model = mh.fit(featured)

    pairs_raw = model.approxSimilarityJoin(
        featured, featured, JACCARD_DISTANCE_THRESHOLD, distCol="_jdist"
    )

    pairs = pairs_raw.filter(
        (F.col("datasetA.listing_id") < F.col("datasetB.listing_id"))
        & (F.col("datasetA._block_key") == F.col("datasetB._block_key"))
    ).select(
        F.col("datasetA.listing_id").alias("id_a"),
        F.col("datasetB.listing_id").alias("id_b"),
    )

    edge_list = [(r["id_a"], r["id_b"]) for r in pairs.collect()]
    id_list = [r["listing_id"] for r in all_ids.collect()]

    _log.info(
        "dedupe: %d listings, %d edges from LSH join", len(id_list), len(edge_list)
    )

    components = _union_find(edge_list, id_list)

    comp_df = spark.createDataFrame(
        [(lid, cid) for lid, cid in components.items()],
        ["listing_id", "canonical_id"],
    )

    sizes = (
        comp_df.groupBy("canonical_id")
        .count()
        .withColumnRenamed("count", "cluster_size")
    )

    result = (
        comp_df.join(sizes, "canonical_id")
        .withColumn("is_canonical", F.col("listing_id") == F.col("canonical_id"))
        .select("listing_id", "canonical_id", "cluster_size", "is_canonical")
    )

    dup_count = result.filter(F.col("cluster_size") > 1).count()
    total = result.count()
    rate = dup_count / total if total > 0 else 0.0
    _log.info("duplicate rate: %d/%d (%.1f%%)", dup_count, total, 100 * rate)

    return result


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s %(message)s")

    ap = argparse.ArgumentParser(description="Deduplicate silver listings.")
    ap.add_argument("--date", required=True, help="silver partition date, YYYY-MM-DD")
    ap.add_argument("--input", default=None, help="override the silver input path")
    ap.add_argument("--output", default=None, help="override the listing_dim output path")
    args = ap.parse_args()

    from spark.common import get_spark, s3_path

    spark = get_spark("dedupe")
    input_path = args.input or s3_path("silver", "listings")
    output_path = args.output or s3_path("gold", "listing_dim")

    silver = spark.read.parquet(input_path)
    clusters = build_clusters(silver)

    (clusters
     .repartition(1)
     .write
     .mode("overwrite")
     .parquet(output_path))
    _log.info("wrote %s", output_path)
    spark.stop()
