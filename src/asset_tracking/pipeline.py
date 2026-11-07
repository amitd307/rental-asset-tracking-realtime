"""Structured Streaming job: landing JSON -> Bronze -> Silver -> Gold.

Locally writes Parquet; on Microsoft Fabric pass `--format delta` with
Lakehouse paths (see docs/fabric.md).
"""
from __future__ import annotations

import argparse
import logging

from pyspark.sql import DataFrame, SparkSession

from .schemas import ASSET_EVENT_SCHEMA
from .transforms import (gold_asset_status, gold_availability, gold_movements,
                         to_bronze, to_silver, validate_transitions)

log = logging.getLogger("asset_tracking")


def run(landing: str, lake: str, checkpoint: str, fmt: str = "parquet", once: bool = False,
        trigger_seconds: int = 30) -> None:
    spark = (SparkSession.builder.appName("rental-asset-tracking")
             .config("spark.sql.session.timeZone", "UTC").getOrCreate())
    trigger = {"availableNow": True} if once else {"processingTime": f"{trigger_seconds} seconds"}

    raw = spark.readStream.schema(ASSET_EVENT_SCHEMA).option("maxFilesPerTrigger", 5).json(landing)
    bronze = to_bronze(raw)
    silver = to_silver(bronze)

    def sink(df: DataFrame, name: str, mode: str = "append"):
        return (df.writeStream.format(fmt).outputMode(mode)
                .option("checkpointLocation", f"{checkpoint}/{name}").trigger(**trigger)
                .start(f"{lake}/{name}"))

    queries = [
        sink(bronze, "bronze/asset_events"),
        sink(silver, "silver/asset_events"),
        sink(gold_movements(silver), "gold/asset_movements_15min"),
    ]

    # Asset status needs the full history per asset, so it is recomputed from
    # the Silver table on each micro-batch (cheap at fleet scale; on Fabric this
    # becomes a Delta MERGE on asset_id).
    def refresh_status(batch: DataFrame, batch_id: int) -> None:
        history = spark.read.format(fmt).load(f"{lake}/silver/asset_events")
        status = gold_asset_status(history)
        status.write.mode("overwrite").format(fmt).save(f"{lake}/gold/asset_current_status")
        gold_availability(status).write.mode("overwrite").format(fmt).save(f"{lake}/gold/availability_by_city_sku")
        bad = validate_transitions(history).filter("NOT valid_transition")
        bad.write.mode("overwrite").format(fmt).save(f"{lake}/gold/invalid_transitions")
        log.info("batch %s: status refreshed, %d invalid transitions", batch_id, bad.count())

    queries.append(
        silver.writeStream.foreachBatch(refresh_status)
        .option("checkpointLocation", f"{checkpoint}/gold_status").trigger(**trigger).start()
    )
    for q in queries:
        log.info("started %s", q.id)
    spark.streams.awaitAnyTermination()


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--landing", default="data/landing")
    p.add_argument("--lake", default="data/lake")
    p.add_argument("--checkpoint", default="data/_checkpoints")
    p.add_argument("--format", default="parquet", choices=["parquet", "delta"])
    p.add_argument("--once", action="store_true")
    a = p.parse_args()
    run(a.landing, a.lake, a.checkpoint, a.format, a.once)


if __name__ == "__main__":
    main()
