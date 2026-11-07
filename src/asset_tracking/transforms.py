"""Bronze -> Silver -> Gold transformations for rental-asset events.

Functions take and return DataFrames, so the same code runs on the stream in
production and on small static DataFrames in the tests.
"""
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from .schemas import ALLOWED_FROM, EVENT_TYPES, STATUS_AFTER_EVENT

WATERMARK = "30 minutes"
MOVEMENT_WINDOW = "15 minutes"


def _map_expr(mapping: dict, col: str):
    """Python dict -> Spark CASE expression."""
    expr = None
    for k, v in mapping.items():
        expr = F.when(F.col(col) == k, F.lit(v)) if expr is None else expr.when(F.col(col) == k, F.lit(v))
    return expr


def to_bronze(raw: DataFrame) -> DataFrame:
    return raw.withColumn("ingested_at", F.current_timestamp()).withColumn("source_file", F.input_file_name())


def to_silver(bronze: DataFrame, streaming: bool = True) -> DataFrame:
    """Type, validate and de-duplicate. Bad rows are flagged, never dropped."""
    df = bronze.withColumn("event_ts", F.to_timestamp("event_time")).withColumn(
        "dq_status",
        F.when(F.col("asset_id").isNull(), "MISSING_ASSET")
        .when(F.col("warehouse_city").isNull(), "MISSING_WAREHOUSE")
        .when(~F.col("event_type").isin(EVENT_TYPES), "UNKNOWN_EVENT")
        .otherwise("OK"),
    )
    if streaming:
        return df.withWatermark("event_ts", WATERMARK).dropDuplicatesWithinWatermark(["event_id"])
    return df.dropDuplicates(["event_id"])


def gold_movements(silver: DataFrame) -> DataFrame:
    """Asset movements per city and event type in 15-minute windows (streaming-safe)."""
    ok = silver.filter(F.col("dq_status") == "OK")
    return (
        ok.groupBy(F.window("event_ts", MOVEMENT_WINDOW).alias("w"), "warehouse_city", "event_type")
        .agg(F.count("*").alias("events"))
        .select(F.col("w.start").alias("window_start"), F.col("w.end").alias("window_end"),
                "warehouse_city", "event_type", "events")
    )


def validate_transitions(silver: DataFrame) -> DataFrame:
    """Flag events that are impossible given the asset's previous status
    (e.g. DISPATCHED while the asset is still ON_RENT)."""
    ok = silver.filter(F.col("dq_status") == "OK")
    w = Window.partitionBy("asset_id").orderBy("event_ts", "event_id")
    with_prev = ok.withColumn("prev_status", F.lag(_map_expr(STATUS_AFTER_EVENT, "event_type")).over(w))

    allowed = None
    for event, sources in ALLOWED_FROM.items():
        cond = F.lit(False)
        for s in sources:
            cond = cond | (F.col("prev_status").isNull() if s is None else F.col("prev_status") == s)
        clause = (F.col("event_type") == event) & cond
        allowed = clause if allowed is None else allowed | clause
    return with_prev.withColumn("valid_transition", allowed)


def gold_asset_status(silver: DataFrame) -> DataFrame:
    """Current status per asset = status after its latest valid event."""
    checked = validate_transitions(silver).filter("valid_transition")
    latest = Window.partitionBy("asset_id").orderBy(F.col("event_ts").desc(), F.col("event_id").desc())
    return (
        checked.withColumn("rn", F.row_number().over(latest)).filter("rn = 1")
        .select("asset_id", "sku", "category", "warehouse_city",
                _map_expr(STATUS_AFTER_EVENT, "event_type").alias("status"),
                F.col("event_ts").alias("status_since"))
    )


def gold_availability(asset_status: DataFrame) -> DataFrame:
    """Rentable stock and utilisation per SKU and city."""
    s = F.col("status")
    return (
        asset_status.filter(s != "WRITTEN_OFF")
        .groupBy("warehouse_city", "category", "sku")
        .agg(
            F.count("*").alias("fleet_size"),
            F.sum(F.when(s == "AVAILABLE", 1).otherwise(0)).alias("available"),
            F.sum(F.when(s == "ON_RENT", 1).otherwise(0)).alias("on_rent"),
            F.sum(F.when(s.isin("PENDING_QC", "IN_REFURB"), 1).otherwise(0)).alias("in_refurb_pipeline"),
        )
        .withColumn("utilisation_pct", F.round(F.col("on_rent") * 100 / F.col("fleet_size"), 1))
        .withColumn("stockout", F.col("available") == 0)
    )
