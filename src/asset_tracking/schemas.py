"""Event schema and the rental-asset lifecycle.

Every physical asset (one specific TV, washing machine, treadmill...) has an
asset_id. Warehouses and logistics systems emit an event each time it moves.
"""
from pyspark.sql.types import StringType, StructField, StructType

ASSET_EVENT_SCHEMA = StructType(
    [
        StructField("event_id", StringType(), False),
        StructField("asset_id", StringType(), True),
        StructField("sku", StringType(), True),
        StructField("category", StringType(), True),
        StructField("warehouse_city", StringType(), True),
        StructField("event_type", StringType(), True),
        StructField("event_time", StringType(), False),  # ISO-8601, cast in Silver
        StructField("rental_order_id", StringType(), True),
    ]
)

# event -> status the asset is in *after* the event
STATUS_AFTER_EVENT = {
    "STOCK_IN": "AVAILABLE",
    "DISPATCHED": "ON_RENT",
    "RETURNED": "PENDING_QC",
    "REFURB_START": "IN_REFURB",
    "REFURB_DONE": "AVAILABLE",
    "WRITE_OFF": "WRITTEN_OFF",
}

# which statuses each event is allowed to happen from (None = first event)
ALLOWED_FROM = {
    "STOCK_IN": {None},
    "DISPATCHED": {"AVAILABLE"},
    "RETURNED": {"ON_RENT"},
    "REFURB_START": {"PENDING_QC"},
    "REFURB_DONE": {"IN_REFURB"},
    "WRITE_OFF": {"PENDING_QC", "IN_REFURB", "AVAILABLE"},
}

EVENT_TYPES = list(STATUS_AFTER_EVENT)
