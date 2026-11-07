import random

from asset_tracking.generator import Fleet, generate_batch
from asset_tracking.schemas import ASSET_EVENT_SCHEMA
from asset_tracking.transforms import (gold_asset_status, gold_availability, gold_movements,
                                       to_silver, validate_transitions)


def ev(eid, asset, etype, ts, city="Bengaluru", sku="SKU-TV55", cat="Electronics"):
    return (eid, asset, sku, cat, city, etype, ts, None)


def silver(spark, rows):
    return to_silver(spark.createDataFrame(rows, ASSET_EVENT_SCHEMA), streaming=False)


def test_silver_flags_and_dedupes(spark):
    s = silver(spark, [
        ev("1", "A1", "STOCK_IN", "2026-01-01T09:00:00"),
        ev("1", "A1", "STOCK_IN", "2026-01-01T09:00:00"),   # duplicate delivery
        ev("2", None, "DISPATCHED", "2026-01-01T09:05:00"),
        ev("3", "A2", "TELEPORTED", "2026-01-01T09:06:00"),
    ])
    status = {r.event_id: r.dq_status for r in s.collect()}
    assert status == {"1": "OK", "2": "MISSING_ASSET", "3": "UNKNOWN_EVENT"}


def test_lifecycle_and_invalid_transition(spark):
    s = silver(spark, [
        ev("1", "A1", "STOCK_IN", "2026-01-01T09:00:00"),
        ev("2", "A1", "DISPATCHED", "2026-01-01T10:00:00"),
        ev("3", "A1", "DISPATCHED", "2026-01-01T11:00:00"),  # already on rent -> invalid
        ev("4", "A2", "STOCK_IN", "2026-01-01T09:00:00"),
    ])
    checks = {r.event_id: r.valid_transition for r in validate_transitions(s).collect()}
    assert checks == {"1": True, "2": True, "3": False, "4": True}

    status = {r.asset_id: r.status for r in gold_asset_status(s).collect()}
    assert status == {"A1": "ON_RENT", "A2": "AVAILABLE"}


def test_availability_and_utilisation(spark):
    rows = []
    for i in range(4):
        rows.append(ev(f"s{i}", f"A{i}", "STOCK_IN", "2026-01-01T09:00:00"))
    rows += [ev("d0", "A0", "DISPATCHED", "2026-01-01T10:00:00"),
             ev("d1", "A1", "DISPATCHED", "2026-01-01T10:00:00"),
             ev("r1", "A1", "RETURNED", "2026-01-02T10:00:00")]
    avail = gold_availability(gold_asset_status(silver(spark, rows))).collect()[0]
    assert (avail.fleet_size, avail.available, avail.on_rent, avail.in_refurb_pipeline) == (4, 2, 1, 1)
    assert avail.utilisation_pct == 25.0 and avail.stockout is False


def test_movements_window(spark):
    s = silver(spark, [
        ev("1", "A1", "STOCK_IN", "2026-01-01T09:01:00"),
        ev("2", "A2", "STOCK_IN", "2026-01-01T09:14:00"),
        ev("3", "A3", "STOCK_IN", "2026-01-01T09:16:00"),
    ])
    counts = sorted(r.events for r in gold_movements(s).collect())
    assert counts == [1, 2]


def test_generator_produces_valid_lifecycles(spark):
    fleet = Fleet(50, random.Random(3))
    events = []
    for _ in range(5):
        events += generate_batch(fleet, 100)
    df = spark.createDataFrame([tuple(e[f.name] for f in ASSET_EVENT_SCHEMA.fields) for e in events],
                               ASSET_EVENT_SCHEMA)
    s = to_silver(df, streaming=False)
    assert s.filter("dq_status = 'MISSING_ASSET'").count() > 0
    invalid = validate_transitions(s).filter("NOT valid_transition").count()
    assert 0 < invalid < s.count() * 0.1   # only the planted bad events
