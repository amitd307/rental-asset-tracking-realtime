"""Synthetic rental-asset event generator (pure Python, no Spark needed).

Simulates assets moving through their lifecycle across city warehouses and
writes newline-delimited JSON files into a landing folder. A small share of
events is deliberately dirty: duplicate deliveries, missing asset ids and
out-of-order status changes, so the Silver layer has something to catch.
"""
from __future__ import annotations

import argparse
import json
import random
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

CITIES = ["Bengaluru", "Mumbai", "Delhi", "Hyderabad", "Pune", "Chennai"]
CATALOG = {
    "Electronics": ["SKU-TV55", "SKU-TV43", "SKU-LAPTOP14", "SKU-SOUNDBAR"],
    "Appliances": ["SKU-FRIDGE260", "SKU-WM7KG", "SKU-AC15T", "SKU-PURIFIER"],
    "Fitness": ["SKU-TREADMILL", "SKU-BIKE"],
}
SKU_CATEGORY = {sku: cat for cat, skus in CATALOG.items() for sku in skus}

# normal next step for each status
NEXT = {
    None: "STOCK_IN",
    "AVAILABLE": "DISPATCHED",
    "ON_RENT": "RETURNED",
    "PENDING_QC": "REFURB_START",
    "IN_REFURB": "REFURB_DONE",
}
AFTER = {"STOCK_IN": "AVAILABLE", "DISPATCHED": "ON_RENT", "RETURNED": "PENDING_QC",
         "REFURB_START": "IN_REFURB", "REFURB_DONE": "AVAILABLE", "WRITE_OFF": "WRITTEN_OFF"}


class Fleet:
    def __init__(self, n_assets: int, rng: random.Random, start: datetime | None = None):
        self.rng = rng
        # event time only moves forward; files may still arrive out of order
        self.clock = start or datetime(2026, 1, 1, 8, 0, tzinfo=timezone.utc)
        self.assets = {}
        for i in range(1, n_assets + 1):
            sku = rng.choice(list(SKU_CATEGORY))
            self.assets[f"AST-{i:06d}"] = {"sku": sku, "city": rng.choice(CITIES), "status": None}

    def next_event(self) -> dict | None:
        self.clock += timedelta(seconds=self.rng.randint(1, 90))
        asset_id = self.rng.choice(list(self.assets))
        a = self.assets[asset_id]
        if a["status"] == "WRITTEN_OFF":
            return None
        if a["status"] == "PENDING_QC" and self.rng.random() < 0.05:
            etype = "WRITE_OFF"
        else:
            etype = NEXT[a["status"]]
        a["status"] = AFTER[etype]
        return {
            "event_id": str(uuid.UUID(int=self.rng.getrandbits(128))),
            "asset_id": asset_id,
            "sku": a["sku"],
            "category": SKU_CATEGORY[a["sku"]],
            "warehouse_city": a["city"],
            "event_type": etype,
            "event_time": self.clock.isoformat(),
            "rental_order_id": f"RO{self.rng.randint(1, 99999):06d}" if etype == "DISPATCHED" else None,
        }


def generate_batch(fleet: Fleet, n: int) -> list[dict]:
    rng = fleet.rng
    events = [e for e in (fleet.next_event() for _ in range(n)) if e]
    dirty = []
    for e in rng.sample(events, k=max(1, len(events) // 50)):
        later = (datetime.fromisoformat(e["event_time"]) + timedelta(seconds=1)).isoformat()
        bad = dict(e, event_id=str(uuid.UUID(int=rng.getrandbits(128))), event_time=later)
        if rng.random() < 0.5:
            bad["asset_id"] = None                   # missing key from a scanner glitch
        else:
            bad["event_type"] = "DISPATCHED"         # dispatched twice / out of order
        dirty.append(bad)
    events += dirty + rng.sample(events, k=max(1, len(events) // 30))  # + duplicate deliveries
    rng.shuffle(events)
    return events


def write_batch(events: list[dict], out_dir: Path, seq: int) -> Path:
    out_dir.mkdir(parents=True, exist_ok=True)
    tmp, final = out_dir / f".events_{seq:05d}.tmp", out_dir / f"events_{seq:05d}.json"
    tmp.write_text("\n".join(json.dumps(e) for e in events), encoding="utf-8")
    tmp.replace(final)  # atomic rename: Spark never sees a half-written file
    return final


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--out", default="data/landing")
    p.add_argument("--assets", type=int, default=500)
    p.add_argument("--files", type=int, default=10)
    p.add_argument("--events-per-file", type=int, default=300)
    p.add_argument("--seed", type=int, default=42)
    a = p.parse_args()
    fleet = Fleet(a.assets, random.Random(a.seed))
    for seq in range(a.files):
        print("wrote", write_batch(generate_batch(fleet, a.events_per_file), Path(a.out), seq))


if __name__ == "__main__":
    main()
