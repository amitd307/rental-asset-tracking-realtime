# Running on Microsoft Fabric

| Local | Microsoft Fabric |
|---|---|
| `data/landing/` | Lakehouse `Files/landing/`, or an **Eventstream** routing scanner events into the Lakehouse |
| Parquet under `data/lake/` | **Delta** tables under `Tables/` (`--format delta`) |
| `--once` run | Notebook scheduled by a Data Pipeline every few minutes, or a long-running Spark Job Definition |

```python
from asset_tracking.pipeline import run
run(landing="Files/landing", lake="Tables", checkpoint="Files/_checkpoints", fmt="delta", once=True)
```

## Production notes

- **Current status at scale:** replace the full recompute in `refresh_status` with a Delta `MERGE INTO asset_current_status USING latest_events ON asset_id`, so only assets that moved in the batch are updated.
- **Alerts:** a Fabric **Activator** (Real-Time Intelligence) rule on `availability_by_city_sku.stockout = true` can notify city ops on Teams.
- **Reporting:** Gold Delta tables are read by a **Direct Lake** semantic model, so the ops dashboard needs no import refresh.
