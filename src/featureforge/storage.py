from __future__ import annotations

from pathlib import Path

import pandas as pd

from featureforge.models import SyntheticDataset


def write_synthetic_dataset(
    dataset: SyntheticDataset,
    output_dir: str | Path,
) -> dict[str, Path]:
    destination = Path(output_dir)
    destination.mkdir(parents=True, exist_ok=True)

    tables = {
        "users": [user.model_dump() for user in dataset.users],
        "content": [item.model_dump() for item in dataset.content_items],
        "events": [event.model_dump() for event in dataset.events],
        "labels": [label.model_dump() for label in dataset.labels],
    }

    output_paths: dict[str, Path] = {}

    for table_name, records in tables.items():
        output_path = destination / f"{table_name}.parquet"
        pd.DataFrame(records).to_parquet(output_path, index=False)
        output_paths[table_name] = output_path

    return output_paths
