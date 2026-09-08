"""Loads config/datasets.yaml into small typed structures.

Keeping this as plain dataclasses (rather than passing raw dicts around)
means every stage of the generic ingestion pipeline (loader, standardizer,
quality checks, writer) agrees on what fields a dataset config has.
"""
import importlib
import os
from dataclasses import dataclass, field
from typing import Callable, Dict, List, Optional

import yaml


@dataclass
class NumericRule:
    column: str
    min: Optional[float] = None
    max: Optional[float] = None


@dataclass
class DatasetConfig:
    name: str
    dataset_type: str  # "fact" | "lookup"
    input_format: str  # "csv" | "parquet"
    input_path: str
    csv_options: Dict[str, str]
    expected_raw_columns: List[str]
    transform_fn_path: str
    primary_key: List[str]
    timestamp_cols: List[str]
    numeric_rules: List[NumericRule]
    partition_by: List[str]
    derive_partition_cols_from: Optional[str]

    def resolve_transform_fn(self) -> Callable:
        module_path, fn_name = self.transform_fn_path.rsplit(".", 1)
        module = importlib.import_module(module_path)
        return getattr(module, fn_name)


@dataclass
class PlatformConfig:
    raw_data_dir: str
    bronze_dir: str
    gold_dir: str
    metadata_dir: str
    schema_version: int
    datasets: List[DatasetConfig] = field(default_factory=list)

    def get_dataset(self, name: str) -> DatasetConfig:
        for ds in self.datasets:
            if ds.name == name:
                return ds
        raise KeyError(f"No dataset config named '{name}'")


def load_config(config_path: str) -> PlatformConfig:
    with open(config_path, "r", encoding="utf-8") as f:
        raw = yaml.safe_load(f)

    project_root = os.path.dirname(os.path.dirname(os.path.abspath(config_path)))
    platform_raw = raw["platform"]

    datasets = []
    for d in raw["datasets"]:
        numeric_rules = [NumericRule(**r) for r in d.get("numeric_rules", [])]
        datasets.append(
            DatasetConfig(
                name=d["name"],
                dataset_type=d["dataset_type"],
                input_format=d["input_format"],
                input_path=d["input_path"],
                csv_options=d.get("csv_options", {}) or {},
                expected_raw_columns=d.get("expected_raw_columns", []),
                transform_fn_path=d["transform_fn"],
                primary_key=d.get("primary_key", []),
                timestamp_cols=d.get("timestamp_cols", []),
                numeric_rules=numeric_rules,
                partition_by=d.get("partition_by", []),
                derive_partition_cols_from=d.get("derive_partition_cols_from"),
            )
        )

    return PlatformConfig(
        raw_data_dir=os.path.join(project_root, platform_raw["raw_data_dir"]),
        bronze_dir=os.path.join(project_root, platform_raw["bronze_dir"]),
        gold_dir=os.path.join(project_root, platform_raw["gold_dir"]),
        metadata_dir=os.path.join(project_root, platform_raw["metadata_dir"]),
        schema_version=platform_raw["schema_version"],
        datasets=datasets,
    )