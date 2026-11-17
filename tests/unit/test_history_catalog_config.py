"""Catalog profiles of the history table (ADR-011): configuration without any server."""

from __future__ import annotations

from pathlib import Path

import pytest

from featureforge.streaming.history import (
    CATALOG_NAME,
    DEFAULT_CATALOG_URL,
    catalog_db_path,
    spark_catalog_config,
    warehouse_path,
)
from featureforge.streaming.materialize_spec import parse_arguments

ROOT = Path(__file__).resolve().parents[2]


def test_local_profile_points_spark_at_the_sqlite_file(tmp_path):
    config = spark_catalog_config(tmp_path)
    root = tmp_path.resolve()
    prefix = f"spark.sql.catalog.{CATALOG_NAME}"

    assert config[f"{prefix}.uri"] == f"jdbc:sqlite:{catalog_db_path(root)}"
    assert config[f"{prefix}.warehouse"] == f"file://{warehouse_path(root)}"
    assert f"{prefix}.jdbc.user" not in config


def test_postgres_profile_is_built_from_the_url_parts(tmp_path):
    url = "postgresql+psycopg2://alice:secret@db.example:6543/lake"
    config = spark_catalog_config(tmp_path, url, name="demo")
    prefix = "spark.sql.catalog.demo"

    assert config[f"{prefix}.uri"] == "jdbc:postgresql://db.example:6543/lake"
    assert config[f"{prefix}.jdbc.user"] == "alice"
    assert config[f"{prefix}.jdbc.password"] == "secret"
    assert config[f"{prefix}.catalog-impl"] == "org.apache.iceberg.jdbc.JdbcCatalog"


def test_other_database_backends_are_rejected(tmp_path):
    with pytest.raises(ValueError, match="postgresql"):
        spark_catalog_config(tmp_path, "mysql://user:pw@host/db")


def test_default_url_matches_the_compose_file():
    compose = (ROOT / "docker-compose.yml").read_text()

    assert "127.0.0.1:55432:5432" in compose
    assert ":55432/" in DEFAULT_CATALOG_URL


def test_materializer_accepts_a_catalog_url():
    url = "postgresql+psycopg2://user:pw@host/db"
    arguments = parse_arguments(
        [
            "--spec",
            "spec.yaml",
            "--group-id",
            "group",
            "--history-root",
            "history",
            "--history-catalog-url",
            url,
        ]
    )

    assert arguments.history_catalog_url == url
    assert parse_arguments(["--spec", "s.yaml", "--group-id", "g"]).history_catalog_url is None
