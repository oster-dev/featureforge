from pathlib import Path

from featureforge.cli import main


def test_generate_command_writes_parquet_dataset(
    monkeypatch,
    tmp_path: Path,
) -> None:
    config_path = Path("configs/synthetic_data.yaml")
    output_dir = tmp_path / "generated"

    monkeypatch.setattr(
        "sys.argv",
        [
            "featureforge",
            "generate",
            "--config",
            str(config_path),
            "--output",
            str(output_dir),
        ],
    )

    main()

    assert (output_dir / "users.parquet").exists()
    assert (output_dir / "content.parquet").exists()
    assert (output_dir / "events.parquet").exists()
    assert (output_dir / "labels.parquet").exists()
