from pathlib import Path

from featureforge.cli import build_parser


def test_generate_command_accepts_config_and_output_paths() -> None:
    parser = build_parser()

    args = parser.parse_args(
        [
            "generate",
            "--config",
            "configs/synthetic_data.yaml",
            "--output",
            "data/generated",
        ]
    )

    assert args.command == "generate"
    assert args.config == Path("configs/synthetic_data.yaml")
    assert args.output == Path("data/generated")
