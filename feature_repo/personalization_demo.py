"""Rank materialized content candidates using Feast online feature retrieval."""

from __future__ import annotations

import argparse
from collections.abc import Sequence
from pathlib import Path

from featureforge.serving import rank_online_content_candidates

PROJECT_ROOT = Path(__file__).parent.parent
FEATURE_REPO_PATH = Path(__file__).parent

DEFAULT_USER_ID = "user_000290"
DEFAULT_CANDIDATE_CONTENT_IDS = (
    "content_000016",
    "content_000089",
    "content_000108",
    "content_000169",
    "content_000170",
    "content_000178",
    "content_000204",
    "content_000216",
    "content_000226",
)
DEFAULT_TOP_K = 5


def parse_args() -> argparse.Namespace:
    """Parse an online personalization ranking request."""
    parser = argparse.ArgumentParser(
        description=("Rank content candidates with current Feast Redis user and content features.")
    )
    parser.add_argument(
        "--user-id",
        default=DEFAULT_USER_ID,
        help=f"Materialized user ID to rank for (default: {DEFAULT_USER_ID}).",
    )
    parser.add_argument(
        "--content-id",
        action="append",
        dest="content_ids",
        help=(
            "Candidate content ID. Repeat this flag for multiple candidates. "
            "Defaults to a deterministic built-in candidate list."
        ),
    )
    parser.add_argument(
        "--top-k",
        type=int,
        default=DEFAULT_TOP_K,
        help=f"Maximum number of candidates to return (default: {DEFAULT_TOP_K}).",
    )
    return parser.parse_args()


def format_score(value: float) -> str:
    """Render a score consistently for terminal output."""
    return f"{value:.4f}"


def print_ranking(
    *,
    user_id: str,
    content_ids: Sequence[str],
    top_k: int,
) -> None:
    """Retrieve online features and print a deterministic candidate ranking."""
    result = rank_online_content_candidates(
        user_id,
        content_ids,
        top_k=top_k,
        repo_path=FEATURE_REPO_PATH,
    )

    print("=== FeatureForge Online Personalization Demo ===")
    print(f"User ID:             {result.user_id}")
    print(f"Candidate count:     {len(set(content_ids))}")
    print(f"Requested top-k:     {top_k}")

    if result.skipped_content_ids:
        print("Skipped candidates:  " + ", ".join(result.skipped_content_ids))

    print("\n=== Ranked Content ===")
    print(f"{'Rank':<6}{'Content ID':<22}{'Score':>10}{'User':>10}{'Content':>10}")
    print("-" * 58)

    for rank, item in enumerate(result.ranked_content, start=1):
        print(
            f"{rank:<6}"
            f"{item.content_id:<22}"
            f"{format_score(item.score):>10}"
            f"{format_score(item.user_engagement_score):>10}"
            f"{format_score(item.content_popularity_score):>10}"
        )


def main() -> None:
    """Run a candidate-ranking demo against Feast's Redis online store."""
    args = parse_args()
    content_ids = (
        tuple(args.content_ids) if args.content_ids is not None else DEFAULT_CANDIDATE_CONTENT_IDS
    )

    try:
        print_ranking(
            user_id=args.user_id,
            content_ids=content_ids,
            top_k=args.top_k,
        )
    except (LookupError, ValueError) as error:
        raise SystemExit(f"Online personalization request failed: {error}") from error


if __name__ == "__main__":
    main()
