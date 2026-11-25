"""The tour notebook is checked in with its outputs and without errors or local paths."""

import json
from pathlib import Path

NOTEBOOK = Path(__file__).resolve().parents[2] / "notebooks" / "featureforge-tour.ipynb"


def test_the_tour_notebook_was_executed_without_errors() -> None:
    notebook = json.loads(NOTEBOOK.read_text())
    code = [cell for cell in notebook["cells"] if cell["cell_type"] == "code"]

    assert code
    for cell in code:
        assert cell["execution_count"] is not None
        assert all(output["output_type"] != "error" for output in cell["outputs"])


def test_the_tour_notebook_holds_no_local_path() -> None:
    assert "/Users/" not in NOTEBOOK.read_text()
