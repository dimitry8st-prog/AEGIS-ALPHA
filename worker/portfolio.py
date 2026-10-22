"""Read the explicitly selected research universe without creating positions."""

import json
from pathlib import Path

PORTFOLIO_FILE = Path(__file__).resolve().parents[1] / "config/portfolios/russian-research.json"


def research_portfolio() -> dict:
    return json.loads(PORTFOLIO_FILE.read_text(encoding="utf-8"))
