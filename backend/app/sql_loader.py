from functools import lru_cache
from pathlib import Path


@lru_cache
def load_sql(filename: str) -> str:
    """
    Load a SQL file from the shared sql/queries directory.
    """
    base = Path(__file__).resolve().parents[2] / "sql" / "queries"
    path = base / filename
    return path.read_text(encoding="utf-8")

