from __future__ import annotations

import ast
from pathlib import Path


def test_scanner_has_no_review_or_broker_write_imports():
    forbidden = {"approval", "bridge", "creator", "broker_write", "execution"}
    root = Path(__file__).parents[2] / "options_copilot" / "scanner"
    for path in root.glob("*.py"):
        tree = ast.parse(path.read_text(encoding="utf-8"))
        imports = [alias.name for node in ast.walk(tree) if isinstance(node, (ast.Import, ast.ImportFrom)) for alias in node.names]
        assert not any(part in forbidden for name in imports for part in name.split(".")), path
