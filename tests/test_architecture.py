"""Keep the SDK responsible for HTTP fetching and HTML parsing."""
import ast
from pathlib import Path
import unittest


class SDKBoundaryTests(unittest.TestCase):
    def test_bot_does_not_depend_on_scraping_implementation(self):
        for path in (Path(__file__).parents[1] / "bot").rglob("*.py"):
            tree = ast.parse(path.read_text(encoding="utf-8"))
            for node in ast.walk(tree):
                if isinstance(node, (ast.Import, ast.ImportFrom)):
                    names = [node.module or ""] if isinstance(node, ast.ImportFrom) else [n.name for n in node.names]
                    for name in names:
                        self.assertFalse(name == "bs4" or name.startswith("backloggd.parsers"), f"{path}: scraping implementation import {name}")
                if isinstance(node, ast.Attribute):
                    self.assertNotIn(node.attr, ("_request_with_retry", "_check_response"), f"{path}: private SDK HTTP API")
