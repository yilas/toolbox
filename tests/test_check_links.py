import importlib.util
import tempfile
import unittest
from pathlib import Path

spec = importlib.util.spec_from_file_location("checker", Path(__file__).resolve().parents[1] / "scripts/check-links.py")
checker = importlib.util.module_from_spec(spec)
spec.loader.exec_module(checker)

class LinkTests(unittest.TestCase):
    def test_static_links(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "index.html").write_text('<a href="tools/#ok">OK</a><a href="tools/#bad">Bad</a><img src="missing.png"><script src="app.js?v=123"></script><a href="https://example.com/#x">External</a><a href="mailto:a@example.com">Mail</a>', encoding="utf-8")
            (root / "tools").mkdir()
            (root / "tools/index.html").write_text('<div id="ok"></div>', encoding="utf-8")
            (root / "app.js").touch()
            errors, external = checker.scan(root)
            self.assertEqual(len(errors), 2)
            self.assertTrue(any("missing fragment" in e for e in errors))
            self.assertTrue(any("missing target" in e for e in errors))
            self.assertEqual(external, {"https://example.com/"})

if __name__ == "__main__":
    unittest.main()
