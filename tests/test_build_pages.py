import importlib.util
import tempfile
import unittest
from pathlib import Path

spec = importlib.util.spec_from_file_location("build_pages", Path(__file__).resolve().parents[1] / "scripts/build-pages.py")
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)

class BuildTests(unittest.TestCase):
    def test_transitive_hash_and_source_unchanged(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            src = root / "src"
            src.mkdir()
            html = '<link href="main.css?v=3" rel="stylesheet"><script src="main.js"></script>'
            (src / "index.html").write_text(html)
            (src / "main.css").write_text('@import "tokens.css";')
            (src / "tokens.css").write_text(':root{color:red}')
            (src / "main.js").write_text('import "./dep.js";')
            (src / "dep.js").write_text('export const n=1;')
            module.build(src, root / "one")
            module.build(src, root / "two")
            first = (root / "one/index.html").read_text()
            self.assertEqual(first, (root / "two/index.html").read_text())
            self.assertNotIn("v=3", first)
            self.assertEqual((src / "index.html").read_text(), html)
            (src / "tokens.css").write_text(':root{color:blue}')
            module.build(src, root / "three")
            self.assertNotEqual(first, (root / "three/index.html").read_text())
            self.assertEqual((root / "one/main.js").read_text(), (root / "three/main.js").read_text())
