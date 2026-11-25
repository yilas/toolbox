"""Check first-party external and inline browser JavaScript."""
from html.parser import HTMLParser
from pathlib import Path
import subprocess
import tempfile

ROOT = Path(__file__).resolve().parents[1]
class Scripts(HTMLParser):
    def __init__(self):
        super().__init__()
        self.active = None
        self.scripts = []
    def handle_starttag(self, tag, attrs):
        a = dict(attrs)
        if tag == "script" and "src" not in a and a.get("type", "").lower() in ("", "module", "text/javascript", "application/javascript"):
            self.active = [a.get("type") == "module", ""]
    def handle_data(self, data):
        if self.active is not None:
            self.active[1] += data
    def handle_endtag(self, tag):
        if tag == "script" and self.active is not None:
            self.scripts.append(self.active)
            self.active = None

def check(file):
    subprocess.run(["node", "--check", str(file)], check=True)

for file in [ROOT / "portal.js", * (ROOT / "tools").rglob("*"), *(ROOT / "assets/js").rglob("*")]:
    if file.suffix in (".js", ".mjs", ".cjs"):
        check(file)
for file in [ROOT / "index.html", *(ROOT / "tools").glob("*.html"), *(ROOT / "watch").glob("*.html")]:
    parser = Scripts()
    parser.feed(file.read_text(encoding="utf-8"))
    for i, (module, source) in enumerate(parser.scripts):
        with tempfile.TemporaryDirectory() as tmp:
            script = Path(tmp) / ("inline.mjs" if module else "inline.cjs")
            script.write_text(source, encoding="utf-8")
            print(f"{file.relative_to(ROOT)} inline script {i+1}", flush=True)
            check(script)
