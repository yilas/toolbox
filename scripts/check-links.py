"""Check static references in the built Toolbox site; external checks are advisory."""
import argparse
import concurrent.futures
import re
from html.parser import HTMLParser
from pathlib import Path
from urllib.parse import unquote, urlsplit
from urllib.request import Request, urlopen
from urllib.error import HTTPError, URLError

class Page(HTMLParser):
    def __init__(self, text):
        super().__init__()
        self.ids = set()
        self.links = []
        self.feed(text)

    def handle_starttag(self, tag, attrs):
        attrs = dict(attrs)
        if attrs.get("id"):
            self.ids.add(attrs["id"])
        if tag == "a" and attrs.get("name"):
            self.ids.add(attrs["name"])
        for key in ("href", "src", "poster"):
            if attrs.get(key):
                self.links.append((self.getpos()[0], attrs[key]))

def scan(root):
    root = root.resolve()
    pages = {p: Page(p.read_text(encoding="utf-8")) for p in root.rglob("*.html")
             if "vendor" not in p.relative_to(root).parts}
    errors, external = [], set()
    for owner, page in pages.items():
        for line, raw in page.links:
            url = urlsplit(raw)
            if url.scheme in ("http", "https") or url.netloc:
                external.add(("https:" if not url.scheme else "") + raw.split("#", 1)[0])
                continue
            if url.scheme:
                continue
            target = ((root / unquote(url.path).lstrip("/")) if url.path.startswith("/")
                      else (owner.parent / unquote(url.path)) if url.path else owner).resolve()
            if target.is_dir():
                target = target / "index.html"
            reason = None
            if not target.is_relative_to(root) or not target.is_file():
                reason = "missing target"
            elif url.fragment and target in pages and unquote(url.fragment) not in pages[target].ids:
                reason = "missing fragment"
            if reason:
                errors.append(f"{owner.relative_to(root)}:{line}: {reason}: {raw}")
    return errors, external

def probe(url):
    try:
        with urlopen(Request(url, headers={"User-Agent": "Toolbox-LinkChecker/1.0"}, method="HEAD"), timeout=10) as response:
            return url, str(response.status)
    except HTTPError as exc:
        if exc.code in (403, 405, 501):
            try:
                with urlopen(Request(url, headers={"User-Agent": "Toolbox-LinkChecker/1.0", "Range": "bytes=0-0"}), timeout=10) as response:
                    return url, str(response.status)
            except HTTPError as retry:
                return url, str(retry.code)
            except (URLError, OSError, ValueError) as retry:
                return url, str(retry)
        return url, str(exc.code)
    except (URLError, OSError, ValueError) as exc:
        return url, str(exc)

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("root", type=Path)
    parser.add_argument("--external", action="store_true")
    args = parser.parse_args()
    if not (args.root / "index.html").is_file():
        parser.error("Expected a built site containing index.html")
    errors, external = scan(args.root)
    for error in errors:
        print(error)
    print(f"Internal errors: {len(errors)}; unique external URLs: {len(external)}")
    if args.external:
        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
            for url, status in pool.map(probe, sorted(external)):
                if not status.startswith("2"):
                    print(f"External review: {status} {url}")
    return int(bool(errors))

if __name__ == "__main__":
    raise SystemExit(main())
