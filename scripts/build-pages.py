"""Prepare a static Pages artifact without modifying the source checkout."""
import hashlib
import re
import shutil
import sys
from pathlib import Path
from urllib.parse import urlsplit, urlunsplit, parse_qsl, urlencode

ROOT = Path(__file__).resolve().parents[1]
ATTR = re.compile(r'(<(?:script|link)\b[^>]*?\b(?:src|href)\s*=\s*)(["\'])([^"\']+)\2', re.I)
CSS = re.compile(r'(@import\s+(?:url\(\s*)?)(["\'])([^"\']+)\2', re.I)
JS = re.compile(r'((?:\bfrom\s*|\bimport\s*(?:\(\s*)?))(["\'])(\.{1,2}/[^"\']+)\2')
EXT = {".css", ".js", ".mjs"}

def build(source, dest):
    source, dest = source.resolve(), dest.resolve()
    if dest == source or source in dest.parents or dest in source.parents:
        raise ValueError("Output must be outside the source tree")
    if dest.exists():
        raise ValueError("Output must not exist")
    dest.mkdir(parents=True)
    for name in ("assets", "tools", "watch"):
        if (source / name).is_dir():
            shutil.copytree(source / name, dest / name)
    for file in source.iterdir():
        if file.is_file() and (file.suffix in {".html", ".css", ".js", ".ico", ".png", ".svg", ".webmanifest", ".txt"} or file.name in {"CNAME", ".nojekyll"}):
            shutil.copy2(file, dest / file.name)
    (dest / ".nojekyll").touch()
    cache = {}
    active = set()
    def version(file):
        if file in cache:
            return cache[file]
        if file in active:
            raise ValueError(f"Cyclic asset imports: {file}")
        active.add(file)
        if "vendor" not in file.relative_to(dest).parts:
            pattern = CSS if file.suffix == ".css" else JS
            original = file.read_text(encoding="utf-8")
            updated = rewrite(file, original, pattern)
            if updated != original:
                file.write_text(updated, encoding="utf-8")
        digest = hashlib.sha256(file.read_bytes()).hexdigest()[:16]
        cache[file] = digest
        active.remove(file)
        return digest
    def rewrite(owner, content, pattern):
        def replace(match):
            prefix, quote, raw = match.groups()
            url = urlsplit(raw)
            if url.scheme or url.netloc or not url.path or url.path.startswith("/"):
                return match.group(0)
            target = (owner.parent / url.path).resolve()
            if dest not in target.parents or target.suffix not in EXT or not target.is_file():
                return match.group(0)
            query = [(k,v) for k,v in parse_qsl(url.query, keep_blank_values=True) if k != "v"]
            query.append(("v", version(target)))
            return prefix + quote + urlunsplit(("", "", url.path, urlencode(query), url.fragment)) + quote
        return pattern.sub(replace, content)
    for file in sorted(dest.rglob("*.html")):
        if "vendor" not in file.relative_to(dest).parts:
            original = file.read_text(encoding="utf-8")
            file.write_text(rewrite(file, original, ATTR), encoding="utf-8")
    return cache

if __name__ == "__main__":
    build(ROOT, Path(sys.argv[1]))
