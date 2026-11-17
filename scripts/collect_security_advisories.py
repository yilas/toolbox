#!/usr/bin/env python3
"""Collect public GitHub security advisories and Microsoft AKS bulletins.

No third-party Python dependencies. The output is reviewed through a PR,
never pushed directly to the published main branch.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timedelta, timezone
from html.parser import HTMLParser
import json
import os
from pathlib import Path
import re
import sys
import time
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

AKS_URL = "https://learn.microsoft.com/en-us/azure/aks/security-bulletins/overview"
REPOSITORIES = {
    "argoproj/argo-cd": "Argo CD",
    "cilium/cilium": "Cilium (OSS)",
    "kyverno/kyverno": "Kyverno",
    "goharbor/harbor": "Harbor",
    "coder/coder": "Coder",
    "akuity/kargo": "Kargo",
    "kubernetes/kubernetes": "Kubernetes",
}
OUTPUT = Path("tools/data/security-advisories.json")
CVE_RE = re.compile(r"\bCVE-\d{4}-\d{4,}\b", re.IGNORECASE)
AKS_RE = re.compile(r"\bAKS-\d{4}-\d{4}\b", re.IGNORECASE)
DATE_RE = re.compile(r"Published date:\s*([A-Za-z]+ \d{1,2}, \d{4})", re.I)
MAX_BYTES = 5_000_000
MAX_AGE_DAYS = 365
MAX_PER_SOURCE = 40


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def request_bytes(url: str, *, token: str = "") -> bytes:
    headers = {
        "User-Agent": "yilas-toolbox-security-advisories/1.0",
        "Accept": "application/vnd.github+json" if url.startswith("https://api.github.com/") else "text/html",
    }
    if token and url.startswith("https://api.github.com/"):
        headers["Authorization"] = "Bearer " + token
        headers["X-GitHub-Api-Version"] = "2022-11-28"
    req = Request(url, headers=headers)
    last_error = None
    for attempt in range(2):
        try:
            with urlopen(req, timeout=25) as response:
                body = response.read(MAX_BYTES + 1)
            if len(body) > MAX_BYTES:
                raise ValueError("Response exceeds the size limit")
            return body
        except (HTTPError, URLError, TimeoutError) as error:
            last_error = error
            if isinstance(error, HTTPError) and error.code not in (429, 500, 502, 503, 504):
                break
            if attempt == 0:
                time.sleep(2)
    raise last_error


def iso_date(value: object) -> str | None:
    if not isinstance(value, str) or not value:
        return None
    try:
        dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            return None
        return dt.astimezone(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")
    except ValueError:
        return None


def recent(value: str | None, *, current: datetime) -> bool:
    if not value:
        return False
    parsed = iso_date(value)
    if not parsed:
        return False
    return datetime.fromisoformat(parsed.replace("Z", "+00:00")) >= current - timedelta(days=MAX_AGE_DAYS)


def github_advisories(repo: str, product: str, *, token: str, current: datetime) -> list[dict]:
    url = f"https://api.github.com/repos/{repo}/security-advisories?per_page=100"
    payload = json.loads(request_bytes(url, token=token))
    if not isinstance(payload, list):
        raise ValueError("GitHub returned an unexpected payload")
    items = []
    for entry in payload:
        if not isinstance(entry, dict) or entry.get("state") != "published":
            continue
        published = iso_date(entry.get("published_at"))
        if not recent(published, current=current):
            continue
        ghsa = entry.get("ghsa_id")
        if not isinstance(ghsa, str) or not re.fullmatch(r"GHSA-[a-z0-9-]{10,}", ghsa, re.I):
            continue
        official = f"https://github.com/{repo}/security/advisories/{ghsa}"
        identifiers = entry.get("identifiers") or []
        cves = sorted({v["value"].upper() for v in identifiers
                       if isinstance(v, dict) and v.get("type") == "CVE"
                       and isinstance(v.get("value"), str) and CVE_RE.fullmatch(v["value"])})
        cve_id = entry.get("cve_id")
        if isinstance(cve_id, str) and CVE_RE.fullmatch(cve_id):
            cves = sorted(set(cves) | {cve_id.upper()})
        if not cves:
            cves = sorted(set(x.upper() for x in CVE_RE.findall(entry.get("summary") or "")))
        affected, patched = set(), set()
        for vulnerability in entry.get("vulnerabilities") or []:
            if not isinstance(vulnerability, dict):
                continue
            package = vulnerability.get("package") or {}
            package_name = package.get("name") if isinstance(package, dict) else None
            prefix = f"{package_name}: " if isinstance(package_name, str) else ""
            version_range = vulnerability.get("vulnerable_version_range")
            if isinstance(version_range, str) and version_range.strip():
                affected.add((prefix + version_range.strip())[:250])
            fixed = vulnerability.get("first_patched_version") or {}
            if isinstance(fixed, dict) and isinstance(fixed.get("identifier"), str):
                patched.add((prefix + fixed["identifier"])[:250])
        severity = str(entry.get("severity") or "unknown").lower()
        if severity not in {"critical", "high", "moderate", "low"}:
            severity = "unknown"
        summary = entry.get("summary") or ghsa
        items.append({
            "id": ghsa.upper(),
            "source": f"github:{repo}",
            "product": product,
            "title": str(summary)[:240],
            "url": official,
            "published_at": published,
            "updated_at": iso_date(entry.get("updated_at")),
            "severity": severity,
            "cves": cves[:12],
            "affected_versions": sorted(affected)[:12],
            "patched_versions": sorted(patched)[:12],
        })
    return sorted(items, key=lambda item: (item["published_at"], item["id"]), reverse=True)[:MAX_PER_SOURCE]


class AKSBulletinParser(HTMLParser):
    """Extract individual AKS bulletins from the official overview, conservatively."""

    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.bulletins = []
        self.current = None
        self.heading = None
        self.heading_id = ""
        self.paragraph = None

    def handle_starttag(self, tag, attrs):
        if tag == "h2":
            self._finish()
            self.heading = []
            self.heading_id = dict(attrs).get("id", "")
        elif tag == "p" and self.current is not None:
            self.paragraph = []

    def handle_data(self, data):
        if self.heading is not None:
            self.heading.append(data)
        elif self.paragraph is not None:
            self.paragraph.append(data)

    def handle_endtag(self, tag):
        if tag == "h2" and self.heading is not None:
            title = " ".join(" ".join(self.heading).split())
            match = AKS_RE.search(title)
            self.current = {
                "id": match.group(0).upper(),
                "title": title[:240],
                "anchor": self.heading_id,
                "paragraphs": [],
            } if match else None
            self.heading = None
        elif tag == "p" and self.paragraph is not None:
            text = " ".join(" ".join(self.paragraph).split())
            if text and self.current is not None:
                self.current["paragraphs"].append(text)
            self.paragraph = None

    def _finish(self):
        if self.current is not None:
            self.bulletins.append(self.current)
            self.current = None

    def close(self):
        super().close()
        self._finish()


def aks_advisories(*, current: datetime) -> list[dict]:
    parser = AKSBulletinParser()
    parser.feed(request_bytes(AKS_URL).decode("utf-8"))
    parser.close()
    if not parser.bulletins:
        raise ValueError("No AKS bulletins detected; refusing to publish an empty feed")
    items = []
    for bulletin in parser.bulletins:
        date = None
        for paragraph in bulletin["paragraphs"][:5]:
            match = DATE_RE.search(paragraph)
            if match:
                try:
                    date = datetime.strptime(match.group(1), "%B %d, %Y").replace(tzinfo=timezone.utc)
                except ValueError:
                    pass
                break
        if date is None:
            continue
        published = date.isoformat(timespec="seconds").replace("+00:00", "Z")
        if not recent(published, current=current):
            continue
        text = " ".join(bulletin["paragraphs"][:8])
        cves = sorted({cve.upper() for cve in CVE_RE.findall(bulletin["title"] + " " + text)})[:12]
        summary = next((p for p in bulletin["paragraphs"] if not DATE_RE.search(p)), bulletin["title"])
        anchor = bulletin["anchor"]
        # Only use the anchor actually supplied by Microsoft Learn.
        url = AKS_URL + ("#" + anchor if re.fullmatch(r"[a-z0-9_-]{1,150}", anchor) else "")
        items.append({
            "id": bulletin["id"],
            "source": "aks",
            "product": "AKS",
            "title": bulletin["title"],
            "url": url,
            "published_at": published,
            "updated_at": None,
            "severity": "unknown",  # Microsoft doesn't assign a uniform severity to bulletins.
            "cves": cves,
            "affected_versions": [],  # Do not infer versions from unstructured prose.
            "patched_versions": [],
            "summary": summary[:320],
        })
    if not items:
        raise ValueError("AKS bulletins found but no recent dated entries parsed")
    unique = {item["id"]: item for item in items}
    return sorted(unique.values(), key=lambda item: (item["published_at"], item["id"]), reverse=True)[:MAX_PER_SOURCE]


def load_existing(path: Path) -> dict:
    try:
        previous = json.loads(path.read_text(encoding="utf-8"))
        if previous.get("schema_version") == 1 and isinstance(previous.get("alerts"), list) and isinstance(previous.get("sources"), dict):
            return previous
    except (OSError, ValueError, AttributeError):
        pass
    return {"schema_version": 1, "generated_at": None, "sources": {}, "alerts": []}


def collect(path: Path, *, token: str = "", current: datetime | None = None,
            github_fetch=None, aks_fetch=None) -> dict:
    current = current or datetime.now(timezone.utc)
    stamp = current.isoformat(timespec="seconds").replace("+00:00", "Z")
    previous = load_existing(path)
    prior_by_source = {}
    for alert in previous["alerts"]:
        if isinstance(alert, dict) and isinstance(alert.get("source"), str):
            prior_by_source.setdefault(alert["source"], []).append(alert)
    sources, all_items = {}, []
    success = 0
    jobs = [("aks", "AKS")] + [(f"github:{repo}", product) for repo, product in REPOSITORIES.items()]
    for key, product in jobs:
        prior_status = previous["sources"].get(key, {})
        try:
            if key == "aks":
                items = (aks_fetch or aks_advisories)(current=current)
            else:
                repo = key.removeprefix("github:")
                items = (github_fetch or github_advisories)(repo, product, token=token, current=current)
            if not isinstance(items, list):
                raise ValueError("Collector returned invalid data")
            success += 1
            sources[key] = {"product": product, "status": "ok", "checked_at": stamp, "last_success_at": stamp}
        except (ValueError, OSError, HTTPError, URLError, TimeoutError, json.JSONDecodeError) as error:
            items = prior_by_source.get(key, [])
            # Never expose HTTP request headers, tokens or raw exception URLs.
            detail = f"HTTP {error.code}" if isinstance(error, HTTPError) else type(error).__name__
            sources[key] = {
                "product": product, "status": "error", "checked_at": stamp,
                "last_success_at": prior_status.get("last_success_at"),
                "error": detail,
            }
            print(f"Warning: {key}: {detail}; retaining {len(items)} previous items", file=sys.stderr)
        all_items.extend(items)
    if success == 0:
        raise RuntimeError("All advisory sources failed; existing snapshot was not overwritten")
    unique = {(item["source"], item["id"]): item for item in all_items
              if isinstance(item, dict) and isinstance(item.get("source"), str) and isinstance(item.get("id"), str)}
    alerts = sorted(unique.values(), key=lambda item: (item.get("published_at") or "", item["id"]), reverse=True)
    snapshot = {"schema_version": 1, "generated_at": stamp, "sources": sources, "alerts": alerts}
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(snapshot, ensure_ascii=False, indent=2, sort_keys=False) + "\n", encoding="utf-8")
    return snapshot


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=OUTPUT)
    args = parser.parse_args()
    try:
        snapshot = collect(args.output, token=os.environ.get("GITHUB_TOKEN", ""))
    except RuntimeError as error:
        print(f"ERROR: {error}", file=sys.stderr)
        return 1
    errors = sum(source["status"] == "error" for source in snapshot["sources"].values())
    print(f"Collected {len(snapshot['alerts'])} advisories; {errors} source(s) unavailable")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
