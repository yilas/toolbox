#!/usr/bin/env python3
"""Collect public GitHub security advisories and Microsoft AKS bulletins.

No third-party Python dependencies. The output is reviewed through a PR,
never pushed directly to the published main branch.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timedelta, timezone
import json
import os
from pathlib import Path
import re
import sys
import time
import xml.etree.ElementTree as ET
from email.utils import parsedate_to_datetime
from html.parser import HTMLParser
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

AKS_URL = "https://learn.microsoft.com/en-us/azure/aks/security-bulletins/overview"
AKS_MARKDOWN_URL = ("https://raw.githubusercontent.com/MicrosoftDocs/azure-aks-docs/"
                    "main/articles/aks/security-bulletins/overview.md")
REPOSITORIES = {
    "argoproj/argo-cd": "Argo CD",
    "cilium/cilium": "Cilium (OSS)",
    "kyverno/kyverno": "Kyverno",
    "kubernetes-sigs/external-dns": "ExternalDNS",
    "cert-manager/cert-manager": "cert-manager",
    "goharbor/harbor": "Harbor",
    "coder/coder": "Coder",
    "akuity/kargo": "Kargo",
    "kubernetes/kubernetes": "Kubernetes",
}
ELASTIC_RSS = "https://discuss.elastic.co/c/announcements/security-announcements/31.rss"
OUTPUT = Path("tools/data/security-advisories.json")
CVE_RE = re.compile(r"\bCVE-\d{4}-\d{4,}\b", re.IGNORECASE)
AKS_RE = re.compile(r"\bAKS-\d{4}-\d{4}\b", re.IGNORECASE)
DATE_RE = re.compile(r"^\s*Published\s+date\s*:\s*([A-Za-z]+ \d{1,2}, \d{4})\s*$", re.I)
AKS_HEADING_RE = re.compile(r"(?m)^##[ \t]+([^\r\n]+)$")
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


def _aks_plain_text(value: str) -> str:
    """Remove only common Markdown formatting from a display excerpt."""
    value = re.sub(r"\[([^\]]+)\]\([^)]+\)", r"\1", value)
    value = value.replace(chr(96), "").replace("**", "").replace("__", "")
    return " ".join(value.split())


def _aks_sections(markdown: str) -> list[tuple[str, str]]:
    """Find level-2 AKS bulletins; never include text from the next section."""
    headings = list(AKS_HEADING_RE.finditer(markdown))
    sections = []
    for index, heading in enumerate(headings):
        title = heading.group(1).strip()
        if not AKS_RE.match(title):
            continue
        end = headings[index + 1].start() if index + 1 < len(headings) else len(markdown)
        sections.append((title, markdown[heading.end():end]))
    return sections


def _aks_date(body: str) -> datetime | None:
    # MicrosoftDocs publishes **Published date**: and **Published Date**: variants.
    # Check only the beginning of each bulletin, never a referenced article's date.
    for line in body.splitlines()[:18]:
        plain = line.strip().replace("**", "").replace("__", "")
        match = DATE_RE.match(plain)
        if not match:
            continue
        try:
            return datetime.strptime(match.group(1), "%B %d, %Y").replace(tzinfo=timezone.utc)
        except ValueError:
            return None
    return None


def _aks_description(body: str) -> str:
    # Extract the first prose paragraph in the Description section, not the
    # References/Affected Versions/Resolutions sections (which may cite unrelated CVEs).
    match = re.search(
        r"(?ms)^###\s+Description\s*$\s*(.*?)(?=^###\s+|^##\s+|\Z)", body
    )
    if not match:
        return ""
    paragraphs = re.split(r"\n\s*\n", match.group(1).strip())
    for paragraph in paragraphs:
        if paragraph and not paragraph.lstrip().startswith(("-", "|", ">", "#")):
            return _aks_plain_text(paragraph)
    return ""


def _aks_anchor(title: str) -> str:
    # Microsoft Learn generates heading slugs. Unknown punctuation is stripped;
    # a changed anchor still opens the authoritative overview page.
    slug = re.sub(r"[^a-z0-9 -]", "", title.lower())
    return re.sub(r"\s+", "-", slug.strip())[:180]


def aks_advisories(*, current: datetime) -> list[dict]:
    """Parse MicrosoftDocs' public Markdown source, not rendered Learn HTML.

    Do not infer severity or affected/fixed versions from free-form text.
    """
    markdown = request_bytes(AKS_MARKDOWN_URL).decode("utf-8-sig")
    sections = _aks_sections(markdown)
    if not sections:
        raise ValueError("No AKS bulletin headings in the official Markdown source")
    items = []
    parsed_dates = 0
    for title, body in sections:
        date = _aks_date(body)
        if date is None:
            continue
        parsed_dates += 1
        published = date.isoformat(timespec="seconds").replace("+00:00", "Z")
        if not recent(published, current=current):
            continue
        bulletin_id = AKS_RE.match(title).group(0).upper()
        description = _aks_description(body)
        # Only associate CVEs in the heading or opening description paragraph.
        # Some AKS advisories mention unrelated CVEs as historical examples.
        cves = sorted({cve.upper() for cve in CVE_RE.findall(title + " " + description)})[:12]
        anchor = _aks_anchor(title)
        items.append({
            "id": bulletin_id,
            "source": "aks",
            "product": "AKS",
            "title": _aks_plain_text(title)[:240],
            "url": AKS_URL + ("#" + anchor if anchor else ""),
            "published_at": published,
            "updated_at": None,
            "severity": "unknown",
            "cves": cves,
            "affected_versions": [],
            "patched_versions": [],
            "summary": description[:320],
        })
    if parsed_dates == 0:
        raise ValueError("AKS headings found but no publication dates parsed")
    unique = {item["id"]: item for item in items}
    return sorted(unique.values(), key=lambda item: (item["published_at"], item["id"]), reverse=True)[:MAX_PER_SOURCE]



class ElasticText(HTMLParser):
    def __init__(self, html):
        super().__init__()
        self.parts = []
        self.feed(html)

    def handle_data(self, data):
        self.parts.append(data)

def elastic_advisories(product, *, current):
    root = ET.fromstring(request_bytes(ELASTIC_RSS))
    channel = root.find("channel")
    if channel is None or not channel.findall("item"):
        raise ValueError("Unexpected Elastic RSS structure")
    items = []
    for entry in channel.findall("item"):
        title = entry.findtext("title", "")
        # Match explicit product names in the product heading, not references in prose.
        heading = re.split(r"\b\d+\.\d+|Security Update", title, maxsplit=1, flags=re.I)[0]
        if not re.search(r"\b" + re.escape(product) + r"\b", heading, re.I):
            continue
        esa = re.search(r"\bESA-\d{4}-[a-z0-9]+(?:-[a-z0-9]+)?\b", title, re.I)
        url = entry.findtext("link", "")
        if not esa or not re.fullmatch(r"https://discuss\.elastic\.co/t/[^?#]+(?:\?[^#]*)?", url):
            continue
        try:
            date = parsedate_to_datetime(entry.findtext("pubDate", ""))
            published = iso_date(date.isoformat())
        except (ValueError, TypeError):
            continue
        if not recent(published, current=current):
            continue
        body = " ".join(ElasticText(entry.findtext("description", "")).parts)
        severity = re.search(r"Severity:\s*(?:CVSSv?[\d.]+\s*:\s*)?(Critical|High|Medium|Moderate|Low)\b", body, re.I)
        cves = set()
        for match in re.finditer(r"CVE\s*IDs?\s*:\s*((?:CVE-\d{4}-\d{4,}(?:\s*[,;]\s*)?)+)", body, re.I):
            cves.update(x.upper() for x in CVE_RE.findall(match.group(1)))
        items.append({"id": esa.group(0).upper(), "source": "elastic:" + product.lower(),
                      "product": product, "title": title[:240], "url": url,
                      "published_at": published, "updated_at": None,
                      "severity": {"medium": "moderate"}.get(severity.group(1).lower(), severity.group(1).lower()) if severity else "unknown",
                      "cves": sorted(cves)[:12], "affected_versions": [], "patched_versions": []})
    return sorted(items, key=lambda x: (x["published_at"], x["id"]), reverse=True)[:MAX_PER_SOURCE]


def load_existing(path: Path) -> dict:
    try:
        previous = json.loads(path.read_text(encoding="utf-8"))
        if previous.get("schema_version") == 1 and isinstance(previous.get("alerts"), list) and isinstance(previous.get("sources"), dict):
            return previous
    except (OSError, ValueError, AttributeError):
        pass
    return {"schema_version": 1, "generated_at": None, "sources": {}, "alerts": []}


def collect(path: Path, *, token: str = "", current: datetime | None = None,
            github_fetch=None, aks_fetch=None, elastic_fetch=None) -> dict:
    current = current or datetime.now(timezone.utc)
    stamp = current.isoformat(timespec="seconds").replace("+00:00", "Z")
    previous = load_existing(path)
    prior_by_source = {}
    for alert in previous["alerts"]:
        if isinstance(alert, dict) and isinstance(alert.get("source"), str):
            prior_by_source.setdefault(alert["source"], []).append(alert)
    sources, all_items = {}, []
    success = 0
    jobs = [("aks", "AKS")] + [(f"github:{repo}", product) for repo, product in REPOSITORIES.items()] + [("elastic:kibana", "Kibana"), ("elastic:elasticsearch", "Elasticsearch")]
    for key, product in jobs:
        prior_status = previous["sources"].get(key, {})
        try:
            if key == "aks":
                items = (aks_fetch or aks_advisories)(current=current)
            elif key.startswith("elastic:"):
                items = (elastic_fetch or elastic_advisories)(product, current=current)
            else:
                repo = key.removeprefix("github:")
                items = (github_fetch or github_advisories)(repo, product, token=token, current=current)
            if not isinstance(items, list):
                raise ValueError("Collector returned invalid data")
            success += 1
            sources[key] = {"product": product, "status": "ok", "checked_at": stamp, "last_success_at": stamp}
        except (ValueError, OSError, HTTPError, URLError, TimeoutError, json.JSONDecodeError, ET.ParseError) as error:
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
