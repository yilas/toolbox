"""Offline regression tests: never depend on live GitHub or Microsoft APIs."""
import importlib.util
import json
from datetime import datetime, timezone
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

MODULE_PATH = Path(__file__).resolve().parents[1] / "scripts" / "collect_security_advisories.py"
spec = importlib.util.spec_from_file_location("security_collector", MODULE_PATH)
collector = importlib.util.module_from_spec(spec)
spec.loader.exec_module(collector)
NOW = datetime(2026, 11, 17, 12, 0, tzinfo=timezone.utc)


class CollectorTests(unittest.TestCase):
    def test_github_advisory_versions_and_severity_are_from_api(self):
        payload = [{
            "state": "published",
            "ghsa_id": "GHSA-aaaa-bbbb-cccc",
            "summary": "Test security advisory CVE-2026-12345",
            "published_at": "2026-11-16T08:00:00Z",
            "updated_at": "2026-11-16T09:00:00Z",
            "severity": "high",
            "identifiers": [{"type": "CVE", "value": "CVE-2026-12345"}],
            "vulnerabilities": [{
                "package": {"name": "example"},
                "vulnerable_version_range": "< 2.1.0",
                "first_patched_version": {"identifier": "2.1.0"}
            }]
        }]
        with patch.object(collector, "request_bytes", return_value=json.dumps(payload).encode()):
            items = collector.github_advisories("argoproj/argo-cd", "Argo CD", token="", current=NOW)
        self.assertEqual(len(items), 1)
        self.assertEqual(items[0]["id"], "GHSA-AAAA-BBBB-CCCC")
        self.assertEqual(items[0]["severity"], "high")
        self.assertEqual(items[0]["cves"], ["CVE-2026-12345"])
        self.assertEqual(items[0]["affected_versions"], ["example: < 2.1.0"])
        self.assertEqual(items[0]["patched_versions"], ["example: 2.1.0"])
        self.assertEqual(items[0]["url"], "https://github.com/argoproj/argo-cd/security/advisories/GHSA-aaaa-bbbb-cccc")

    def test_aks_markdown_bulletins_parse_official_format_and_ignore_unrelated_cves(self):
        # Representative excerpt of MicrosoftDocs/azure-aks-docs' overview.md.
        markdown = """
---
title: Security bulletins for Azure Kubernetes Service (AKS)
---
# Security bulletins for Azure Kubernetes Service (AKS)

## AKS-2026-0008 AKS advisory for CVE-2026-111497

**Published date**: November 3, 2026

### Description

CVE-2026-111497 affects the Brotli decoder. Applications can expose memory.

### Affected components

- **AKS node OS:** Node components don't decompress untrusted Brotli data.

### Resolutions

Rebuild affected container images.

---

## AKS-2026-0007 AKS advisory for CVE-2026-99999 and CVE-2026-98432

**Published Date**: November 2, 2026

### Description

This security bulletin covers two vulnerabilities in the Linux kernel.

CVE-2026-98432 and CVE-2026-99999 may affect AKS Linux nodes.

### References

- [CVE-2026-99999](https://ubuntu.com/security/CVE-2026-99999)

## AKS-2026-0006 OpenSSL end-of-life scanner findings in AKS-managed add-on images

**Published date**: October 21, 2026

### Description

Some vulnerability scanners report OpenSSL 3.3 as end of life (EOL).

The changelog contains CVE-2026-45447 as a historical backport example.

## AKS-2025-0001 Historical bulletin

**Published date**: January 1, 2025

### Description

Old bulletin outside the retention window.
"""
        with patch.object(collector, "request_bytes", return_value=markdown.encode()) as fetch:
            items = collector.aks_advisories(current=NOW)
        fetch.assert_called_once_with(collector.AKS_MARKDOWN_URL)
        self.assertEqual([item["id"] for item in items],
                         ["AKS-2026-0008", "AKS-2026-0007", "AKS-2026-0006"])
        self.assertEqual(items[0]["cves"], ["CVE-2026-111497"])
        self.assertEqual(items[1]["cves"], ["CVE-2026-98432", "CVE-2026-99999"])
        self.assertEqual(items[2]["cves"], [])  # historical examples are not AKS CVEs
        self.assertEqual(items[0]["severity"], "unknown")
        self.assertEqual(items[0]["affected_versions"], [])
        self.assertEqual(items[0]["patched_versions"], [])
        self.assertEqual(items[0]["published_at"], "2026-11-03T00:00:00Z")
        self.assertIn("Brotli decoder", items[0]["summary"])
        self.assertTrue(items[0]["url"].endswith(
            "#aks-2026-0008-aks-advisory-for-cve-2026-111497"))

    def test_aks_markdown_fails_closed_on_unexpected_structure(self):
        for source in (
            b"<html><h2>AKS-2026-0008</h2></html>",
            b"## AKS-2026-0008 Security bulletin\n\nNo publication date",
        ):
            with self.subTest(source=source):
                with patch.object(collector, "request_bytes", return_value=source):
                    with self.assertRaises(ValueError):
                        collector.aks_advisories(current=NOW)

    def test_aks_old_dated_bulletins_are_a_valid_empty_result(self):
        source = b"## AKS-2023-0001 Old bulletin\n\n**Published Date**: January 1, 2023\n"
        with patch.object(collector, "request_bytes", return_value=source):
            self.assertEqual(collector.aks_advisories(current=NOW), [])

    def test_failed_source_preserves_previous_advisories(self):
        old = {
            "schema_version": 1,
            "generated_at": "2026-11-16T12:00:00Z",
            "sources": {"aks": {"last_success_at": "2026-11-16T12:00:00Z", "status": "ok"}},
            "alerts": [{
                "id": "AKS-2026-0008", "source": "aks", "product": "AKS",
                "published_at": "2026-11-03T00:00:00Z", "title": "Existing bulletin"
            }],
        }
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / "snapshot.json"
            path.write_text(json.dumps(old), encoding="utf-8")
            result = collector.collect(
                path, current=NOW, github_fetch=lambda *args, **kwargs: [],
                aks_fetch=lambda **kwargs: (_ for _ in ()).throw(ValueError("upstream unavailable")),
                elastic_fetch=lambda *args, **kwargs: [],
            )
            self.assertEqual(len(result["alerts"]), 1)
            self.assertEqual(result["sources"]["aks"]["status"], "error")
            self.assertEqual(result["sources"]["aks"]["last_success_at"], "2026-11-16T12:00:00Z")
            self.assertEqual(json.loads(path.read_text())["alerts"][0]["id"], "AKS-2026-0008")

    def test_elastic_official_rss_product_and_severity(self):
        rss = b"""<rss><channel><item><title>Kibana 9.5.7 Security Update (ESA-2026-wwmh-mqwj)</title><link>https://discuss.elastic.co/t/example/123</link><pubDate>Mon, 16 Nov 2026 12:00:00 GMT</pubDate><description>Severity: CVSSv3.1: High (8.1) CVE ID: CVE-2026-12345</description></item></channel></rss>"""
        with patch.object(collector, "request_bytes", return_value=rss):
            items = collector.elastic_advisories("Kibana", current=NOW)
            self.assertEqual(collector.elastic_advisories("Elasticsearch", current=NOW), [])
        self.assertEqual(items[0]["id"], "ESA-2026-WWMH-MQWJ")
        self.assertEqual(items[0]["severity"], "high")
        self.assertEqual(items[0]["cves"], ["CVE-2026-12345"])
        self.assertEqual(items[0]["patched_versions"], [])
        self.assertIn("kubernetes-sigs/external-dns", collector.REPOSITORIES)
        self.assertIn("cert-manager/cert-manager", collector.REPOSITORIES)
        self.assertIn("kyverno/kyverno", collector.REPOSITORIES)

    def test_all_sources_failed_never_overwrite_snapshot(self):
        old = {"schema_version": 1, "generated_at": None, "sources": {}, "alerts": []}
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / "snapshot.json"
            original = json.dumps(old)
            path.write_text(original, encoding="utf-8")
            def fail(*args, **kwargs):
                raise ValueError("service unavailable")
            with self.assertRaises(RuntimeError):
                collector.collect(path, current=NOW, github_fetch=fail, aks_fetch=fail, elastic_fetch=fail)
            self.assertEqual(path.read_text(), original)


if __name__ == "__main__":
    unittest.main()
