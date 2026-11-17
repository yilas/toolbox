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
        self.assertEqual(items[0]["url"], "https://github.com/argoproj/argo-cd/security/advisories/GHSA-AAAA-BBBB-CCCC")

    def test_aks_bulletins_keep_unknown_severity_and_do_not_infer_versions(self):
        html = """
        <main>
          <h2 id="aks-2026-0008-test">AKS-2026-0008 AKS advisory for CVE-2026-111497</h2>
          <p>Published date: November 3, 2026</p>
          <h3>Description</h3>
          <p>Applications using vulnerable Brotli decoders should be rebuilt.</p>
          <h3>Affected versions</h3><p>Some images may be affected.</p>
          <h2 id="aks-2025-0001-old">AKS-2025-0001 Historical bulletin</h2>
          <p>Published date: January 1, 2025</p>
        </main>
        """
        with patch.object(collector, "request_bytes", return_value=html.encode()):
            items = collector.aks_advisories(current=NOW)
        self.assertEqual(len(items), 1)
        self.assertEqual(items[0]["id"], "AKS-2026-0008")
        self.assertEqual(items[0]["cves"], ["CVE-2026-111497"])
        self.assertEqual(items[0]["severity"], "unknown")
        self.assertEqual(items[0]["affected_versions"], [])
        self.assertEqual(items[0]["patched_versions"], [])
        self.assertTrue(items[0]["url"].endswith("#aks-2026-0008-test"))

    def test_aks_parser_fails_closed_when_document_structure_changes(self):
        with patch.object(collector, "request_bytes", return_value=b"<html><h2>General page</h2></html>"):
            with self.assertRaises(ValueError):
                collector.aks_advisories(current=NOW)

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
            )
            self.assertEqual(len(result["alerts"]), 1)
            self.assertEqual(result["sources"]["aks"]["status"], "error")
            self.assertEqual(result["sources"]["aks"]["last_success_at"], "2026-11-16T12:00:00Z")
            self.assertEqual(json.loads(path.read_text())["alerts"][0]["id"], "AKS-2026-0008")

    def test_all_sources_failed_never_overwrite_snapshot(self):
        old = {"schema_version": 1, "generated_at": None, "sources": {}, "alerts": []}
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / "snapshot.json"
            original = json.dumps(old)
            path.write_text(original, encoding="utf-8")
            def fail(*args, **kwargs):
                raise ValueError("service unavailable")
            with self.assertRaises(RuntimeError):
                collector.collect(path, current=NOW, github_fetch=fail, aks_fetch=fail)
            self.assertEqual(path.read_text(), original)


if __name__ == "__main__":
    unittest.main()
