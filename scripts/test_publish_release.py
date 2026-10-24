"""Publication and recovery without changing published tags."""
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

spec = importlib.util.spec_from_file_location("publish_release", Path(__file__).with_name("publish-release.py"))
publisher = importlib.util.module_from_spec(spec)
spec.loader.exec_module(publisher)


def result(code=0, data="", error=""):
    return subprocess.CompletedProcess([], code, data, error)


class PublishTest(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.notes = Path(self.directory.name) / "notes.md"
        self.notes.write_text("### Исправлено\n\n- FIN-227.\n", encoding="utf-8")
        self.repo = "fin-assist/go-idempotency"
        self.tag = "v0.4.1"

    def stable(self, **updates):
        data = dict(tagName=self.tag, isDraft=False, isPrerelease=False)
        data.update(updates)
        return result(data=json.dumps(data))

    def test_create_new_release_and_verify_existing_tag(self):
        with patch.object(publisher, "gh", return_value=result()) as gh:
            publisher.publish(self.repo, self.tag, self.notes)
        gh.assert_called_once_with("release", "create", self.tag, "--repo", self.repo, "--verify-tag",
                                   "--title", self.tag, "--notes-file", str(self.notes))

    def test_existing_or_concurrently_created_release_gets_changelog_notes(self):
        for error in ("HTTP 422 already exists", "request timed out"):
            with self.subTest(error=error), patch.object(publisher, "gh", side_effect=[
                    result(1, error=error), self.stable(), result()]) as gh:
                publisher.publish(self.repo, self.tag, self.notes)
                self.assertEqual(3, gh.call_count)
                self.assertEqual(("release", "edit", self.tag, "--repo", self.repo,
                                  "--title", self.tag, "--notes-file", str(self.notes)), gh.call_args.args)

    def test_creation_error_and_failed_inspection_do_not_succeed(self):
        with patch.object(publisher, "gh", side_effect=[result(1, error="denied"), result(1, error="network")]) as gh:
            with self.assertRaisesRegex(ValueError, "cannot inspect"):
                publisher.publish(self.repo, self.tag, self.notes)
            self.assertEqual(2, gh.call_count)

    def test_draft_prerelease_wrong_tag_or_incomplete_metadata_never_edited(self):
        cases = [self.stable(isDraft=True), self.stable(isPrerelease=True), self.stable(tagName="v0.4.0"),
                 result(data="{}"), result(data="[]"), result(data="invalid")]
        for metadata in cases:
            with self.subTest(metadata=metadata.stdout), patch.object(publisher, "gh", side_effect=[result(1), metadata]) as gh:
                with self.assertRaises(ValueError):
                    publisher.publish(self.repo, self.tag, self.notes)
                self.assertEqual(2, gh.call_count)

    def test_edit_failure_is_not_success(self):
        with patch.object(publisher, "gh", side_effect=[result(1), self.stable(), result(1, error="denied")]):
            with self.assertRaisesRegex(ValueError, "cannot update"):
                publisher.publish(self.repo, self.tag, self.notes)

    def test_invalid_tag_or_notes_fail_before_github_write(self):
        with patch.object(publisher, "gh") as gh:
            for tag in ("main", "v0.4.1;echo bad", "v01.4.1"):
                with self.assertRaises(ValueError):
                    publisher.publish(self.repo, tag, self.notes)
            self.notes.write_text(" \n")
            with self.assertRaises(ValueError):
                publisher.publish(self.repo, self.tag, self.notes)
            self.notes.unlink()
            with self.assertRaises(OSError):
                publisher.publish(self.repo, self.tag, self.notes)
            gh.assert_not_called()

    def test_command_timeout_propagates_as_failure(self):
        with patch.object(publisher, "gh", side_effect=subprocess.TimeoutExpired("gh", 60)):
            with self.assertRaises(subprocess.TimeoutExpired):
                publisher.publish(self.repo, self.tag, self.notes)

    def test_cli_recovers_existing_release_with_real_subprocesses(self):
        directory = Path(self.directory.name)
        fake = directory / "gh"
        log = directory / "calls.jsonl"
        fake.write_text(f"#!{sys.executable}\n" + '''import json, os, sys
with open(os.environ['RELEASE_CALL_LOG'], 'a') as log:
    log.write(json.dumps(sys.argv[1:]) + '\\n')
if sys.argv[2] == 'create':
    sys.exit(1)
if sys.argv[2] == 'view':
    print(json.dumps(dict(tagName='v0.4.1', isDraft=False, isPrerelease=False)))
''')
        fake.chmod(0o755)
        env = dict(os.environ, PATH=str(directory) + os.pathsep + os.environ["PATH"], RELEASE_CALL_LOG=str(log))
        command = [sys.executable, publisher.__file__, self.tag, str(self.notes), "--repo", self.repo]
        completed = subprocess.run(command, env=env, capture_output=True, text=True, timeout=10)
        self.assertEqual(0, completed.returncode, completed.stderr)
        calls = [json.loads(line) for line in log.read_text().splitlines()]
        self.assertEqual(["create", "view", "edit"], [call[1] for call in calls])
        self.assertTrue(all("--repo" in call for call in calls))
        self.notes.unlink()
        completed = subprocess.run(command, env=env, capture_output=True, text=True, timeout=10)
        self.assertEqual(1, completed.returncode)
        self.assertIn("Release publication failed", completed.stderr)
        self.assertEqual(3, len(log.read_text().splitlines()))


if __name__ == "__main__":
    unittest.main()
