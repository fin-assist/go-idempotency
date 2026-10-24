"""Tag ancestry and pre-publication refusal in a real disposable Git repository."""
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest

SCRIPTS = Path(__file__).resolve().parent


class ValidationTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.repo = self.root / "repo"
        self.repo.mkdir()
        self.command(["git", "init", "--bare", str(self.root / "origin.git")])
        self.git("init", "-b", "main")
        self.git("config", "user.email", "release-test@example.invalid")
        self.git("config", "user.name", "Release test")
        self.git("remote", "add", "origin", str(self.root / "origin.git"))
        (self.repo / "go.mod").write_text("module example.invalid/library\n\ngo 1.26.0\n")
        (self.repo / "scripts").mkdir()
        shutil.copy(SCRIPTS / "changelog.py", self.repo / "scripts/changelog.py")
        self.changelog("v0.4.1")
        self.commit("Old release")
        self.git("tag", "v0.4.1")
        self.old = self.git("rev-parse", "HEAD").stdout.strip()
        (self.repo / "later.txt").write_text("Later main commit")
        self.commit("Advance main")
        self.main = self.git("rev-parse", "HEAD").stdout.strip()
        self.git("push", "origin", "main")
        self.marker = self.root / "published"

    def command(self, args, **kwargs):
        return subprocess.run(args, cwd=self.repo, capture_output=True, text=True, timeout=10, **kwargs)

    def git(self, *args):
        return self.command(["git", *args], check=True)

    def commit(self, message):
        self.git("add", ".")
        self.git("commit", "-m", message)

    def changelog(self, version):
        (self.repo / "CHANGELOG.md").write_text(
            f"# Changes\n\n## [Unreleased]\n\n## [{version}] — 2026-10-24\n\n- Notes for {version}.\n")

    def validate_then_publish(self, tag):
        # Match Actions: every validation failure stops the step before publication. GITHUB_SHA
        # deliberately points to newer main, as it does for workflow_dispatch from main.
        env = dict(os.environ, TAG=tag, GITHUB_SHA=self.main,
                   VALIDATOR=str(SCRIPTS / "validate-release.sh"), PUBLISH_MARKER=str(self.marker))
        return self.command(["bash", "-e", "-c",
                             'bash "$VALIDATOR" "$TAG"; printf published > "$PUBLISH_MARKER"'], env=env)

    def test_old_tag_on_main_ancestor_is_accepted_and_its_notes_used(self):
        self.git("checkout", "--detach", "v0.4.1")
        result = self.validate_then_publish("v0.4.1")
        self.assertEqual(0, result.returncode, result.stdout + result.stderr)
        self.assertTrue(self.marker.exists())
        self.assertEqual("- Notes for v0.4.1.\n", (self.repo / "notes.md").read_text())
        self.assertEqual(self.old, self.git("rev-parse", "v0.4.1").stdout.strip())

    def test_side_branch_tag_is_rejected_even_when_github_sha_is_main(self):
        self.git("checkout", "-b", "side", self.old)
        self.changelog("v0.4.2")
        self.commit("Unmerged release")
        self.git("tag", "v0.4.2")
        self.git("checkout", "--detach", "v0.4.2")
        result = self.validate_then_publish("v0.4.2")
        self.assertNotEqual(0, result.returncode)
        self.assertIn("on main", result.stdout)
        self.assertFalse(self.marker.exists())
        self.assertFalse((self.repo / "notes.md").exists())

    def test_checkout_must_match_the_requested_tag(self):
        result = self.validate_then_publish("v0.4.1")
        self.assertNotEqual(0, result.returncode)
        self.assertFalse(self.marker.exists())

    def test_tag_without_matching_changelog_is_rejected(self):
        self.git("tag", "v0.4.0")
        result = self.validate_then_publish("v0.4.0")
        self.assertNotEqual(0, result.returncode)
        self.assertIn("newest section", result.stdout)
        self.assertFalse(self.marker.exists())

    def test_invalid_tag_and_major_module_path_are_rejected(self):
        for tag in ("main", "v2.0.0"):
            with self.subTest(tag=tag):
                result = self.validate_then_publish(tag)
                self.assertNotEqual(0, result.returncode)
                self.assertFalse(self.marker.exists())


if __name__ == "__main__":
    unittest.main()
