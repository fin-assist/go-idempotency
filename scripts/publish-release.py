#!/usr/bin/env python3
"""Create release notes, or reconcile an existing stable release of the same tag."""

import argparse
import json
from pathlib import Path
import re
import subprocess
import sys


def gh(*args):
    return subprocess.run(["gh", *args], capture_output=True, text=True, timeout=60)


def publish(repository, tag, notes):
    if not re.fullmatch(r"v(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)", tag):
        raise ValueError("release tag must be vX.Y.Z")
    if not notes.read_text(encoding="utf-8").strip():
        raise ValueError("release notes must not be empty")
    created = gh("release", "create", tag, "--repo", repository, "--verify-tag",
                 "--title", tag, "--notes-file", str(notes))
    if created.returncode == 0:
        return
    # A manual release or a concurrent/repeated run may already have created it. Reconcile
    # only a confirmed stable release; failed requests never count as successful publication.
    viewed = gh("release", "view", tag, "--repo", repository,
                "--json", "tagName,isDraft,isPrerelease")
    if viewed.returncode != 0:
        raise ValueError(f"release creation failed: {created.stderr.strip()}; "
                         f"cannot inspect an existing release: {viewed.stderr.strip()}")
    release = json.loads(viewed.stdout)
    if (not isinstance(release, dict) or release.get("tagName") != tag
            or release.get("isDraft") is not False or release.get("isPrerelease") is not False):
        raise ValueError("existing release must be published, stable and match the requested tag")
    edited = gh("release", "edit", tag, "--repo", repository,
                "--title", tag, "--notes-file", str(notes))
    if edited.returncode != 0:
        raise ValueError(f"cannot update release notes: {edited.stderr.strip()}")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("tag")
    parser.add_argument("notes", type=Path)
    parser.add_argument("--repo", required=True)
    args = parser.parse_args()
    try:
        publish(args.repo, args.tag, args.notes)
    except (OSError, ValueError, subprocess.SubprocessError) as error:
        print(f"Release publication failed: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
