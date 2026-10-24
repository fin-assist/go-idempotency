#!/usr/bin/env bash
# Validate the checked-out immutable tag before publishing any GitHub release.
set -euo pipefail
TAG=${1:?usage: validate-release.sh vX.Y.Z}
if ! [[ "$TAG" =~ ^v(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)$ ]]; then
  echo "$TAG is not vX.Y.Z"
  exit 1
fi
major=${BASH_REMATCH[1]}
module=$(sed -n 's/^module //p' go.mod)
if [ "$major" -ge 2 ] && [[ "$module" != */v"$major" ]]; then
  echo "$TAG needs the module path to end in /v$major, go.mod has $module"
  exit 1
fi
if [ "$major" -lt 2 ] && [[ "$module" =~ /v[0-9]+$ ]]; then
  echo "$TAG is v$major, but go.mod has the major suffix in $module"
  exit 1
fi
git fetch -q origin '+refs/heads/main:refs/remotes/origin/main'
tagged=$(git rev-parse "refs/tags/$TAG^{commit}")
if [ "$(git rev-parse HEAD)" != "$tagged" ] || ! git merge-base --is-ancestor "$tagged" origin/main; then
  echo "$TAG does not point to the checked-out commit on main"
  exit 1
fi
python3 scripts/changelog.py check
if [ "$(python3 scripts/changelog.py latest)" != "$TAG" ]; then
  echo "The newest section of CHANGELOG.md is not $TAG: merge the release PR first"
  exit 1
fi
python3 scripts/changelog.py notes "$TAG" > notes.md
