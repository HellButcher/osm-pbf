#!/bin/bash
set -e

REMOTE=https://github.com/openstreetmap/OSM-binary.git
TARGET_PREFIX=src/protos

git remote add --no-tags osm-binary $REMOTE || true
git fetch --depth=1 osm-binary HEAD:osm-binary-main
git co osm-binary-main
git subtree split --prefix=src/ -b osm-binary-split-src
git co osm-binary-split-src
git branch -D osm-binary-main

FILTER_BRANCH_SQUELCH_WARNING=1 git filter-branch --prune-empty --index-filter '\
  git ls-files | \
  grep -Fiv -e LICENSE -e .proto | \
  xargs -r git rm --cached --ignore-unmatch'
rm -f .git/refs/original/refs/heads/osm-binary-split-src
git co main
if [ -d "$TARGET_PREFIX" ]; then
  MESSAGE="update $TARGET_PREFIX from $REMOTE"
  git subtree merge --squash --prefix "$TARGET_PREFIX" osm-binary-split-src -m "$MESSAGE"
else
  MESSAGE="add $TARGET_PREFIX from $REMOTE"
  git subtree add --squash --prefix "$TARGET_PREFIX" osm-binary-split-src -m "$MESSAGE"
fi
git branch -D osm-binary-split-src
