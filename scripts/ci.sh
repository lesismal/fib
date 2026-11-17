#!/usr/bin/env bash
# Runs CI on a commit: CI no longer runs on a push to main (see the comment
# at the top of .github/workflows/ci.yml), so this tags the commit ci-* (or
# ci-quick-* for the quick set) and pushes the tag, which starts the run.
# The commit must be pushed already. Only the last ten ci-* tags are kept,
# on the remote and here; older ones are deleted.
#
#   scripts/ci.sh            full run on HEAD
#   scripts/ci.sh quick      quick run (lint, build, Linux tests) on HEAD
#   scripts/ci.sh [quick] REV
set -euo pipefail

keep=10
remote=${CI_REMOTE:-origin}
prefix=ci
if [ "${1:-}" = quick ]; then
	prefix=ci-quick
	shift
fi
rev=$(git rev-parse --verify "${1:-HEAD}^{commit}")
short=$(git rev-parse --short "$rev")

if ! git branch -r --contains "$rev" | grep -q "^ *$remote/"; then
	echo "ci.sh: $short is not on $remote yet; push it first" >&2
	exit 1
fi

tag="$prefix-$(date -u +%Y%m%d-%H%M%S)-$short"
git tag "$tag" "$rev"
git push -q "$remote" "refs/tags/$tag"
echo "pushed $tag"

# Old ci-* tags (ci-quick-* among them), oldest first by the time in the name.
old=$(git ls-remote --tags --refs "$remote" 'ci-*' |
	sed 's|.*refs/tags/||' |
	awk '{ t = $0; sub(/^ci-(quick-)?/, "", t); print t "\t" $0 }' |
	sort | cut -f2 |
	awk -v k="$keep" '{ a[NR] = $0 } END { for (i = 1; i <= NR - k; i++) print a[i] }')
if [ -n "$old" ]; then
	# shellcheck disable=SC2086
	git push -q "$remote" $(printf ':refs/tags/%s ' $old)
	# shellcheck disable=SC2086
	git tag -d $old >/dev/null 2>&1 || true
	echo "deleted $(echo "$old" | wc -l | tr -d ' ') old ci tag(s)"
fi

if command -v gh >/dev/null 2>&1; then
	repo=$(gh repo view --json nameWithOwner -q .nameWithOwner 2>/dev/null || true)
	[ -n "$repo" ] && echo "https://github.com/$repo/actions/workflows/ci.yml?query=branch%3A$tag"
fi
