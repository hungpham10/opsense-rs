#!/usr/bin/env bash
#
# bump.sh - Bump version and create git tag.
# Usage: ./bump.sh [major|minor|patch]
#
# The script:
#   1. Reads the current version from version.txt (MAJOR.MINOR.PATCH)
#   2. Increments the version according to the bump type
#   3. Writes the new version back to version.txt
#   4. Commits the change with message "chore: bump version to <new_version>"
#   5. Creates a Git tag v<new_version>
#   6. Prints the new version.

set -euo pipefail

# Determine script directory (project root)
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
VERSION_FILE="$SCRIPT_DIR/../version.txt"

# Check version file existence
if [[ ! -f "$VERSION_FILE" ]]; then
  echo "Error: version.txt not found at $VERSION_FILE" >&2
  exit 1
fi

# Validate git repository
if ! git rev-parse --git-dir > /dev/null 2>&1; then
  echo "Error: this directory is not a git repository." >&2
  exit 1
fi

# Parse current version
CURRENT_VERSION=$(cat "$VERSION_FILE")
IFS='.' read -r MAJOR MINOR PATCH <<< "$CURRENT_VERSION"

# Validate version format
if [[ -z "$MAJOR" || -z "$MINOR" || -z "$PATCH" ]]; then
  echo "Error: version.txt does not contain a valid MAJOR.MINOR.PATCH version." >&2
  exit 1
fi

# Determine bump level
case "${1:-patch}" in
  major) BUMP_LEVEL=0 ;;
  minor) BUMP_LEVEL=1 ;;
  patch) BUMP_LEVEL=2 ;;
  *) echo "Error: Invalid bump type '$1'. Use 'major', 'minor', or 'patch'." >&2; exit 1 ;;
esac

# Perform bump
case "$BUMP_LEVEL" in
  0) MAJOR=$((MAJOR + 1)); MINOR=0; PATCH=0 ;;
  1) MINOR=$((MINOR + 1)); PATCH=0 ;;
  2) PATCH=$((PATCH + 1)) ;;
esac

NEW_VERSION="${MAJOR}.${MINOR}.${PATCH}"
echo "$NEW_VERSION" > "$VERSION_FILE"
echo "Bumped version: $CURRENT_VERSION -> $NEW_VERSION"

# Git operations
git add "$VERSION_FILE"
git commit -m "chore: bump version to $NEW_VERSION"
git tag "v$NEW_VERSION"

git push origin HEAD
git push origin "v$NEW_VERSION"
echo "Version $NEW_VERSION committed, tagged, and pushed."