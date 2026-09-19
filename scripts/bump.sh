#!/usr/bin/env bash
#
# bump.sh - Bump version and create git tag (auto-fallback to latest tag).
# Usage: ./bump.sh [major|minor|patch]
#
# The script:
#   1. Determines the current version:
#        • If a `version.txt` file exists at the repository root, reads it.
#        • Otherwise, uses the most recent git tag (e.g. v1.2.3) as the current version.
#   2. Increments the version according to the bump type.
#   3. Writes the new version back to `version.txt` (creating the file if needed).
#   4. Commits the change with message "chore: bump version to <new_version>".
#   5. Creates a Git tag v<new_version>.
#   6. Pushes both the commit and the tag to the remote.
#   7. Prints the new version.

set -euo pipefail

# Determine script directory (project root)
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# Path to version file (repository root)
VERSION_FILE="$SCRIPT_DIR/version.txt"

# ------------------------------------------------------------
# Resolve current version
# ------------------------------------------------------------
if [[ -f "$VERSION_FILE" ]]; then
    # Use version.txt if it exists
    CURRENT_VERSION=$(cat "$VERSION_FILE")
else
    # Fallback: get the latest tag that looks like a version
    LATEST_TAG=$(git describe --tags --abbrev=0 2>/dev/null || true)
    if [[ -z "$LATEST_TAG" ]]; then
        echo "Error: No version.txt found and no git tags available." >&2
        exit 1
    fi
    # Strip a leading 'v' if present (e.g. v1.2.3 -> 1.2.3)
    CURRENT_VERSION="${LATEST_TAG#v}"
fi

# Validate version format (MAJOR.MINOR.PATCH)
if [[ -z "$CURRENT_VERSION" || "$CURRENT_VERSION" != *[0-9]* ]]; then
    echo "Error: Unable to parse a valid version from '$CURRENT_VERSION'." >&2
    exit 1
fi

IFS='.' read -r MAJOR MINOR PATCH <<< "$CURRENT_VERSION"

# Validate that all parts are numeric
if [[ -z "$MAJOR" || -z "$MINOR" || -z "$PATCH" ]]; then
    echo "Error: version '$CURRENT_VERSION' is not in MAJOR.MINOR.PATCH format." >&2
    exit 1
fi

# ------------------------------------------------------------
# Determine bump level
# ------------------------------------------------------------
case "${1:-patch}" in
    major) BUMP_LEVEL=0 ;;
    minor) BUMP_LEVEL=1 ;;
    patch) BUMP_LEVEL=2 ;;
    *) echo "Error: Invalid bump type '$1'. Use 'major', 'minor', or 'patch'." >&2; exit 1 ;;
esac

# ------------------------------------------------------------
# Perform bump
# ------------------------------------------------------------
case "$BUMP_LEVEL" in
    0) MAJOR=$((MAJOR + 1)); MINOR=0; PATCH=0 ;;
    1) MINOR=$((MINOR + 1)); PATCH=0 ;;
    2) PATCH=$((PATCH + 1)) ;;
esac

NEW_VERSION="${MAJOR}.${MINOR}.${PATCH}"
echo "$NEW_VERSION" > "$VERSION_FILE"
echo "Bumped version: $CURRENT_VERSION -> $NEW_VERSION"

# ------------------------------------------------------------
# Git operations
# ------------------------------------------------------------
git add "$VERSION_FILE"
git commit -m "chore: bump version to $NEW_VERSION"
git tag "v$NEW_VERSION"

# Push commit and tag to remote
git push origin HEAD
git push origin "v$NEW_VERSION"

echo "Version $NEW_VERSION committed, tagged, and pushed."