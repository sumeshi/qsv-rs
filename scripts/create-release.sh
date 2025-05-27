#!/bin/bash

# Quilter-CSV Release Creation Script
# Usage: ./scripts/create-release.sh <version>
# Example: ./scripts/create-release.sh v0.1.0

set -e

if [ $# -eq 0 ]; then
    echo "Usage: $0 <version>"
    echo "Example: $0 v0.1.0"
    exit 1
fi

VERSION=$1

# Validate version format
if [[ ! $VERSION =~ ^v[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
    echo "Error: Version must be in format vX.Y.Z (e.g., v0.1.0)"
    exit 1
fi

echo "Creating release for version: $VERSION"

# Check if we're on main branch
CURRENT_BRANCH=$(git branch --show-current)
if [ "$CURRENT_BRANCH" != "main" ]; then
    echo "Warning: You are not on the main branch (current: $CURRENT_BRANCH)"
    read -p "Continue anyway? (y/N): " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        exit 1
    fi
fi

# Check if working directory is clean
if [ -n "$(git status --porcelain)" ]; then
    echo "Error: Working directory is not clean. Please commit or stash your changes."
    git status --short
    exit 1
fi

# Update version in Cargo.toml
echo "Updating version in Cargo.toml..."
sed -i.bak "s/^version = \".*\"/version = \"${VERSION#v}\"/" Cargo.toml
rm Cargo.toml.bak

# Update Cargo.lock
echo "Updating Cargo.lock..."
cargo check

# Commit version bump
echo "Committing version bump..."
git add Cargo.toml Cargo.lock
git commit -m "Bump version to $VERSION"

# Create and push tag
echo "Creating and pushing tag..."
git tag -a "$VERSION" -m "Release $VERSION"
git push origin main
git push origin "$VERSION"

echo "✅ Release $VERSION created successfully!"
echo ""
echo "The GitHub Actions workflow will now:"
echo "1. Run all tests"
echo "2. Build binaries for all platforms"
echo "3. Create a GitHub release with the binaries"
echo ""
echo "You can monitor the progress at:"
echo "https://github.com/sumeshi/qsv-rs/actions"
echo ""
echo "The release will be available at:"
echo "https://github.com/sumeshi/qsv-rs/releases/tag/$VERSION" 