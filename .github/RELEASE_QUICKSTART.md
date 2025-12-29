# Quick Release Guide

## Create a New Release

```bash
# 1. Update to latest main
git checkout main
git pull

# 2. Create and push a tag
git tag -a v1.0.0 -m "Release v1.0.0"
git push origin v1.0.0
```

## What Happens Next

The GitHub Actions workflow will automatically build binaries for linux/amd64 and linux/arm64, 
generate release notes with contributor mentions, and publish a GitHub release with all assets.

## Release Assets

Each release includes:
- `pgremapper-{version}-linux-amd64` (binary)
- `pgremapper-{version}-linux-arm64` (binary)
