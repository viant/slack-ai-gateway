#!/usr/bin/env bash
set -euo pipefail

release_root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
cd "$release_root"

release_version=$(cat Version)
release_version="${release_version#"${release_version%%[![:space:]]*}"}"
release_version="${release_version%"${release_version##*[![:space:]]}"}"
if [ -z "$release_version" ]; then
  printf 'Version must not be empty.\n' >&2
  exit 1
fi

release_tmp=$(mktemp -d "${TMPDIR:-/tmp}/slack-ai-gateway-build.XXXXXX")
trap 'rm -rf "$release_tmp"' EXIT

for release_target in linux/amd64 darwin/amd64 darwin/arm64 windows/amd64; do
  release_os="${release_target%/*}"
  release_arch="${release_target#*/}"
  release_dir="$release_tmp/$release_os-$release_arch"
  mkdir -p "$release_dir"
  release_binary=slack-ai-gateway
  if [ "$release_os" = windows ]; then
    release_binary=slack-ai-gateway.exe
  fi
  printf 'Building %s %s\n' "$release_target" "$release_version"
  env CGO_ENABLED=0 GOWORK=off GOOS="$release_os" GOARCH="$release_arch" \
    go build -mod=readonly -trimpath -ldflags="-X main.Version=$release_version" \
    -o "$release_dir/$release_binary" ./cmd/slack-ai-gateway
  tar -czf "$release_tmp/slack-ai-gateway_${release_os}_${release_arch}_${release_version}.tar.gz" \
    -C "$release_dir" "$release_binary"
done

cd "$release_tmp"
shasum -a 256 \
  "slack-ai-gateway_linux_amd64_${release_version}.tar.gz" \
  "slack-ai-gateway_darwin_amd64_${release_version}.tar.gz" \
  "slack-ai-gateway_darwin_arm64_${release_version}.tar.gz" \
  "slack-ai-gateway_windows_amd64_${release_version}.tar.gz" > SHA256SUMS
for release_archive in slack-ai-gateway_*.tar.gz; do
  mv "$release_archive" "$release_root/"
done
mv SHA256SUMS "$release_root/"
printf 'Release archives and SHA256SUMS written to %s\n' "$release_root"
