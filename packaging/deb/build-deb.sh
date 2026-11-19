#!/usr/bin/env bash
# Builds the kafcli Debian package from an already compiled binary.
#
# Usage: packaging/deb/build-deb.sh <binary> <version> <architecture> <output directory>
# Example: packaging/deb/build-deb.sh target/x86_64-unknown-linux-musl/release/kafcli 0.0.1 amd64 dist
#
# The binary is statically linked, so the package declares no runtime dependencies.
set -euo pipefail

if [ "$#" -ne 4 ]; then
  echo "usage: $0 <binary> <version> <architecture> <output directory>" >&2
  exit 1
fi

binary="$1"
version="$2"
architecture="$3"
output_directory="$4"
repository_root="$(cd "$(dirname "$0")/../.." && pwd)"

if [ ! -x "$binary" ]; then
  echo "binary '$binary' does not exist or is not executable" >&2
  exit 1
fi
case "$version" in
  [0-9]*.[0-9]*.[0-9]*) ;;
  *) echo "version '$version' is not of the form X.Y.Z" >&2; exit 1 ;;
esac

staging="$(mktemp -d)"
trap 'rm -rf "$staging"' EXIT
package_root="$staging/kafcli_${version}_${architecture}"

install -D -m 0755 "$binary" "$package_root/usr/bin/kafcli"
install -D -m 0644 "$repository_root/README.md" "$package_root/usr/share/doc/kafcli/README.md"
{
  echo "Format: https://www.debian.org/doc/packaging-manuals/copyright-format/1.0/"
  echo "Upstream-Name: kafcli"
  echo "Source: https://github.com/flisbao/kafcli"
  echo
  echo "Files: *"
  echo "Copyright: 2026 Felipe Lisbão"
  echo "License: MIT or Apache-2.0"
  echo
  echo "License: MIT"
  sed 's/^$/./; s/^/ /' "$repository_root/LICENSE-MIT"
  echo
  echo "License: Apache-2.0"
  echo " On Debian systems, the full text of the Apache License 2.0 is in"
  echo " /usr/share/common-licenses/Apache-2.0."
} > "$staging/copyright"
install -D -m 0644 "$staging/copyright" "$package_root/usr/share/doc/kafcli/copyright"

installed_size_kib="$(du -sk "$package_root/usr" | cut -f1)"
mkdir -p "$package_root/DEBIAN"
cat > "$package_root/DEBIAN/control" <<CONTROL
Package: kafcli
Version: ${version}
Architecture: ${architecture}
Maintainer: Felipe Lisbão <flisbao@gmail.com>
Installed-Size: ${installed_size_kib}
Section: utils
Priority: optional
Homepage: https://github.com/flisbao/kafcli
Description: command-line client for Apache Kafka and Azure Event Hubs
 kafcli produces and consumes messages (JSON and Avro, with Confluent and
 Azure schema registries), shows consumer lag, audits consumer groups and
 resets offsets with dry-run, backups and confirmation for protected contexts.
CONTROL

mkdir -p "$output_directory"
package_file="$output_directory/kafcli_${version}_${architecture}.deb"
dpkg-deb --root-owner-group --build "$package_root" "$package_file" >/dev/null
echo "$package_file"
