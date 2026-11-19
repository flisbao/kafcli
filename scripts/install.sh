#!/usr/bin/env bash
# Downloads a kafcli release .deb from GitHub, checks its SHA-256 and installs it with apt.
#
#   curl -fsSL https://raw.githubusercontent.com/flisbao/kafcli/main/scripts/install.sh | bash
#
# Environment:
#   KAFCLI_VERSION  version to install, for example 0.0.1 (default: the latest release)
set -euo pipefail

repository="flisbao/kafcli"

fail() {
  echo "kafcli install: $*" >&2
  exit 1
}

for tool in curl sha256sum dpkg; do
  command -v "$tool" > /dev/null || fail "'$tool' is required"
done

architecture="$(dpkg --print-architecture)"
[ "$architecture" = "amd64" ] || fail "only amd64 packages are published (this system is $architecture)"

version="${KAFCLI_VERSION:-}"
if [ -z "$version" ]; then
  latest_url="$(curl -fsSLI -o /dev/null -w '%{url_effective}' "https://github.com/$repository/releases/latest")" \
    || fail "could not reach GitHub to find the latest release"
  tag="${latest_url##*/}"
  case "$tag" in
    v[0-9]*) version="${tag#v}" ;;
    *) fail "no release found at https://github.com/$repository/releases" ;;
  esac
fi
version="${version#v}"
case "$version" in
  [0-9]*.[0-9]*.[0-9]*) ;;
  *) fail "version '$version' is not of the form X.Y.Z" ;;
esac

package="kafcli_${version}_${architecture}.deb"
download_url="https://github.com/$repository/releases/download/v${version}"
work_directory="$(mktemp -d)"
trap 'rm -rf "$work_directory"' EXIT
chmod 0755 "$work_directory"

echo "Downloading kafcli $version ($architecture)..."
curl -fsSL -o "$work_directory/$package" "$download_url/$package" || fail "could not download $download_url/$package"
curl -fsSL -o "$work_directory/SHA256SUMS" "$download_url/SHA256SUMS" || fail "could not download $download_url/SHA256SUMS"

expected_line="$(grep -E "  ${package}\$" "$work_directory/SHA256SUMS" || true)"
[ -n "$expected_line" ] || fail "SHA256SUMS has no entry for $package"
(cd "$work_directory" && printf '%s\n' "$expected_line" | sha256sum --check --quiet) || fail "checksum mismatch for $package"
echo "Checksum verified."

sudo_command=""
if [ "$(id -u)" -ne 0 ]; then
  command -v sudo > /dev/null || fail "run as root or install sudo"
  sudo_command="sudo"
fi
chmod 0644 "$work_directory/$package"
$sudo_command apt-get install -y "$work_directory/$package"

echo
kafcli --version
echo "Run 'kafcli help' to get started."
