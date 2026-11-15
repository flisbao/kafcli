#!/usr/bin/env bash
# Generates the throwaway PKI used by the Redpanda mTLS fixture.
#
# Output (PEM, gitignored): tests/fixtures/certificates/
#   ca.pem / ca.key         self-signed certificate authority
#   server.pem / server.key broker certificate for localhost and 127.0.0.1
#   client.pem / client.key client certificate for mutual TLS
set -euo pipefail

script_directory="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
certificate_directory="${script_directory}/certificates"
mkdir -p "$certificate_directory"
cd "$certificate_directory"

certificate_validity_days=3650
server_extensions=$'basicConstraints=CA:FALSE\nkeyUsage=digitalSignature,keyEncipherment\nextendedKeyUsage=serverAuth\nsubjectAltName=DNS:localhost,IP:127.0.0.1'
client_extensions=$'basicConstraints=CA:FALSE\nkeyUsage=digitalSignature,keyEncipherment\nextendedKeyUsage=clientAuth'

openssl req -x509 -newkey rsa:2048 -nodes -days "$certificate_validity_days" \
  -keyout ca.key -out ca.pem -subj "/CN=kafcli-it-ca" \
  -addext "basicConstraints=critical,CA:TRUE" -addext "keyUsage=critical,keyCertSign,cRLSign"

sign_leaf_certificate() {
  local common_name="$1" file_prefix="$2" extensions="$3"
  printf '%s\n' "$extensions" > "${file_prefix}.ext"
  openssl req -newkey rsa:2048 -nodes -keyout "${file_prefix}.key" \
    -out "${file_prefix}.csr" -subj "/CN=${common_name}"
  openssl x509 -req -in "${file_prefix}.csr" -CA ca.pem -CAkey ca.key -CAcreateserial \
    -days "$certificate_validity_days" -extfile "${file_prefix}.ext" -out "${file_prefix}.pem"
  rm -f "${file_prefix}.csr" "${file_prefix}.ext"
}

sign_leaf_certificate "localhost" server "$server_extensions"
sign_leaf_certificate "kafcli-it-client" client "$client_extensions"
rm -f ca.srl

# The broker runs as a different user inside its container; the files are throwaway test keys.
chmod 644 ./*.key ./*.pem
echo "certificates written to ${certificate_directory}"
