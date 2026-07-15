#!/usr/bin/env bash

# Configure passwordless SSH from node0 to all experiment nodes.
# CloudLab does not enable inter-node SSH by default; use the experiment
# key from geni-get, which is the supported cluster-wide key pair.

set -euo pipefail

source "$(cd "$(dirname "$0")" && pwd)/common.sh"

KEY="${DDRT_SSH_KEY:-$HOME/.ssh/id_rsa}"

if [[ ! -x "$(command -v geni-get)" ]]; then
  echo "geni-get not found; this script must run on a CloudLab node." >&2
  exit 1
fi

mkdir -p "$HOME/.ssh"
chmod 700 "$HOME/.ssh"

if [[ ! -f "$KEY" ]]; then
  echo "Installing experiment SSH key at $KEY ..."
  geni-get key >"$KEY"
  chmod 600 "$KEY"
fi

PUB="${KEY}.pub"
ssh-keygen -y -f "$KEY" >"$PUB"
chmod 644 "$PUB"

if ! grep -qF "$(cat "$PUB")" "$HOME/.ssh/authorized_keys" 2>/dev/null; then
  echo "Adding experiment public key to ~/.ssh/authorized_keys ..."
  cat "$PUB" >>"$HOME/.ssh/authorized_keys"
  chmod 600 "$HOME/.ssh/authorized_keys"
fi

cat >"$HOME/.ssh/config" <<EOF
Host node* 10.10.1.*
  User ${SSH_USER}
  IdentityFile ${KEY}
  IdentitiesOnly yes
  StrictHostKeyChecking accept-new
EOF
chmod 600 "$HOME/.ssh/config"

load_hosts
echo "Testing SSH from node0 to a few cluster nodes ..."
for host in "$CLIENT_HOST" "${STORAGE_HOSTS[@]:0:3}"; do
  printf '  %-16s -> ' "$host"
  if ssh -i "$KEY" -o BatchMode=yes -o ConnectTimeout=10 "${SSH_USER}@${host}" hostname; then
    :
  else
    echo "FAILED"
    echo
    echo "If this still fails, terminate and recreate the experiment after adding"
    echo "your SSH public key in the CloudLab portal (Profile -> Manage SSH Keys)."
    exit 1
  fi
done

echo
echo "SSH setup complete. Next:"
echo "  cd ~/DdlRT"
echo "  ./cloudlab/deploy.sh"
echo "  ./cloudlab/start.sh"
