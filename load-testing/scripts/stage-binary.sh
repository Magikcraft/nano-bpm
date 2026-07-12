#!/bin/bash
# stage-binary.sh — fan a freshly built server binary out to all 3 nodes as
# ~/nano-gw-new (deploy.sh then atomically swaps nano-gw-new -> nano-gw on restart).
#
# The binary must be built on Linux (the cluster is x86_64-linux). It is normally
# built on the build host (node0) by build-server.sh --stage ~/nano-gw-new. node0
# lacks the peer ssh key, so we PULL it to the loadbox first, then PUSH to each node.
#
# Usage: stage-binary.sh [--from <src>]
#   --from node0            (default) pull /home/joshua.wulf/nano-gw-new off node0
#   --from <loadbox-path>   stage a binary that already lives on the loadbox
set -eu
SSHK="-i $HOME/.ssh/google_compute_engine -o StrictHostKeyChecking=no -o ConnectTimeout=10"
NODES="10.128.0.19 10.128.0.20 10.128.0.18"
BUILD_HOST="10.128.0.19"
SRC="node0"
while [ $# -gt 0 ]; do
  case "$1" in
    --from) SRC="$2"; shift 2 ;;
    -h|--help) sed -n '2,12p' "$0"; exit 0 ;;
    *) echo "unknown arg: $1" >&2; exit 2 ;;
  esac
done

LOCAL="/tmp/nano-gw-new.$$"
if [ "$SRC" = "node0" ]; then
  echo "=== pulling nano-gw-new off build host ($BUILD_HOST) ==="
  scp $SSHK "$BUILD_HOST:~/nano-gw-new" "$LOCAL"
else
  cp -f "$SRC" "$LOCAL"
fi
SHA=$(sha256sum "$LOCAL" | cut -c1-16)
echo "=== staging binary sha $SHA to all nodes ==="
for ip in $NODES; do
  scp $SSHK "$LOCAL" "$ip:~/nano-gw-new" >/dev/null
  got=$(ssh $SSHK "$ip" "sha256sum ~/nano-gw-new | cut -c1-16")
  [ "$got" = "$SHA" ] && echo "  node-${ip##*.}: OK ($got)" || echo "  node-${ip##*.}: MISMATCH ($got != $SHA)"
done
rm -f "$LOCAL"
echo "=== STAGE DONE (deploy.sh will swap nano-gw-new -> nano-gw) ==="
