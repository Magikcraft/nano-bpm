#!/usr/bin/env bash
# list-builds.sh — show hash-tagged builds under ~/builds (newest first).
set -euo pipefail
BUILDS="${1:-$HOME/builds}"
[ -d "$BUILDS" ] || { echo "(no builds yet at $BUILDS)"; exit 0; }
printf "%-14s %-16s %-11s %-20s %s\n" SHORT BIN_SHA16 PROFILE COMMIT_DATE SUBJECT
for d in $(ls -1dt "$BUILDS"/*/ 2>/dev/null); do
  m="${d}metadata.json"; b="${d}nano-gw"
  [ -f "$m" ] || continue
  get(){ grep -o "\"$1\": *\"[^\"]*\"" "$m" | head -1 | sed 's/.*: *"//; s/"$//'; }
  short=$(get short); bsha=$(get binary_sha256_16); prof=$(get profile)
  cdate=$(get commit_date | cut -c1-19); subj=$(get subject)
  [ -x "$b" ] || short="$short(MISSING)"
  printf "%-14s %-16s %-11s %-20s %s\n" "$short" "$bsha" "$prof" "$cdate" "$subj"
done
