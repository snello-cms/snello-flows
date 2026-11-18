#!/usr/bin/env bash
# Backs up the runner's MVStore file (workflow instance state, ./data/flowRunnerData.mv) to ./backups/.
# The container is paused (not stopped) during the copy so the file is not written mid-copy; in-flight HTTP calls
# just wait a moment. Keeps the newest $KEEP copies (default 14).
# Restore: docker compose stop; cp backups/<file> data/flowRunnerData.mv; docker compose start
set -euo pipefail
cd "$(dirname "$0")"
KEEP=${KEEP:-14}
SRC=data/flowRunnerData.mv
DEST=backups/flowRunnerData-$(date +%Y%m%d-%H%M%S).mv
mkdir -p backups
[ -f "$SRC" ] || { echo "no $SRC yet (runner never started?)" >&2; exit 1; }

running=$(docker inspect -f '{{.State.Running}}' flow-runner 2>/dev/null || echo false)
echo "backup $SRC -> $DEST (flow-runner running=$running)"
if [ "$running" = true ]; then
  docker pause flow-runner >/dev/null
  trap 'docker unpause flow-runner >/dev/null; echo "flow-runner resumed"' EXIT
fi
cp --preserve=timestamps "$SRC" "$DEST"
echo "copied $(stat -c %s "$DEST") bytes"

ls -1t backups/flowRunnerData-*.mv | tail -n +$((KEEP + 1)) | while read -r old; do
  echo "removing old backup $old"; rm -f -- "$old"
done
