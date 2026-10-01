#!/bin/sh
# Seed the data directory from the baked-in profile, then run grain as the
# unprivileged user. Files already on the volume are never overwritten: after
# the first boot, edit them in place (or through /admin), and grain hot-reloads.
set -e
DATA="${GRAIN_DATA_DIR:-/data}"
mkdir -p "$DATA"
for f in /app/profile/*.yml /app/profile/*.json; do
  [ -e "$f" ] || continue
  name="$(basename "$f")"
  [ -e "$DATA/$name" ] || cp "$f" "$DATA/$name"
done
chown -R grain:grain "$DATA"
exec su-exec grain /app/grain --data-dir "$DATA"
