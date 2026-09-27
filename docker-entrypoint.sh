#!/bin/sh
set -eu

# Runs once per container start, as root (the image's default user --
# `USER waxum` was removed from the Dockerfile specifically so this step
# has permission to run). Existing deployments have volumes created by
# pre-0.11.1 images, which ran as root: `chown -R waxum` on every start
# is what lets those keep working after upgrading to a non-root image,
# without an operator having to manually fix permissions on the host.
# Cheap on repeat runs -- chown is a no-op once ownership already matches.

storage_dir="${WHATSAPP_STORAGE_PATH:-/app/whatsapp_sessions}"
mkdir -p "$storage_dir"
chown -R waxum:waxum "$storage_dir"

if [ -n "${SQLITE_PATH:-}" ]; then
    sqlite_dir=$(dirname "$SQLITE_PATH")
    mkdir -p "$sqlite_dir"
    chown -R waxum:waxum "$sqlite_dir"
fi

if [ -n "${WAXUM_SEND_LEDGER_PATH:-}" ]; then
    ledger_dir=$(dirname "$WAXUM_SEND_LEDGER_PATH")
    mkdir -p "$ledger_dir"
    chown waxum:waxum "$ledger_dir"
fi

exec gosu waxum:waxum "$@"
