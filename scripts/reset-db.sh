#!/usr/bin/env bash
# Drop and recreate the schema. Destroys all local data.
set -euo pipefail
DB_URL="${TEST_DATABASE_URL:-postgres://chainrail:chainrail@127.0.0.1:55432/chainrail}"
read -r -p "This will DELETE ALL DATA in $DB_URL. Continue? [y/N] " confirm
[[ "$confirm" == "y" || "$confirm" == "Y" ]] || { echo "aborted"; exit 1; }
psql "$DB_URL" -v ON_ERROR_STOP=1 -c 'DROP SCHEMA public CASCADE; CREATE SCHEMA public;'
echo "schema reset; migrations will re-run on next boot"
