#!/bin/sh
# A dedicated database for the test suite.
#
# The integration harness TRUNCATEs every table between tests. Pointing it at the
# application database would silently wipe the state of a running worker mid-run
# -- which looks like a bewildering flake rather than the self-inflicted wound it
# is. Separate databases make the two uses independent.
set -e
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<SQL
    CREATE DATABASE chainrail_test OWNER $POSTGRES_USER;
SQL
echo "created database chainrail_test"
