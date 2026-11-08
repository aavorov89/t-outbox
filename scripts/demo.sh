#!/usr/bin/env bash
# make demo: create and capture DEMO_PAYMENTS payments through the API, wait until the
# relays and the ledger consumer caught up, then print the ledger balance and check it.
#
# Needs the stack (make up) and the three binaries: `make app`, or make run-api,
# run-relay and run-consumer in separate terminals.
#
#   DEMO_PAYMENTS (100), DEMO_CONCURRENCY (4), API_URL (http://localhost:8080)
set -euo pipefail
cd "$(dirname "$0")/.."
source scripts/lib.sh

n=${DEMO_PAYMENTS:-100}
c=${DEMO_CONCURRENCY:-4}

wait_api
log "creating and capturing $n payments ($c clients) via $API_URL"
start=$SECONDS
load "$n" "$c"
log "done in $(( SECONDS - start ))s; waiting for relays and consumer to catch up"
wait_drained 120
check_ledger
