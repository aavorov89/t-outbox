#!/usr/bin/env bash
# make load: the load numbers of the README, measured by cmd/loadgen against the compose
# app (api, relay-1, relay-2, ledger-consumer), which it starts and rebuilds itself.
#
#  1. API capacity: LOAD_CONCURRENCY clients create and capture payments as fast as the
#     API answers, for LOAD_DURATION; then the relays and the consumer catch up.
#  2. Steady state: LOAD_RATE payments/s (2 events each) for LOAD_DURATION, below every
#     component's capacity: the end-to-end lag of the pipeline.
#  3. Backlog drain: relays stopped, LOAD_BACKLOG payments written, then 1 relay and 1
#     consumer started; then the same with 2 relays and 3 consumers (6 partitions).
#
# Needs `make up`. LOAD_DURATION (30s), LOAD_CONCURRENCY (16), LOAD_RATE (100),
# LOAD_BACKLOG (10000).
set -euo pipefail
cd "$(dirname "$0")/.."
source scripts/lib.sh

duration=${LOAD_DURATION:-30s}
c=${LOAD_CONCURRENCY:-16}
rate=${LOAD_RATE:-100}
backlog=${LOAD_BACKLOG:-10000}
APP="$COMPOSE --profile app"
LOADGEN=./bin/loadgen

log "starting api, relay-1, relay-2, ledger-consumer"
$APP up -d --build api relay-1 relay-2 ledger-consumer
trap '$APP up -d --scale ledger-consumer=1 relay-1 relay-2 ledger-consumer >/dev/null 2>&1 || true' EXIT
wait_api
wait_drained 300
go build -o "$LOADGEN" ./cmd/loadgen

echo
echo "## 1. API capacity: $c clients, $duration, 2 relays, 1 consumer"
echo
$LOADGEN -concurrency "$c" -duration "$duration" -drain-timeout 10m

echo
echo "## 2. Steady state: $rate payments/s, $duration, 2 relays, 1 consumer"
echo
$LOADGEN -concurrency "$c" -rate "$rate" -duration "$duration"

# drain CONSUMERS RELAYS...: stop the relays, write the backlog, start RELAYS with
# CONSUMERS consumer members, report how fast both drain it.
drain() {
  local consumers=$1 since
  shift
  echo
  echo "## 3. Backlog drain: $backlog payments ($(( backlog * 2 )) events); relays: $*; consumers: $consumers"
  echo
  $APP stop relay-1 relay-2 >/dev/null 2>&1
  since=$($LOADGEN -concurrency "$c" -payments "$backlog" -wait=false | sed -n 's/.*-report-since \([0-9]*\).*/\1/p')
  $APP up -d --no-recreate --scale ledger-consumer="$consumers" ledger-consumer >/dev/null 2>&1
  $APP start "$@" >/dev/null 2>&1
  $LOADGEN -report-since "$since" -drain-timeout 10m
}
drain 1 relay-1
drain 3 relay-1 relay-2
