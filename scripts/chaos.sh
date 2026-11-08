#!/usr/bin/env bash
# make chaos: SIGKILL relay containers in a loop while payments are created and captured,
# then check that the ledger is exact: every capture posted once, every transaction
# balanced. Kills that land between Kafka's ack and the outbox COMMIT re-publish events
# (F5); the summary counts those duplicates, which the consumer skips.
#
# Needs `make up`. Starts the app profile (api, relay-1, relay-2, ledger-consumer) itself.
#
#   CHAOS_PAYMENTS (2000), CHAOS_CONCURRENCY (8),
#   CHAOS_TARGETS ("relay-1 relay-2"; add ledger-consumer to kill it as well)
set -euo pipefail
cd "$(dirname "$0")/.."
source scripts/lib.sh

n=${CHAOS_PAYMENTS:-2000}
c=${CHAOS_CONCURRENCY:-8}
read -r -a targets <<<"${CHAOS_TARGETS:-relay-1 relay-2}"
APP="$COMPOSE --profile app"

log "starting api, relay-1, relay-2, ledger-consumer"
$APP up -d --build api relay-1 relay-2 ledger-consumer
trap '$APP start "${targets[@]}" >/dev/null 2>&1 || true' EXIT # never leave a target dead
wait_api
wait_drained 120 # leftovers of earlier runs, so the counts below are this run's

# Container ids (stable across kill/start): plain docker is much faster than compose here.
declare -A ctr
for t in "${targets[@]}"; do ctr[$t]=$($APP ps -q "$t"); done

records_before=$(kafka_records)
events_before=$(sql "SELECT count(*) FROM outbox")

log "load: $n payments, $c clients; killing ${targets[*]} at random"
load "$n" "$c" &
load_pid=$!
kills=0
while kill -0 "$load_pid" 2>/dev/null; do
  sleep "0.$(( RANDOM % 9 + 1 ))"
  t=${targets[RANDOM % ${#targets[@]}]}
  docker kill -s KILL "${ctr[$t]}" >/dev/null
  kills=$(( kills + 1 ))
  sleep "0.$(( RANDOM % 5 + 1 ))"
  docker start "${ctr[$t]}" >/dev/null
done
if ! wait "$load_pid"; then
  log "load failed (API errors)"
  exit 1
fi
$APP start "${targets[@]}" >/dev/null 2>&1

log "load done after $kills kills; waiting for relays and consumer to catch up"
wait_drained 180
events=$(( $(sql "SELECT count(*) FROM outbox") - events_before ))
records=$(( $(kafka_records) - records_before ))

echo
echo "SIGKILLs:                $kills (${targets[*]})"
echo "Outbox events:           $events"
echo "Kafka records:           $records"
echo "Duplicates (re-published, skipped by the consumer): $(( records - events ))"
echo
if check_ledger; then
  log "OK: the ledger is exact"
else
  log "FAIL: ledger invariant broken"
  exit 1
fi
