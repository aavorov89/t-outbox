# shellcheck shell=bash
# Shared by demo.sh and chaos.sh. Talks to the `make up` stack through docker compose, so
# neither psql nor rpk has to be installed locally.

COMPOSE=${COMPOSE:-docker compose -f deploy/docker-compose.yml --env-file deploy/.env}
API_URL=${API_URL:-http://localhost:8080}
TOPIC=${KAFKA_TOPIC:-payments.events}

log() { printf '[%s] %s\n' "$(date +%T)" "$*" >&2; }

# sql QUERY: one value per line, no headers.
sql() { $COMPOSE exec -T postgres psql -U outbox -d outbox -v ON_ERROR_STOP=1 -qAtc "$1"; }

new_uuid() { uuidgen 2>/dev/null || cat /proc/sys/kernel/random/uuid; }

# wait_api: the API answers (404 for an unknown payment is an answer).
wait_api() {
  for _ in $(seq 1 60); do
    if curl -s -o /dev/null "$API_URL/payments/$(new_uuid)"; then return 0; fi
    sleep 1
  done
  log "API at $API_URL did not come up"
  return 1
}

# one_payment MERCHANT AMOUNT: create, then capture. Prints the payment id.
one_payment() {
  local body id
  body=$(curl -fsS "$API_URL/payments" -H 'Content-Type: application/json' \
    -H "Idempotency-Key: $(new_uuid)" \
    -d "{\"merchant_id\":\"$1\",\"amount\":{\"amount_minor\":$2,\"currency\":\"EUR\"}}") || return 1
  id=$(sed -E 's/.*"id":"([^"]+)".*/\1/' <<<"$body")
  curl -fsS -o /dev/null -X POST "$API_URL/payments/$id/capture" -H "Idempotency-Key: $(new_uuid)" || return 1
  echo "$id"
}

# load N CONCURRENCY: create and capture N payments with CONCURRENCY parallel clients.
load() {
  local n=$1 c=$2 w per rc=0
  local -a pids=()
  for w in $(seq 1 "$c"); do
    per=$(( n / c + (w <= n % c ? 1 : 0) ))
    (
      for _ in $(seq 1 "$per"); do
        one_payment "m_$(( RANDOM % 5 + 1 ))" "$(( RANDOM % 9900 + 100 ))" >/dev/null || exit 1
      done
    ) &
    pids+=($!)
  done
  for w in "${pids[@]}"; do wait "$w" || rc=1; done
  return $rc
}

# wait_drained [TIMEOUT_S]: every outbox row is published and processed by the ledger.
wait_drained() {
  local timeout=${1:-120} pending unprocessed
  for _ in $(seq 1 "$timeout"); do
    pending=$(sql "SELECT count(*) FROM outbox WHERE published_at IS NULL")
    unprocessed=$(sql "SELECT count(*) FROM outbox o WHERE NOT EXISTS (
      SELECT 1 FROM processed_events p WHERE p.consumer = 'ledger' AND p.event_id = o.event_id)")
    if [[ $pending == 0 && $unprocessed == 0 ]]; then return 0; fi
    sleep 1
  done
  log "not drained after ${timeout}s: $pending unpublished, $unprocessed unprocessed"
  return 1
}

# kafka_records: records in the topic (sum of high watermarks over the partitions).
kafka_records() {
  $COMPOSE exec -T redpanda rpk topic describe "$TOPIC" -p |
    awk 'NR == 1 { for (i = 1; i <= NF; i++) if ($i == "HIGH-WATERMARK") c = i; next } { s += $c } END { print s + 0 }'
}

# check_ledger: prints the balance and fails if any invariant is broken.
#  - every ledger transaction sums to zero per currency
#  - every captured payment has exactly one ledger transaction (none lost, none doubled)
#  - psp:receivable equals the captured amounts, per currency
check_ledger() {
  local unbalanced wrong mismatch
  unbalanced=$(sql "SELECT count(*) FROM (SELECT transaction_id, currency FROM ledger_entries
    GROUP BY 1, 2 HAVING sum(amount_minor) <> 0) t")
  wrong=$(sql "SELECT count(*) FROM payments p WHERE p.status = 'captured' AND (
    SELECT count(DISTINCT e.transaction_id) FROM outbox o JOIN ledger_entries e ON e.event_id = o.event_id
    WHERE o.aggregate_id = p.id::text AND o.event_type = 'payment.captured') <> 1")
  mismatch=$(sql "SELECT count(*) FROM (
    (SELECT currency, sum(amount_minor) FROM payments WHERE status = 'captured' GROUP BY 1
     EXCEPT SELECT currency, sum(amount_minor) FROM ledger_entries WHERE account = 'psp:receivable' GROUP BY 1)
    UNION ALL
    (SELECT currency, sum(amount_minor) FROM ledger_entries WHERE account = 'psp:receivable' GROUP BY 1
     EXCEPT SELECT currency, sum(amount_minor) FROM payments WHERE status = 'captured' GROUP BY 1)) d")

  echo "Ledger balance by account:"
  $COMPOSE exec -T postgres psql -U outbox -d outbox -qc "SELECT account, currency,
      count(*) AS entries, sum(amount_minor) AS amount_minor
    FROM ledger_entries GROUP BY GROUPING SETS ((currency, account), (currency)) ORDER BY currency, account NULLS LAST"
  echo "Captured payments:      $(sql "SELECT count(*) FROM payments WHERE status = 'captured'")"
  echo "Ledger transactions:    $(sql "SELECT count(DISTINCT transaction_id) FROM ledger_entries")"
  echo "Unbalanced transactions: $unbalanced"
  echo "Captures without exactly one transaction: $wrong"
  echo "Currencies where psp:receivable != captured total: $mismatch"
  [[ $unbalanced == 0 && $wrong == 0 && $mismatch == 0 ]]
}
