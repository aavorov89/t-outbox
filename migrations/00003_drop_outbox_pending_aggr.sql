-- +goose Up

-- The relay's ordering guard probes the previous version through the unique key
-- (aggregate_type, aggregate_id, aggregate_version) instead of scanning an aggregate's
-- pending rows, so this index has no reader left; it only cost a write per insert and
-- per publish. Why the guard changed: internal/outbox/relay.go, claimSQL.
DROP INDEX outbox_pending_aggr;

-- +goose Down
CREATE INDEX outbox_pending_aggr ON outbox (aggregate_id, id) WHERE published_at IS NULL;
