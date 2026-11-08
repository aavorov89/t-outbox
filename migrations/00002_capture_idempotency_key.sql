-- +goose Up

-- The Idempotency-Key of the capture request. A retry with the same key replays the
-- capture; any other key gets 409. Set exactly when the payment is captured.
ALTER TABLE payments
    ADD COLUMN capture_idempotency_key TEXT,
    ADD CONSTRAINT payments_capture_key_iff_captured
        CHECK ((status = 'captured') = (capture_idempotency_key IS NOT NULL));

-- +goose Down
ALTER TABLE payments
    DROP CONSTRAINT payments_capture_key_iff_captured,
    DROP COLUMN capture_idempotency_key;
