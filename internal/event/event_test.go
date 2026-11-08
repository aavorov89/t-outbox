package event

import (
	"encoding/json"
	"errors"
	"flag"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

var update = flag.Bool("update", false, "rewrite golden files")

var (
	fixedEventID   = uuid.MustParse("0192f0c4-7a3e-7b1c-9d2e-3f4a5b6c7d8e")
	fixedPaymentID = uuid.MustParse("0192f0c1-1111-7222-8333-444455556666")
	fixedTime      = time.Date(2026, 11, 8, 12, 0, 0, 0, time.UTC)
)

func mustJSON(t *testing.T, v any) json.RawMessage {
	t.Helper()
	b, err := json.Marshal(v)
	require.NoError(t, err)
	return b
}

func goldenCases(t *testing.T) map[string]Envelope {
	amount := Amount{AmountMinor: 1999, Currency: "EUR"}
	return map[string]Envelope{
		"payment.created.v1": {
			EventID: fixedEventID, EventType: TypePaymentCreated, EventVersion: 1,
			AggregateType: AggregatePayment, AggregateID: fixedPaymentID.String(), AggregateVersion: 1,
			OccurredAt: fixedTime,
			Data:       mustJSON(t, PaymentCreatedV1{PaymentID: fixedPaymentID, MerchantID: "m_42", Amount: amount}),
		},
		"payment.captured.v1": {
			EventID: fixedEventID, EventType: TypePaymentCaptured, EventVersion: 1,
			AggregateType: AggregatePayment, AggregateID: fixedPaymentID.String(), AggregateVersion: 2,
			OccurredAt: fixedTime,
			Data:       mustJSON(t, PaymentCapturedV1{PaymentID: fixedPaymentID, MerchantID: "m_42", Amount: amount}),
		},
	}
}

// TestGolden pins the wire format. A failure here means the contract changed.
func TestGolden(t *testing.T) {
	for name, env := range goldenCases(t) {
		t.Run(name, func(t *testing.T) {
			path := filepath.Join("testdata", name+".json")
			got, err := Encode(env)
			require.NoError(t, err)

			if *update {
				var pretty json.RawMessage = got
				out, err := json.MarshalIndent(pretty, "", "  ")
				require.NoError(t, err)
				require.NoError(t, os.WriteFile(path, append(out, '\n'), 0o644))
			}

			want, err := os.ReadFile(path)
			require.NoError(t, err, "run: go test ./internal/event -update")
			require.JSONEq(t, string(want), string(got), "encoded envelope differs from %s", path)

			// The golden file must also decode back to the same value.
			dec, err := Decode(want)
			require.NoError(t, err)
			require.Equal(t, env.EventID, dec.EventID)
			require.Equal(t, env.AggregateVersion, dec.AggregateVersion)
			require.True(t, env.OccurredAt.Equal(dec.OccurredAt))
			require.JSONEq(t, string(env.Data), string(dec.Data))
		})
	}
}

// Amounts above 2^53 must survive encode/decode exactly (no float64 on the way).
func TestAmountAbove2Pow53RoundTrips(t *testing.T) {
	const big = int64(1<<53 + 1)
	env := goldenCases(t)["payment.captured.v1"]
	env.Data = mustJSON(t, PaymentCapturedV1{PaymentID: fixedPaymentID, MerchantID: "m", Amount: Amount{AmountMinor: big, Currency: "EUR"}})

	b, err := Encode(env)
	require.NoError(t, err)
	dec, err := Decode(b)
	require.NoError(t, err)
	data, err := DecodeData[PaymentCapturedV1](dec)
	require.NoError(t, err)
	require.Equal(t, big, data.Amount.AmountMinor)
}

func TestEncodeNormalizesToUTC(t *testing.T) {
	env := goldenCases(t)["payment.created.v1"]
	env.OccurredAt = fixedTime.In(time.FixedZone("CET", 3600))
	b, err := Encode(env)
	require.NoError(t, err)
	require.Contains(t, string(b), `"occurred_at":"2026-11-08T12:00:00Z"`)
}

func TestDecodeRejectsMalformed(t *testing.T) {
	valid, err := Encode(goldenCases(t)["payment.created.v1"])
	require.NoError(t, err)

	mutate := func(f func(m map[string]any)) []byte {
		var m map[string]any
		require.NoError(t, json.Unmarshal(valid, &m))
		f(m)
		return mustJSON(t, m)
	}

	tests := map[string][]byte{
		"not json":             []byte("{nope"),
		"array":                []byte("[]"),
		"missing event_id":     mutate(func(m map[string]any) { delete(m, "event_id") }),
		"event_version 0":      mutate(func(m map[string]any) { m["event_version"] = 0 }),
		"missing aggregate_id": mutate(func(m map[string]any) { delete(m, "aggregate_id") }),
		"data is array":        mutate(func(m map[string]any) { m["data"] = []int{1} }),
		"data missing":         mutate(func(m map[string]any) { delete(m, "data") }),
	}
	for name, b := range tests {
		t.Run(name, func(t *testing.T) {
			_, err := Decode(b)
			require.True(t, errors.Is(err, ErrMalformed), "err = %v", err)
		})
	}
}

func TestDecodeIgnoresUnknownEnvelopeFields(t *testing.T) {
	valid, err := Encode(goldenCases(t)["payment.created.v1"])
	require.NoError(t, err)
	var m map[string]any
	require.NoError(t, json.Unmarshal(valid, &m))
	m["added_later"] = "x"
	_, err = Decode(mustJSON(t, m))
	require.NoError(t, err)
}

func TestIsReserved(t *testing.T) {
	for _, k := range []string{HeaderEventID, HeaderEventType, HeaderContentType} {
		require.True(t, IsReserved(k), k)
	}
	for _, k := range []string{HeaderTraceparent, HeaderTracestate, "baggage"} {
		require.False(t, IsReserved(k), k)
	}
}
