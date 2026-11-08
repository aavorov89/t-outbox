# API errors

Every error response of `payments-api` is an [RFC 9457](https://www.rfc-editor.org/rfc/rfc9457)
problem details object with `Content-Type: application/problem+json`:

```json
{
  "type": "https://github.com/aavorov89/t-outbox/blob/main/docs/api-errors.md#validation-error",
  "title": "Request validation failed",
  "status": 422,
  "instance": "/payments",
  "errors": [
    { "pointer": "/amount/currency", "detail": "must be a supported ISO 4217 code" }
  ]
}
```

- **`type`** identifies the problem. Clients branch on `type`, never on `title` or `detail`.
- **`status`** always equals the HTTP status code.
- **`detail`** is for humans and may change.
- Generic HTTP errors (404, 413, 415, 500) use `"type": "about:blank"`, and their `title`
  is the HTTP status phrase, as RFC 9457 §4.2.1 prescribes. A 500 never carries internal
  details.

## Problem types

### malformed-request

`400`. The body is not a single JSON object matching the schema: invalid JSON, an unknown
field, trailing data after the object, or a wrong type, such as a fractional
`amount_minor` (`19.99`) or a string (`"1999"`). Amounts are integers in minor units, never
floats.

### validation-error

`422`. The JSON is well formed, but values are invalid. The `errors` extension lists each
field as a JSON Pointer (RFC 6901) into the request body.

### idempotency-key-missing

`400`. `POST /payments` without an `Idempotency-Key` header, or with a key that is longer
than 255 characters or contains anything other than printable ASCII without spaces.
Generate one key per payment on the client (a UUID is fine) and reuse it for every retry
of that payment.

### idempotency-key-reused

`422`. The `Idempotency-Key` was already used for a payment with a **different** request
(merchant, amount or currency). Nothing was created. A new payment needs a new key.

## Idempotency semantics of `POST /payments`

| Situation | Response |
|---|---|
| New key | `201 Created`, `Location: /payments/{id}` |
| Same key, same request (retry) | `200 OK`, the stored payment, `Idempotent-Replayed: true` |
| Same key, different request | `422` `idempotency-key-reused` |
| Concurrent requests with one key | One `201`, the others `200` (see below) |

"Same request" compares the parsed values (`merchant_id`, `amount_minor`, `currency`),
not the raw bytes, so whitespace and key order don't matter.

The replay returns the payment's **current** state, which can differ from the original
`201` body once the payment changes (e.g. after capture). Stripe stores and replays the
original response instead. This project skips that because the outcome (which payment)
is what retries need.

### Why 422 and not 409 for a reused key

[draft-ietf-httpapi-idempotency-key-header](https://datatracker.ietf.org/doc/draft-ietf-httpapi-idempotency-key-header/)
separates two cases:

- **422 Unprocessable Content**: the key is reused with a different payload. The request
  is well formed, but it can't be processed under this key. Retrying it can never succeed.
- **409 Conflict**: a request with the same key is **still being processed**. Retrying
  later can succeed.

This API never returns the 409 case. The key is claimed by the same transaction that
writes the payment and its outbox event (`UNIQUE (idempotency_key)`). A concurrent
duplicate's `INSERT` waits on the unique index until the first transaction ends. If that
transaction commits, the duplicate fails with a unique violation and reads the committed
payment. If it rolls back, the duplicate creates the payment itself. No intermediate
"in progress" state is ever visible. APIs that store the key in a separate table or in
Redis before doing the work need the 409.
