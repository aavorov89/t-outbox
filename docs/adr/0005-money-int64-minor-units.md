# 0005. Money as `int64` minor units

**Status:** Accepted

## Context

Payments move exact amounts in many currencies with different exponents: EUR has 2
decimals, JPY 0, KWD 3. The amount crosses Go, JSON, PostgreSQL (`JSONB` and columns)
and Kafka.

## Decision

`money.Money{Amount int64, Currency}`: the amount in minor units (`1999` = €19.99), with
the ISO 4217 currency carrying its exponent. In SQL it is `amount_minor BIGINT` plus
`currency CHAR(3)`, and in JSON `{"amount_minor": 1999, "currency": "EUR"}`. All
arithmetic is overflow-checked (`ErrOverflow`) and refuses mixed currencies
(`ErrCurrencyMismatch`).

## Consequences

- **Exact, fast and boring.** Integer addition, `BIGINT` sums and comparisons. The ledger's
  zero-sum invariant is an integer equality.
- **The limit is about 9.2·10^18 minor units**, about 9.2·10^16 EUR. Overflow is an
  error, never a wrap, and `CheckBalanced` sums as `NUMERIC` so it cannot overflow.
- **JSON numbers above 2^53 lose precision in JavaScript.** Go and Postgres round-trip
  them exactly (a test pins `2^53+1` through `JSONB`). Browser clients would need the
  amount as a string; the README says so.
- **No fractional minor units.** Interest, FX conversion or per-unit pricing would need
  a decimal type and a rounding policy. They are out of scope here.

## Alternatives

- **`float64`**: `0.1 + 0.2 != 0.3`. Never for money.
- **A decimal type** (`shopspring/decimal`, `NUMERIC` columns): exact and flexible, but
  slower, allocation-heavy, and its precision and rounding mode still have to be decided
  per operation. Worth it once there is arithmetic beyond addition.
- **Amount as a decimal string on the wire**: safer for JavaScript clients, but it moves
  parsing and validation into every consumer. Possible later for the public API, without
  changing the internal type.
