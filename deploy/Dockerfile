# One image with all binaries (payments-api, outbox-relay, ledger-consumer, migrate); each
# compose service picks its command. Image versions come from deploy/.env via build args.
ARG GO_IMAGE
ARG RUNTIME_IMAGE

FROM ${GO_IMAGE} AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download
COPY . .
RUN --mount=type=cache,target=/go/pkg/mod --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 go build -trimpath -ldflags='-s -w' -o /out/ ./cmd/...

FROM ${RUNTIME_IMAGE}
COPY --from=build /out/ /usr/local/bin/
