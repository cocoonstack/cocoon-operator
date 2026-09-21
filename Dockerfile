FROM --platform=$BUILDPLATFORM golang:1.27.1-alpine3.24@sha256:4cb7ac979db5fcc41cae44b2227ba5ab8a51e8807f40d9ba4dee20a0ad960b5b AS build
ARG TARGETOS
ARG TARGETARCH
ARG VERSION=dev
ARG REVISION=unknown
ARG BUILTAT=unknown
WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=${TARGETOS:-linux} GOARCH=${TARGETARCH:-amd64} \
    go build -trimpath \
      -ldflags="-s -w \
        -X github.com/cocoonstack/cocoon-operator/version.VERSION=${VERSION} \
        -X github.com/cocoonstack/cocoon-operator/version.REVISION=${REVISION} \
        -X github.com/cocoonstack/cocoon-operator/version.BUILTAT=${BUILTAT}" \
      -o /out/cocoon-operator .

FROM alpine:3.24@sha256:28bd5fe8b56d1bd048e5babf5b10710ebe0bae67db86916198a6eec434943f8b AS runtime-deps
RUN apk add --no-cache ca-certificates

FROM busybox:stable-musl@sha256:3c6ae8008e2c2eedd141725c30b20d9c36b026eb796688f88205845ef17aa213
COPY --from=runtime-deps /etc/ssl/certs/ /etc/ssl/certs/
COPY --from=build /out/cocoon-operator /usr/bin/cocoon-operator

ENTRYPOINT ["/usr/bin/cocoon-operator"]
