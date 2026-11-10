FROM golang:1.27.1-bookworm AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /podpocalypse ./cmd/podpocalypse

# k9s for the booth screen's pod panel, verified against the release checksums.
# The arch comes from the build machine, so builders without BuildKit's
# TARGETARCH still get the right binary.
FROM alpine:3.21 AS k9s
ARG K9S_VERSION=v0.51.0
RUN set -eu; \
    case "$(uname -m)" in x86_64) arch=amd64 ;; aarch64) arch=arm64 ;; *) echo "unsupported arch $(uname -m)"; exit 1 ;; esac; \
    base="https://github.com/derailed/k9s/releases/download/${K9S_VERSION}"; \
    asset="k9s_Linux_${arch}.tar.gz"; \
    cd /tmp; \
    wget -q "${base}/${asset}" "${base}/checksums.sha256"; \
    grep " ${asset}\$" checksums.sha256 | sha256sum -c -; \
    tar -xzf "${asset}" k9s; \
    install -m 0755 k9s /usr/local/bin/k9s

FROM alpine:3.21
RUN apk add --no-cache ca-certificates
COPY --from=k9s /usr/local/bin/k9s /usr/local/bin/k9s
COPY --from=build /podpocalypse /usr/local/bin/podpocalypse
EXPOSE 8080
ENTRYPOINT ["podpocalypse"]
