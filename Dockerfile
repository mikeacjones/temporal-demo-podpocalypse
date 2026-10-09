FROM golang:1.27.1-bookworm AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /podpocalypse ./cmd/podpocalypse

FROM alpine:3.21
RUN apk add --no-cache ca-certificates
COPY --from=build /podpocalypse /usr/local/bin/podpocalypse
EXPOSE 8080
ENTRYPOINT ["podpocalypse"]
