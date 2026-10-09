# Build the binary on the host, then copy it in. The cluster does not need
# a Go toolchain or a pull from the internet at image-build time.
FROM alpine:3.21
RUN apk add --no-cache ca-certificates
COPY podpocalypse /usr/local/bin/podpocalypse
EXPOSE 8080
ENTRYPOINT ["podpocalypse"]
