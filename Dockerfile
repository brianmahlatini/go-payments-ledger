# syntax=docker/dockerfile:1
FROM golang:1.25-alpine AS build
WORKDIR /src
COPY go.mod ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/ledgerd ./cmd/ledgerd

# distroless static: no shell, no package manager, runs as non-root
FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/ledgerd /ledgerd
USER nonroot:nonroot
EXPOSE 8080
ENTRYPOINT ["/ledgerd"]
