# syntax=docker/dockerfile:1
FROM golang:1.22-alpine AS build
WORKDIR /src
ARG TARGET=api
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/app ./cmd/${TARGET}

FROM alpine:3.20
RUN apk add --no-cache ca-certificates tzdata
WORKDIR /app
COPY --from=build /out/app /app/app
COPY testdata/dcgm_metrics.csv /data/dcgm_metrics.csv
USER 65532:65532
ENTRYPOINT ["/app/app"]
