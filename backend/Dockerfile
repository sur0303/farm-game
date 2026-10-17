# syntax=docker/dockerfile:1

FROM golang:1.26-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
ENV GOPROXY=https://goproxy.cn,direct
ENV GOPRIVATE=git.internal.example.invalid
RUN go mod download
COPY . .
ARG SERVICE
ARG APP_VERSION="0.0.0-dev"
ARG GIT_COMMIT=""
ARG IMAGE_REVISION=""
RUN test -n "$SERVICE" && CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags "-s -w -X farmgame.Version=$APP_VERSION -X farmgame.GitCommit=$GIT_COMMIT -X farmgame.ImageRevision=$IMAGE_REVISION" -o /out/service "./cmd/$SERVICE"

FROM alpine:3.21
# Keep the runtime image network-independent. BusyBox already provides wget for
# the local HTTP health checks; the current services do not need OS packages.
RUN addgroup -S farm && adduser -S -G farm farm
COPY --from=build /out/service /usr/local/bin/farm-service
ARG IMAGE_REVISION=""
LABEL org.opencontainers.image.revision=${IMAGE_REVISION}
ENV FARM_IMAGE_REVISION=${IMAGE_REVISION}
USER farm
ENTRYPOINT ["/usr/local/bin/farm-service"]
