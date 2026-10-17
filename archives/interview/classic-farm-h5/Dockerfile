# syntax=docker/dockerfile:1

FROM node:22-alpine AS build

WORKDIR /app
COPY package.json package-lock.json ./
RUN npm ci
COPY . .

ARG BUILD_MODE=staging
RUN case "$BUILD_MODE" in \
      staging) npm run build:staging ;; \
      web) npm run build:web ;; \
      wechat) npm run build:wechat ;; \
      *) echo "unsupported BUILD_MODE=$BUILD_MODE" >&2; exit 1 ;; \
    esac

FROM nginx:1.27-alpine

COPY deploy/nginx.conf /etc/nginx/conf.d/default.conf
COPY --from=build /app/dist /usr/share/nginx/html

EXPOSE 8080
HEALTHCHECK --interval=10s --timeout=3s --retries=3 \
  CMD ["busybox", "wget", "-q", "-O", "/dev/null", "http://127.0.0.1:8080/healthz"]
