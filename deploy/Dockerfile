# Builds this fork from source and bakes in one deploy profile
# (deploy/<PROFILE>/), whose files seed the data directory on first boot.
#   docker build -f deploy/Dockerfile --build-arg PROFILE=glyphbyte .
# The build context must include the nostrdb submodule:
#   git submodule update --init --recursive

# ── builder ────────────────────────────────────────────────────────
FROM golang:1.23-alpine AS builder

RUN apk add --no-cache git bash curl tar gcc musl-dev make autoconf automake libtool libstdc++ libgcc

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .

RUN find server/db/nostrdb -type f \! -name '*.o' \! -name '*.a' \! -name '*.png' \! -name '*.pdf' \
        -exec sed -i 's/\r$//' {} + \
    && rm -rf server/db/nostrdb/lib server/db/nostrdb/include \
    && cd server/db/nostrdb && bash build.sh

# Dashboard assets, as in docs/docker/Dockerfile.dev.
RUN curl -fsSL -o /tmp/tailwindcss \
        https://github.com/tailwindlabs/tailwindcss/releases/latest/download/tailwindcss-linux-x64-musl \
    && chmod +x /tmp/tailwindcss \
    && (cd www/style && /tmp/tailwindcss -i input.css -o tailwind.min.css --minify) \
    && sed -i 's|<script src="https://cdn.tailwindcss.com"></script>|<link rel="stylesheet" href="/style/tailwind.min.css?v={{assetVersion}}">|g' \
        www/views/templates/layout.html \
    && rm /tmp/tailwindcss
ARG MILL_VERSION=1.8.2
RUN mkdir -p www/static/mill \
    && curl -fsSL -o www/static/mill/mill.umd.min.js \
        "https://cdn.jsdelivr.net/npm/nostr-mill@${MILL_VERSION}/dist/mill.umd.min.js"
RUN mkdir -p docs/openapi && [ -s docs/openapi/swagger.json ] \
    || echo '{"swagger":"2.0","info":{"title":"grain","version":"fork"},"paths":{}}' > docs/openapi/swagger.json

RUN CGO_ENABLED=1 go build -buildvcs=false -o /out/grain .

# ── runtime ────────────────────────────────────────────────────────
FROM alpine:3.20

RUN apk add --no-cache ca-certificates libstdc++ libgcc wget su-exec \
    && addgroup -g 1001 grain && adduser -D -s /bin/sh -u 1001 -G grain grain

ARG PROFILE
WORKDIR /app
COPY --from=builder /out/grain /app/grain
COPY deploy/${PROFILE}/ /app/profile/
COPY deploy/entrypoint.sh /app/entrypoint.sh
RUN chmod +x /app/entrypoint.sh

ENV GRAIN_DATA_DIR=/data
EXPOSE 8181
HEALTHCHECK --interval=15s --timeout=5s --start-period=10s --retries=5 \
  CMD wget --no-verbose --tries=1 --spider http://localhost:8181/ || exit 1

ENTRYPOINT ["/app/entrypoint.sh"]
