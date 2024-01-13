FROM alpine:3.6

RUN apk update && apk add bash curl && rm -rf /var/cache/apk/*

RUN mkdir -p /tmp/load/

COPY ./export.ndjson /tmp/load/export.ndjson
COPY ./load_ndjson.sh /tmp/load/load_ndjson.sh
RUN chmod -R 777 /tmp/load/*