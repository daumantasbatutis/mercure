FROM caddy:2-alpine

ENV MERCURE_TRANSPORT_URL=bolt:///data/mercure.db

COPY mercure /usr/bin/caddy
COPY Caddyfile /etc/caddy/Caddyfile
COPY Caddyfile.dev /etc/caddy/Caddyfile.dev
COPY Caddyfile.test /etc/caddy/Caddyfile.test
COPY public public/
