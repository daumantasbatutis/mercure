FROM caddy:2-alpine

COPY mercure /usr/bin/caddy
COPY CaddyConfig.json /etc/caddy/CaddyConfig.json
COPY public public/
RUN mkdir /var/log/caddy
RUN chmod -R 755 /var/log/caddy
