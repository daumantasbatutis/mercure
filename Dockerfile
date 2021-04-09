FROM caddy:2-alpine

COPY --from=caddy:2-builder-alpine /usr/local/go/ /usr/local/go/
COPY --from=caddy:2-builder-alpine /usr/bin/xcaddy /usr/bin/xcaddy
ENV PATH="/usr/local/go/bin:${PATH}"

COPY $PWD mercure_source/

RUN xcaddy build \
    --with github.com/dunglas/mercure/caddy=$PWD/mercure_source/caddy \
    --with github.com/dunglas/mercure=$PWD/mercure_source

RUN mv caddy /usr/bin/caddy

RUN rm -rf mercure_source
RUN rm -rf /usr/local/go/
RUN rm /usr/bin/xcaddy
RUN rm -rf /root/.go
RUN rm -rf /root/.cache/go-build

COPY CaddyConfig.json /etc/caddy/CaddyConfig.json
COPY public public/
RUN mkdir /var/log/caddy
RUN chmod -R 755 /var/log/caddy
