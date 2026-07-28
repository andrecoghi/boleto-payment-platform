FROM nginx:1.27-alpine

RUN apk add --no-cache jq gettext

COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh
COPY templates /etc/nginx/templates

# The rendered config is generated at container start (see entrypoint.sh),
# once the real API Gateway id is known -- there is intentionally no
# default.conf baked into the image.
RUN rm -f /etc/nginx/conf.d/default.conf

ENTRYPOINT ["/entrypoint.sh"]
