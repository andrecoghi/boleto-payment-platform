#!/bin/sh
set -eu

SHARED_CONFIG_PATH="${SHARED_CONFIG_PATH:-/shared/endpoints.json}"

echo "[nginx-entrypoint] waiting for ${SHARED_CONFIG_PATH} (written by the bootstrap service)..."
until [ -f "$SHARED_CONFIG_PATH" ]; do
  sleep 1
done

export API_ID
API_ID="$(jq -r '.api_id' "$SHARED_CONFIG_PATH")"
echo "[nginx-entrypoint] resolved API Gateway id: ${API_ID}"

envsubst '${API_ID}' < /etc/nginx/templates/default.conf.template > /etc/nginx/conf.d/default.conf

echo "[nginx-entrypoint] starting nginx"
exec nginx -g 'daemon off;'
