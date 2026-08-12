#!/bin/sh
# Renders /usr/share/nginx/html/config.js from environment variables at
# container start, so a deployment can repoint every service URL without
# rebuilding the image.
#
# The official nginx image runs every executable file in /docker-entrypoint.d
# in alphabetical order before starting nginx; the 40- prefix puts this after
# the built-in envsubst step (20-) that templates the CSP.
#
# Variable names are the SAME VITE_* names used at build time and by
# dataops-ui, so one set of names configures every DALI front end. The
# difference is only when they are read: in this image, at start-up.
#
# Only variables that are actually set are emitted. Anything omitted falls back
# to the value baked into the bundle at build time, then to the default in
# src/config.ts — the same precedence the frontend already implements.
set -eu

TARGET=/usr/share/nginx/html/config.js

# An operator may bind-mount their own config.js read-only. Respect it.
if [ -e "$TARGET" ] && ! [ -w "$TARGET" ]; then
  echo "40-dataops-config.sh: $TARGET is not writable (mounted override?) — leaving it untouched"
  exit 0
fi

# Escape for a single-quoted JS string literal: backslash first, then quote.
esc() {
  printf '%s' "$1" | sed -e 's/\\/\\\\/g' -e "s/'/\\\\'/g"
}

emitted=0
tmp=$(mktemp)

# shellcheck disable=SC2129
{
  echo "/* Generated at container start by 40-dataops-config.sh — do not edit."
  echo " * Values come from the environment; unset keys fall back to the values"
  echo " * compiled into the bundle. */"
  echo "window.__DATAOPS_CONFIG__ = {"
} > "$tmp"

# key:env-var pairs. Keep in step with ENV_KEYS in src/config.ts.
for pair in \
  'orchestratorUrl:VITE_ORCHESTRATOR_URL' \
  'catalogueBaseUrl:VITE_CATALOGUE_BASE_URL' \
  'portalUrl:VITE_PORTAL_URL' \
  'daliUrl:VITE_DALI_URL' \
  'dataspaceUrl:VITE_DATASPACE_URL' \
  'dataopsUrl:VITE_DATAOPS_URL' \
  'mlopsUrl:VITE_MLOPS_URL' \
  'keycloakUrl:VITE_KEYCLOAK_URL' \
  'keycloakRealm:VITE_KEYCLOAK_REALM' \
  'keycloakClientId:VITE_KEYCLOAK_CLIENT_ID'
do
  key=${pair%%:*}
  var=${pair#*:}
  # Indirect expansion, POSIX-sh style: unset and empty both mean "not set".
  value=$(printf '%s' "$(eval "printf '%s' \"\${$var:-}\"")")
  if [ -n "$value" ]; then
    printf "  %s: '%s',\n" "$key" "$(esc "$value")" >> "$tmp"
    emitted=$((emitted + 1))
  fi
done

echo "}" >> "$tmp"

if [ "$emitted" -eq 0 ]; then
  # Nothing supplied — keep the file that shipped in the image rather than
  # replacing it with an empty object.
  echo "40-dataops-config.sh: no VITE_* overrides set — keeping the image's config.js"
  rm -f "$tmp"
  exit 0
fi

cat "$tmp" > "$TARGET"
rm -f "$tmp"
echo "40-dataops-config.sh: wrote $TARGET with $emitted override(s) from the environment"
