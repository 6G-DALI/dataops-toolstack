# Build (guidelines §27)
FROM node:24-alpine AS build

WORKDIR /app

COPY package*.json ./
RUN npm ci

COPY . .

# The commit this image was built from, surfaced in the app footer. Passed in
# rather than read from git: .dockerignore excludes .git, so the build context
# has no repository to ask. Unset simply yields an unmarked build.
ARG GIT_SHA=""
ENV GIT_SHA=$GIT_SHA

RUN npm run build

FROM nginx:1.27-alpine

# Templated rather than copied straight to conf.d: the official nginx image runs
# envsubst over /etc/nginx/templates at start-up, which lets the CSP name this
# deployment's Keycloak and Orchestrator origins without rebuilding.
COPY nginx/default.conf.template /etc/nginx/templates/default.conf.template

# Restrict envsubst to our own variables so nginx's $uri, $host etc. survive.
ENV NGINX_ENVSUBST_FILTER='KEYCLOAK_ORIGIN|ORCHESTRATOR_ORIGIN|ORCHESTRATOR_UPSTREAM'
ENV KEYCLOAK_ORIGIN=https://auth.dspace.sparkworks.net
# Internal address of the orchestrator, used by the /api reverse proxy. Not
# reachable from outside the compose network — that is the point.
ENV ORCHESTRATOR_UPSTREAM=http://dataops-orchestrator:8000
# Only needed if the browser calls the API cross-origin instead of via /api.
ENV ORCHESTRATOR_ORIGIN=''

# Renders config.js from VITE_* environment variables at start-up, so URLs are
# settable at run time instead of only at build time. chmod via RUN rather than
# COPY --chmod so the build does not require BuildKit.
COPY docker-entrypoint.d/40-dataops-config.sh /docker-entrypoint.d/40-dataops-config.sh
RUN chmod +x /docker-entrypoint.d/40-dataops-config.sh

COPY --from=build /app/dist /usr/share/nginx/html

EXPOSE 80

CMD ["nginx", "-g", "daemon off;"]
