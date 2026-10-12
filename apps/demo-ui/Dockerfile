# syntax=docker/dockerfile:1

FROM node:20.18.1-bookworm-slim AS build
WORKDIR /app
RUN corepack enable && corepack prepare pnpm@9.0.0 --activate
ARG VITE_API_URL=http://localhost:3000
ENV VITE_API_URL=$VITE_API_URL
COPY package.json pnpm-workspace.yaml pnpm-lock.yaml ./
COPY apps/demo-ui ./apps/demo-ui
RUN pnpm install --frozen-lockfile --filter @activation-poc/demo-ui...
RUN pnpm --filter @activation-poc/demo-ui build

FROM nginx:1.25.4-alpine
COPY --from=build /app/apps/demo-ui/dist /usr/share/nginx/html
EXPOSE 80
