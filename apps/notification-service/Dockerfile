# syntax=docker/dockerfile:1

FROM node:20.18.1-bookworm-slim AS build
WORKDIR /app
RUN corepack enable && corepack prepare pnpm@9.0.0 --activate
COPY package.json pnpm-workspace.yaml pnpm-lock.yaml ./
COPY tsconfig.json ./
COPY libs ./libs
COPY apps/notification-service ./apps/notification-service
RUN pnpm install --frozen-lockfile --filter @activation-poc/notification-service...
RUN pnpm --filter @activation-poc/notification-service... build

FROM node:20.18.1-bookworm-slim
WORKDIR /app
ENV NODE_ENV=production
COPY --from=build /app/node_modules ./node_modules
COPY --from=build /app/libs ./libs
COPY --from=build /app/apps/notification-service ./apps/notification-service
WORKDIR /app/apps/notification-service
CMD ["node", "dist/main.js"]
