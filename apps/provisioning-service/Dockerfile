# syntax=docker/dockerfile:1

FROM node:20.18.1-bookworm-slim AS build
WORKDIR /app
RUN corepack enable && corepack prepare pnpm@9.0.0 --activate
COPY package.json pnpm-workspace.yaml pnpm-lock.yaml ./
COPY tsconfig.json ./
COPY libs ./libs
COPY apps/provisioning-service ./apps/provisioning-service
RUN pnpm install --frozen-lockfile --filter @activation-poc/provisioning-service...
RUN pnpm --filter @activation-poc/provisioning-service... build

FROM node:20.18.1-bookworm-slim
WORKDIR /app
ENV NODE_ENV=production
COPY --from=build /app/node_modules ./node_modules
COPY --from=build /app/libs ./libs
COPY --from=build /app/apps/provisioning-service ./apps/provisioning-service
WORKDIR /app/apps/provisioning-service
CMD ["node", "dist/main.js"]
