FROM node:22-alpine

# Install system dependencies (tzdata for timezone support)
RUN apk add --no-cache tzdata

# Install pnpm globally
RUN npm install -g pnpm

WORKDIR /app

# Copy package management files first
COPY package.json pnpm-lock.yaml pnpm-workspace.yaml ./

# Copy telegram-bot package files
COPY packages/telegram-bot/package.json ./packages/telegram-bot/package.json

# Install dependencies
RUN pnpm install --frozen-lockfile

# Copy telegram-bot source code
COPY packages/telegram-bot ./packages/telegram-bot

# Build the telegram-bot package
RUN pnpm --filter telegram-bot-template run build

# Verify the build output
RUN ls -la packages/telegram-bot/build/

# Set working directory to the telegram-bot package
WORKDIR /app/packages/telegram-bot

CMD ["pnpm", "start"]

