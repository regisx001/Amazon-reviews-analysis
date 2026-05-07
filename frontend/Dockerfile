# Build stage
FROM oven/bun:latest AS builder

WORKDIR /app

# Copy package files
COPY package.json bun.lock ./

# Install dependencies
RUN bun install

# Copy source code
COPY . .

# Build the app
RUN bun run build

# Runtime stage
FROM node:20-slim

WORKDIR /app

# Copy package.json
COPY --from=builder /app/package.json ./

# Install production dependencies only (express for serving)
RUN npm install express --production

# Copy built dist directory
COPY --from=builder /app/dist ./dist

# Copy static files
COPY --from=builder /app/static ./static

# Copy server.js
COPY server.js ./

# Environment variables
ENV NODE_ENV=production
ENV PORT=3000

# Expose port
EXPOSE 3000

# Run the server
CMD ["node", "server.js"]