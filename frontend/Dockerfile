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
FROM oven/bun:latest

WORKDIR /app

# Copy built artifacts and necessary files from builder
COPY --from=builder /app/build ./build
COPY --from=builder /app/package.json ./package.json
COPY --from=builder /app/node_modules ./node_modules

# Environment variables
ENV NODE_ENV=production
ENV PORT=3000

# Expose port
EXPOSE 3000

# # Run the node app (using the node adapter build)
# CMD ["node", "build"]
