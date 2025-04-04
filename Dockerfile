FROM node:20-alpine AS builder

WORKDIR /app

# Copy package files
COPY package*.json ./

# Copy source code
COPY tsconfig.json ./
COPY src ./src

# Install dependencies
RUN npm ci

# Build the application
RUN npm run build

FROM node:20-alpine AS release

WORKDIR /app

# Set to production environment
ENV NODE_ENV=production

# Copy package files and install production dependencies
COPY package*.json ./
RUN npm ci --omit=dev --ignore-scripts

# Copy built application from builder stage
COPY --from=builder /app/dist ./dist

# Create a non-root user and set permissions
RUN addgroup -S mcp && \
    adduser -S mcp -G mcp && \
    chown -R mcp:mcp /app

# Switch to non-root user
USER mcp

# Set the entrypoint
ENTRYPOINT ["node", "dist/index.js"]