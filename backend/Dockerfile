FROM python:3.10-slim-bookworm

WORKDIR /app

# Install Node.js and curl
RUN echo ">>> Updating and installing curl..." && \
    apt-get update && apt-get install -y --no-install-recommends curl && \
    echo ">>> Installing Node.js 20..." && \
    curl -fsSL https://deb.nodesource.com/setup_20.x | bash - && \
    apt-get install -y --no-install-recommends nodejs && \
    apt-get clean && rm -rf /var/lib/apt/lists/* && \
    echo ">>> Node.js version:" && node --version && \
    echo ">>> npm version:" && npm --version

# Copy requirements file
COPY requirements.txt .

# Install Python dependencies
RUN echo ">>> Installing Python dependencies..." && \
    pip install --no-cache-dir -r requirements.txt && \
    echo ">>> Python packages installed successfully"

# Install Playwright and its dependencies separately
RUN echo ">>> Installing Playwright..." && \
    playwright install chromium && \
    echo ">>> Installing Playwright system dependencies..." && \
    playwright install-deps && \
    echo ">>> Playwright installed successfully"

# Copy package files for Node.js
COPY package*.json ./

# Install Node.js dependencies
RUN echo ">>> Installing Node.js dependencies..." && \
    npm ci --omit=dev && \
    echo ">>> Node.js dependencies installed successfully"

# Copy source code
COPY . .

# Create non-root user
RUN echo ">>> Creating non-root user..." && \
    adduser --system --uid 1001 backenduser && \
    chown -R backenduser /app

# Switch to the non-root user
USER backenduser

# Expose port
EXPOSE 8000

# Start server
CMD uvicorn main:app --host 0.0.0.0 --port $PORT