FROM ubuntu:22.04

ENV DEBIAN_FRONTEND=noninteractive

# Base tools and prerequisites
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
       curl ca-certificates gnupg \
       software-properties-common \
    && rm -rf /var/lib/apt/lists/*

# Install PostgreSQL 17 client from PGDG
RUN install -d -m 0755 /etc/apt/keyrings \
    && curl -fsSL https://www.postgresql.org/media/keys/ACCC4CF8.asc | gpg --dearmor -o /etc/apt/keyrings/postgresql.gpg \
    && echo "deb [signed-by=/etc/apt/keyrings/postgresql.gpg] http://apt.postgresql.org/pub/repos/apt jammy-pgdg main" \
       > /etc/apt/sources.list.d/pgdg.list \
    && apt-get update \
    && apt-get install -y --no-install-recommends postgresql-client-17 \
    && rm -rf /var/lib/apt/lists/*

# Install Node.js via NVM (Node 22)
ENV NVM_DIR=/usr/local/nvm
ENV NODE_VERSION=22
RUN mkdir -p "$NVM_DIR" \
    && curl -o- https://raw.githubusercontent.com/nvm-sh/nvm/v0.40.3/install.sh | bash \
    && . "$NVM_DIR/nvm.sh" \
    && nvm install $NODE_VERSION \
    && nvm alias default $NODE_VERSION \
    && nvm use default \
    && n=$(nvm version default) \
    && ln -s "$NVM_DIR/versions/node/$n/bin/node" /usr/local/bin/node \
    && ln -s "$NVM_DIR/versions/node/$n/bin/npm" /usr/local/bin/npm \
    && ln -s "$NVM_DIR/versions/node/$n/bin/npx" /usr/local/bin/npx \
    && node -v && npm -v

# Requested global NPM installs
RUN npm install -g @openai/codex @anthropic-ai/claude-code

# Default to an interactive-friendly shell
CMD ["bash"]
