# Building pg_hashdb in Docker

This document describes how to build the `pg_hashdb` PostgreSQL extension in a Docker environment or similar containerized setup.

## Overview

`pg_hashdb` is a PostgreSQL extension built with [pgrx](https://github.com/tcdi/pgrx) that provides functions to compute cryptographic hashes of PostgreSQL table contents for data integrity verification and change detection.

## System Requirements

- Linux environment (tested on ARM64/AArch64)
- Internet connectivity for downloading packages and source code
- Sufficient disk space (~2GB for compilation artifacts)

## Required System Packages

Install the following packages using `apt-get`:

```bash
# Update package lists
sudo apt-get update

# Essential build tools
sudo apt-get install -y build-essential

# PostgreSQL development packages
sudo apt-get install -y postgresql-server-dev-all postgresql-client postgresql-common

# LLVM/Clang for bindgen (required by pgrx)
sudo apt-get install -y llvm-dev libclang-dev clang

# Additional build dependencies
sudo apt-get install -y pkg-config libssl-dev libreadline-dev zlib1g-dev libicu-dev

# Parser generators (if not installing from source)
sudo apt-get install -y bison flex

# Download tools
sudo apt-get install -y wget curl
```

## Alternative: Manual Installation of Build Tools

If you cannot use `sudo` or prefer to install tools manually:

### 1. Install Rust Toolchain

```bash
# Install rustup and Rust
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
source $HOME/.cargo/env

# Verify installation
cargo --version
rustc --version
```

### 2. Install Bison (if not available via apt)

```bash
cd /tmp
wget http://ftp.gnu.org/gnu/bison/bison-3.8.2.tar.gz
tar -xzf bison-3.8.2.tar.gz
cd bison-3.8.2
./configure --prefix=$HOME/local
make && make install
export PATH=$HOME/local/bin:$PATH
```

### 3. Install Flex (if not available via apt)

```bash
cd /tmp
wget https://github.com/westes/flex/files/981163/flex-2.6.4.tar.gz
tar -xzf flex-2.6.4.tar.gz
cd flex-2.6.4
./configure --prefix=$HOME/local
make && make install
export PATH=$HOME/local/bin:$PATH
```

### 4. Install LLVM/Clang (for bindgen)

**Note:** This is the most critical dependency for pgrx builds.

```bash
# Option 1: Download precompiled LLVM (recommended)
cd /tmp
wget https://github.com/llvm/llvm-project/releases/download/llvmorg-16.0.6/clang+llvm-16.0.6-aarch64-linux-gnu.tar.xz
tar -xf clang+llvm-16.0.6-aarch64-linux-gnu.tar.xz
mv clang+llvm-16.0.6-aarch64-linux-gnu $HOME/local/llvm
export PATH=$HOME/local/llvm/bin:$PATH
export LIBCLANG_PATH=$HOME/local/llvm/lib
```

## Build Process

### 1. Install cargo-pgrx

```bash
cargo install --locked cargo-pgrx
```

### 2. Initialize pgrx with PostgreSQL

**Option A: Use system PostgreSQL (if installed via apt)**
```bash
cargo pgrx init --pg15 $(which pg_config)
```

**Option B: Download and compile PostgreSQL (if no system installation)**
```bash
# This will download, compile, and install PostgreSQL 15 for development
cargo pgrx init --pg15 download
```

**Note:** PostgreSQL compilation can take 15-30 minutes depending on system performance.

### 3. Configure the Extension

The extension is configured to use PostgreSQL 15 by default. If you need a different version, edit `Cargo.toml`:

```toml
[features]
default = ["pg15"]  # Change to pg13, pg14, pg16, pg17 as needed
```

### 4. Build the Extension

```bash
# Set up environment
export PATH=$HOME/local/bin:$HOME/.cargo/bin:$PATH

# Build the extension package
cargo pgrx package
```

If you used a custom PostgreSQL installation:
```bash
cargo pgrx package --pg-config $HOME/.pgrx/15.14/pgrx-install/bin/pg_config
```

## Troubleshooting

### Common Issues

1. **libclang not found**
   - Ensure `libclang-dev` is installed or `LIBCLANG_PATH` is set correctly
   - Error: `Unable to find libclang`

2. **bison/flex not found**
   - Install via apt: `sudo apt-get install bison flex`
   - Or install manually to `$HOME/local/bin` and update PATH

3. **PostgreSQL headers missing**
   - Install: `sudo apt-get install postgresql-server-dev-all`
   - Or use `cargo pgrx init --pg15 download` to compile from source

4. **Permission denied during package installation**
   - Use manual installation methods described above
   - All tools can be installed to `$HOME/local` without sudo

### Environment Variables

Key environment variables for the build process:

```bash
# Add local tools to PATH
export PATH=$HOME/local/bin:$HOME/.cargo/bin:$PATH

# For manual LLVM installation
export LIBCLANG_PATH=$HOME/local/llvm/lib

# For pgrx debugging
export PGRX_BUILD_VERBOSE=1
```

## Docker-specific Considerations

When building in Docker containers:

1. **Base Image**: Use a Debian/Ubuntu base with build tools
   ```dockerfile
   FROM ubuntu:22.04
   RUN apt-get update && apt-get install -y \
       build-essential postgresql-server-dev-all \
       llvm-dev libclang-dev clang \
       pkg-config libssl-dev curl
   ```

2. **Multi-stage Builds**: Consider using multi-stage builds to reduce final image size

3. **Layer Caching**: Install system packages in early layers for better caching

4. **User Permissions**: Run builds as non-root user when possible

## Verification

After successful build, you should have:

- Extension files in `target/release/pg_hashdb-pg15/`
- SQL files for installation
- Shared library (`.so` file)

Test the build:
```bash
# Check if files were generated
ls -la target/release/pg_hashdb-pg15/

# Verify the shared library
file target/release/pg_hashdb-pg15/*.so
```

## Extension Functions

The built extension provides these functions:

- `vkar_hash_table(regclass, int)` - Hash a single table
- `vkar_db_hash(int)` - Hash all tables in the database

Both functions use BLAKE3 cryptographic hashing for data integrity verification.

## Performance Notes

- PostgreSQL compilation (if using `download` option): 15-30 minutes
- Extension compilation: 2-5 minutes
- Total build time: 20-35 minutes on typical systems

## Support

For issues with:
- **pgrx**: https://github.com/tcdi/pgrx
- **PostgreSQL**: https://www.postgresql.org/support/
- **This extension**: Check the project repository for issue tracking