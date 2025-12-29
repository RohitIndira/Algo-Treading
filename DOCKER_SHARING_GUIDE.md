# Project Sharing Guide - Docker Distribution

This guide explains how to share your Algo Trading System with others using Docker images.

## Overview

There are multiple ways to share your project depending on your needs and audience:

1. **Local Docker Images** - Build and share via file
2. **Docker Hub Registry** - Publish publicly or privately
3. **Docker Compose** - Complete system in one file
4. **Kubernetes** - Enterprise deployment

---

## Method 1: Local Docker Images (Recommended for Testing)

### Step 1: Build All Images

**On Windows (PowerShell):**
```powershell
# Navigate to project root
cd d:\Algo_Trade\Algo-Treading

# Build all images
.\scripts\docker-build-push.ps1 -Version "v1.0.0"

# Or with detailed output
.\scripts\docker-build-push.ps1 -Version "v1.0.0" -NoBuildKit
```

**On Linux/Mac:**
```bash
cd Algo-Treading
chmod +x scripts/docker-build-push.sh
./scripts/docker-build-push.sh v1.0.0
```

### Step 2: Verify Images

```bash
# List all built images
docker images | grep trading-

# Expected output:
# REPOSITORY                          TAG                    IMAGE ID
# docker.io/rohitindira/trading-api-gateway       v1.0.0                 abc123def456
# docker.io/rohitindira/trading-user-config       v1.0.0                 def789ghi012
# ...
```

### Step 3: Share Images

#### Option A: Export as TAR files
```bash
# Export all images to a single file (large ~500MB-2GB)
docker save -o trading-system.tar $(docker images --format '{{.Repository}}:{{.Tag}}' | grep trading-)

# Recipient imports:
docker load -i trading-system.tar
```

#### Option B: Share via Compressed Archive
```bash
# Compress for easier transfer
docker save $(docker images --format '{{.Repository}}:{{.Tag}}' | grep trading-) | gzip > trading-system.tar.gz

# Recipient decompresses and loads:
gunzip < trading-system.tar.gz | docker load
```

---

## Method 2: Docker Hub (Recommended for Production)

Docker Hub is a free service for public images. Great for sharing with the world or teams.

### Step 1: Create Docker Hub Account
1. Go to https://hub.docker.com/
2. Sign up (free)
3. Create a repository for each image or one organization

### Step 2: Login to Docker Hub

**Windows:**
```powershell
docker login
# Enter username and password when prompted
```

**Linux/Mac:**
```bash
docker login
```

### Step 3: Build and Push

```powershell
# Windows - Push to Docker Hub
.\scripts\docker-build-push.ps1 -Version "v1.0.0" -Namespace "yourusername" -Push
```

```bash
# Linux/Mac
./scripts/docker-build-push.sh v1.0.0
# When prompted, enter 'y' to push
```

### Step 4: Share with Others

Others can now pull and run your images:

```bash
# Pull the images
docker pull yourusername/trading-api-gateway:v1.0.0
docker pull yourusername/trading-user-config:v1.0.0
# ... etc

# Or simply run the entire system
docker-compose up -d
```

### Managing Docker Hub Images

```bash
# Make repository public
# Login to hub.docker.com → Repository settings → Public

# View your images
docker images

# Remove local image
docker rmi yourusername/trading-api-gateway:v1.0.0

# View image details
docker inspect yourusername/trading-api-gateway:v1.0.0
```

---

## Method 3: Private Registry (Docker Enterprise)

For sensitive projects, use a private registry:

### Option A: Self-Hosted Registry
```bash
# Start a private registry locally
docker run -d -p 5000:5000 --name registry registry:2

# Tag and push
docker tag trading-api-gateway:v1.0.0 localhost:5000/trading-api-gateway:v1.0.0
docker push localhost:5000/trading-api-gateway:v1.0.0
```

### Option B: GitLab Container Registry
```bash
# Login to GitLab
docker login registry.gitlab.com

# Push to GitLab
docker tag trading-api-gateway:v1.0.0 registry.gitlab.com/yourusername/trading/api-gateway:v1.0.0
docker push registry.gitlab.com/yourusername/trading/api-gateway:v1.0.0
```

---

## Method 4: Complete Docker Compose File

### Step 1: Update Docker Compose References

Edit `docker-compose.yml` to reference shared images:

```yaml
api-gateway:
  image: yourusername/trading-api-gateway:v1.0.0  # Use pre-built image
  # Remove: build section
  
user-config:
  image: yourusername/trading-user-config:v1.0.0
  # Remove: build section
```

### Step 2: Share the Compose File

Recipients just need:
1. `docker-compose.yml` (the file you created)
2. `.env.example` (for configuration)
3. Run: `docker-compose up -d`

---

## For Recipients: How to Run Your Shared Project

### Quick Start

1. **Install Docker Desktop**
   - Windows: https://docs.docker.com/desktop/install/windows-install/
   - Mac: https://docs.docker.com/desktop/install/mac-install/
   - Linux: https://docs.docker.com/engine/install/

2. **Get the Files**
   - `docker-compose.yml`
   - `.env.example` (optional, but recommended)

3. **Run the System**
   ```bash
   # Option A: Use pre-built images
   docker-compose up -d
   
   # Option B: Load local tar file
   docker load -i trading-system.tar
   docker-compose up -d
   ```

4. **Access Services**
   - API: http://localhost:8081
   - RabbitMQ UI: http://localhost:15672 (guest/guest)
   - PostgreSQL: localhost:5432 (postgres/postgres)

5. **Stop Services**
   ```bash
   docker-compose down
   ```

---

## Verification Checklist

Before sharing, verify:

- [ ] All services build without errors
- [ ] `docker-compose up -d` starts successfully
- [ ] All health checks pass: `docker-compose ps`
- [ ] API responds: `curl http://localhost:8081`
- [ ] Database connections work
- [ ] No hardcoded credentials in images (use .env)
- [ ] Image sizes are reasonable (<500MB each)
- [ ] Documentation is clear

---

## Optimization Tips

### Reduce Image Size

```dockerfile
# Use slim/alpine base images (already done)
FROM golang:1.24.6-alpine

# Multi-stage build (already done)
# Remove unnecessary files
RUN rm -rf /tmp/* /var/tmp/*
```

### Build Faster

```bash
# Enable BuildKit (faster builds)
export DOCKER_BUILDKIT=1
docker build ...

# Windows PowerShell
$env:DOCKER_BUILDKIT=1
docker build ...
```

### Version Control

```bash
# Use semantic versioning
docker build -t yourusername/trading-api-gateway:v1.0.0 .
docker build -t yourusername/trading-api-gateway:latest .

# Tag multiple versions
docker tag yourusername/trading-api-gateway:v1.0.0 yourusername/trading-api-gateway:v1
```

---

## Troubleshooting

### Images won't build
```bash
# Clear builder cache
docker builder prune

# Rebuild without cache
docker-compose build --no-cache
```

### Push fails to Docker Hub
```bash
# Verify login
docker info | grep Username

# Re-login
docker logout
docker login

# Check image naming
# Must be: dockerhub_username/repository:tag
docker tag trading-api-gateway yourusername/trading-api-gateway:v1.0.0
```

### Services won't start
```bash
# Check logs
docker-compose logs -f

# Verify ports not in use
netstat -ano | findstr :8081  # Windows
lsof -i :8081                 # Linux/Mac

# Check docker resources
docker stats
```

### Out of disk space
```bash
# Clean up old images
docker image prune -a

# Clean up old containers
docker container prune

# Full cleanup
docker system prune -a -v
```

---

## Security Best Practices

### Before Sharing

1. **No Credentials in Images**
   ```bash
   # ✓ Good - Use environment variables
   ENV DATABASE_PASSWORD=""  # Empty, set at runtime
   
   # ✗ Bad - Hardcoded passwords
   ENV DATABASE_PASSWORD="secret123"
   ```

2. **Scan for Vulnerabilities**
   ```bash
   docker scout cves yourusername/trading-api-gateway:v1.0.0
   ```

3. **Minimal Base Images**
   - ✓ Use: `alpine:latest`, `golang:alpine`
   - ✗ Avoid: `ubuntu:latest`, `debian:latest`

4. **Run as Non-Root**
   ```dockerfile
   RUN addgroup -g 1000 app && adduser -u 1000 -G app app
   USER app
   ```

### For Recipients

- [ ] Pull images only from trusted sources
- [ ] Verify image signatures
- [ ] Keep Docker updated
- [ ] Scan pulled images for vulnerabilities
- [ ] Use environment variables for secrets

---

## Support Resources

- **Docker Docs**: https://docs.docker.com/
- **Docker Hub**: https://hub.docker.com/
- **Compose Reference**: https://docs.docker.com/compose/compose-file/
- **Docker Best Practices**: https://docs.docker.com/develop/dev-best-practices/

---

## Next Steps

1. ✅ Build and test locally
2. ✅ Optimize images (reduce size)
3. ✅ Push to Docker Hub (or private registry)
4. ✅ Create share link with instructions
5. ✅ Document environment setup
6. ✅ Provide `.env.example` template
7. ✅ Create quick start guide for recipients

Good luck sharing your project! 🚀
