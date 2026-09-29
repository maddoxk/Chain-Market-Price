---
id: cloud-deployment
title: Cloud-Native Deployment Suite (Docker, Kubernetes Helm, AWS & GCP Tuning)
sidebar_label: Cloud Deployment
---

# Cloud-Native Deployment Suite

In addition to bare-metal colocation deployments, `Chain-Market-Price` provides a full cloud-native deployment suite for deploying across Docker, Kubernetes (EKS / GKE), AWS Nitro instances, and GCP Compute Engine VMs.

---

## 1. Multi-Stage Docker Container

The engine features a production-grade multi-stage Docker build producing a secure, minimal container running as non-root user `UID 10001` (`cmp`):

```bash
# Build the container locally
docker build -t chain-market-price:latest -f deploy/docker/Dockerfile .

# Run with 512MB shared memory and Tier 2 cloud profile
docker run -d \
  --name cmp-node \
  --shm-size=512m \
  -e CMP_HARDWARE_PROFILE=tier2 \
  -p 9001:9001 \
  -p 9002:9002 \
  chain-market-price:latest
```

### Docker Compose

For local developer workstation verification, run:

```bash
docker-compose -f deploy/docker/docker-compose.yml up -d
```

---

## 2. Kubernetes Helm Chart (`deploy/helm/chain-market-price`)

The Helm chart includes production templates for Deployments, Services, ConfigMaps, and Prometheus ServiceMonitors:

```bash
# Install with the cloud profile (AWS EKS / GCP GKE)
helm install cmp deploy/helm/chain-market-price/ \
  --set profile=cloud \
  --set shm.size=1Gi

# Install for local minikube / development
helm install cmp deploy/helm/chain-market-price/ \
  --set profile=development
```

### Key Kubernetes Features:
- **Memory-Backed Shared Memory:** Configures an `emptyDir` volume with `medium: Memory` mounted at `/dev/shm` for zero-copy lock-free IPC.
- **Health & Readiness Endpoints:** Native HTTP probes (`/healthz`, `/readyz`) on port 9001 prevent traffic routing until ring buffers and feed parsers are live.
- **Non-Root Execution:** Fully drops all Linux capabilities (`capabilities: drop: [ALL]`) and executes as non-root `UID 10001`.

---

## 3. Cloud Hypervisor Tuning Scripts

### AWS Nitro & ENA Network Tuning (`deploy/cloud/aws-nitro-tune.sh`)
Run on AWS EC2 instances (`c6i`, `c7g`, `r6i`, `m6i`) before starting the market data engine:
- Sets invariant clocksource to `tsc`.
- Expands AWS ENA RX and TX hardware ring buffers to 4096 descriptors.
- Configures ENA interrupt coalescing and hashing (SRD / ENA Express).
- Restricts CPU deep C-states (`cpuidle.state[2+]`) to minimize vCPU wake jitter.

### GCP Compute Engine & gVNIC Tuning (`deploy/cloud/gcp-c3-tune.sh`)
Run on GCP C3, C3D, or N2 instances:
- Configures Google Virtual NIC (gVNIC) descriptor rings.
- Enables multi-queue Receive Packet Steering (RPS) and Flow Steering (RFS) across all host vCPUs.
- Enables TCP BBR congestion control and low-latency socket busy-polling (`SO_BUSY_POLL`).
