# cocoon-operator

Kubernetes operator that manages VM-backed pod lifecycles through two CRDs: **Hibernation** (suspend/wake a single VM pod) and **CocoonSet** (manage a group of related VM pods).

## Overview

- **Hibernation controller** -- watches `Hibernation` CRDs and annotates pods with `cocoon.cis/hibernate=true` to trigger vk-cocoon snapshot/restore
- **CocoonSet controller** -- watches `CocoonSet` CRDs, creates/deletes agent and toolbox pods, manages suspend/unsuspend, and reports aggregate status
- **Pod watcher** -- detects pod changes owned by CocoonSets and triggers reconciliation

Kubernetes controllers assume pods are replaceable. VM-backed workloads are not. This operator keeps stateful VM workflows inside native Kubernetes APIs: scale out from a known main VM, keep stable slot identities, hibernate without letting ReplicaSets recreate the pod, and expose aggregate state through CRD status.

A 30-second informer resync and 60-second periodic reconciliation catch status transitions that informer events may miss.

## Architecture

The operator runs a single binary with three informer loops:

1. **Hibernation controller** -- annotates pods for vk-cocoon snapshot/restore
2. **CocoonSet controller** -- manages agent and toolbox pod groups
3. **Pod watcher** -- triggers reconciliation on pod changes

## Installation

### Prerequisites

- Kubernetes cluster (v1.26+)
- `kubectl` configured to talk to the cluster
- [vk-cocoon](https://github.com/cocoonstack/vk-cocoon) virtual kubelet provider running on at least one node

### Deploy

1. Install the CRDs:

```bash
kubectl apply -f deploy/crd.yaml
kubectl apply -f deploy/cocoonset-crd.yaml
```

2. Deploy the operator (includes RBAC and Deployment):

```bash
kubectl apply -f deploy/deploy.yaml
```

3. Verify the operator is running:

```bash
kubectl get pods -l app=cocoon-operator
```

### Build from source

```bash
git clone https://github.com/cocoonstack/cocoon-operator.git
cd cocoon-operator
make build          # produces ./cocoon-operator
```

Or with Docker:

```bash
docker build -t cocoon-operator .
```

## Usage

### Hibernate a pod

```yaml
apiVersion: cocoon.cis/v1alpha1
kind: Hibernation
metadata:
  name: hibernate-bot-1
  namespace: prod
spec:
  podName: sre-agent-xxx
  action: hibernate
```

### Manage a VM group

```yaml
apiVersion: cocoon.cis/v1alpha1
kind: CocoonSet
metadata:
  name: demo
spec:
  agent:
    image: ubuntu-dev-base
    replicas: 1
    resources:
      cpu: "2"
      memory: "4Gi"
  toolboxes:
    - name: windows
      os: windows
      image: windows-server-2022
      mode: static
```

### Manifests

| File | Description |
|---|---|
| `deploy/crd.yaml` | Hibernation CRD |
| `deploy/cocoonset-crd.yaml` | CocoonSet CRD |
| `deploy/deploy.yaml` | Operator Deployment + RBAC |

## Development

```bash
make build          # build binary
make test           # run tests
make lint           # run golangci-lint
make fmt            # format code
make help           # show all targets
```

## Related Projects

| Project | Role |
|---|---|
| [vk-cocoon](https://github.com/cocoonstack/vk-cocoon) | Virtual kubelet provider that performs the actual VM lifecycle |
| [epoch](https://github.com/cocoonstack/epoch) | Snapshot storage backend for hibernated VMs |
| [glance](https://github.com/cocoonstack/glance) | Web UI that surfaces CocoonSet and Hibernation state |
| [cocoon-webhook](https://github.com/cocoonstack/cocoon-webhook) | Admission webhook for sticky scheduling |

## License

[MIT](LICENSE)
