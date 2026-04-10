# cocoon-operator

Kubernetes operator that manages VM-backed pod lifecycles through two CRDs: **Hibernation** for suspending or waking a single VM pod, and **CocoonSet** for managing a group of related VM pods.

## Overview

- **Hibernation controller** -- watches `Hibernation` CRDs and annotates pods with `vm.cocoonstack.io/hibernate=true` (also mirrored to the legacy `cocoon.cis/hibernate` key during the rename window) to trigger vk-cocoon snapshot and restore
- **CocoonSet controller** -- watches `CocoonSet` CRDs, creates and deletes agent and toolbox pods, manages suspend and unsuspend, and reports aggregate status
- **Pod watcher** -- detects pod changes owned by CocoonSets and triggers reconciliation

Kubernetes controllers assume pods are replaceable. VM-backed workloads are not. This operator keeps stateful VM workflows inside native Kubernetes APIs: scale out from a known main VM, keep stable slot identities, hibernate without letting ReplicaSets recreate the pod, and expose aggregate state through CRD status.

## Architecture

The operator runs a single binary with three informer loops:

1. **Hibernation controller** -- annotates pods for vk-cocoon snapshot and restore
2. **CocoonSet controller** -- manages agent and toolbox pod groups
3. **Pod watcher** -- triggers reconciliation on pod changes

A 30-second informer resync and 60-second periodic reconciliation catch status transitions that informer events may miss.

## Installation

### Prerequisites

- Kubernetes cluster (v1.26+)
- `kubectl` configured to talk to the cluster
- [vk-cocoon](https://github.com/cocoonstack/vk-cocoon) virtual kubelet provider running on at least one node

### Download

Download pre-built binaries from [GitHub Releases](https://github.com/cocoonstack/cocoon-operator/releases).

### Build from source

```bash
git clone https://github.com/cocoonstack/cocoon-operator.git
cd cocoon-operator
make build          # produces ./cocoon-operator
```

### Deploy

1. Install the CRDs:

```bash
kubectl apply -f deploy/crd.yaml
kubectl apply -f deploy/cocoonset-crd.yaml
```

2. Deploy the operator:

```bash
kubectl apply -f deploy/deploy.yaml
```

3. Verify the operator is running:

```bash
kubectl get pods -l app=cocoon-operator
```

## Configuration

| Variable | Default | Description |
|---|---|---|
| `KUBECONFIG` | `~/.kube/config` | Path to kubeconfig when running outside the cluster |
| `LOG_LEVEL` | `info` | Log level for the operator process |

### Annotation namespace

The operator writes managed-Pod annotations under two cocoonstack.io subdomains:

| Prefix | Meaning | Examples |
|---|---|---|
| `cocoonset.cocoonstack.io/` | Declarative fields mirrored from a CocoonSet spec | `mode`, `image`, `os`, `storage`, `snapshot-policy`, `network`, `managed` |
| `vm.cocoonstack.io/` | Runtime state observed about the VM backing a Pod | `id`, `name`, `ip`, `vnc-port`, `hibernate`, `fork-from` |

For the duration of the legacy `cocoon.cis/*` migration the operator **dual-writes** every annotation: the canonical key under cocoonstack.io plus its `cocoon.cis/*` mirror, in the same merge patch. Providers (like vk-cocoon) that have not yet caught up to the rename keep reading the legacy key; providers that have caught up read the canonical key. Both stay in sync because every reconcile rewrites both keys to the same value. The legacy mirror will be retired once every consumer is on the new prefix; see [cocoon-common's `meta` package docs](https://github.com/cocoonstack/cocoon-common#meta) for the helper API.

The CRD API group (`cocoon.cis/v1alpha1`) and the selector labels (`cocoon.cis/cocoonset` etc.) stay on the legacy prefix in this round — renaming the API group requires migrating every existing CocoonSet CR, and renaming the selector labels requires a coordinated cutover so in-flight Pods are not orphaned by their owner controllers. Both will move under cocoonstack.io in dedicated follow-ups.

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
| `deploy/deploy.yaml` | Operator Deployment and RBAC |

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
| [cocoon-common](https://github.com/cocoonstack/cocoon-common) | Shared metadata, Kubernetes, and logging helpers |
| [cocoon-webhook](https://github.com/cocoonstack/cocoon-webhook) | Admission webhook for sticky scheduling |
| [epoch](https://github.com/cocoonstack/epoch) | Snapshot storage backend for hibernated VMs |
| [vk-cocoon](https://github.com/cocoonstack/vk-cocoon) | Virtual kubelet provider that performs the actual VM lifecycle |

## License

[MIT](LICENSE)
