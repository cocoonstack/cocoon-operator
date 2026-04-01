# cocoon-operator

`cocoon-operator` adds higher-level Kubernetes workflows on top of `vk-cocoon`.

It provides two CRDs:

- `Hibernation`: suspend and wake a single VM-backed pod without deleting the pod object
- `CocoonSet`: manage a small group of related VM-backed pods, including a main VM, sub-VMs, and optional toolboxes

## Why It Exists

Kubernetes controllers assume pods are replaceable. VM-backed workloads often are not.

This operator keeps stateful VM workflows inside native Kubernetes APIs:

- scale out from a known main VM
- keep stable slot identities
- hibernate without letting ReplicaSets recreate the pod
- expose aggregate state through CRDs

## Included Manifests

- [deploy/crd.yaml](./deploy/crd.yaml): `Hibernation` CRD
- [deploy/cocoonset-crd.yaml](./deploy/cocoonset-crd.yaml): `CocoonSet` CRD
- [deploy/deploy.yaml](./deploy/deploy.yaml): controller deployment

## Example

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
```

## Related Components

- `vk-cocoon` performs the actual VM lifecycle
- `epoch` stores hibernated snapshots
- `glance` surfaces `CocoonSet` and `Hibernation` state in the UI
- `cocoon-webhook` is optional and improves sticky scheduling

## License

MIT
