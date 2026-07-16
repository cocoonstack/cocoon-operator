# CocoonHibernation reconcile loop

| Spec.Desire | What the reconciler does | Terminal phase |
|---|---|---|
| `Hibernate` | `meta.HibernateState(true).Apply` on the target pod, then poll `Registry.HasManifest(vmName, meta.HibernateSnapshotTag)` until the snapshot lands or `hibernateTimeout` (3 minutes) trips. A probe error (transport / 5xx / auth) surfaces as a returned error so controller-runtime logs + retries with backoff. | `Hibernated` |
| `Wake` | Clear `meta.HibernateState` if the annotation is still set (idempotent — the patch is skipped once already cleared, so requeue cycles don't spam informer events), then wait for the container to be `Running` **with a freshly written VMID** before dropping the hibernation snapshot tag from the registry. A wake that does not complete within `wakeTimeout` (5 minutes) is escalated to `Phase=Failed` with a dated message in the `Ready` condition. | `Active` |

On CR deletion the reconciler runs a finalizer (`cocoonhibernation.cocoonset.cocoonstack.io/finalizer`) that clears the `:hibernate` tag from the registry (if `Status.VMName` is set) before removing itself, so deleting a CocoonHibernation never leaves an orphaned snapshot on the registry.

There is no `cocoon-vm-snapshots` ConfigMap bridge — the registry is the single source of truth for hibernation state. Failure paths set `Phase=Failed` with a one-shot message in the `Ready` condition instead of looping forever on a bad reference. Both `Hibernate` and `Wake` Failed phases are recoverable: on re-entry from a non-deadline phase the reconciler refreshes the Ready condition's `LastTransitionTime` so the budget resets cleanly (without the override, `apimeta.SetStatusCondition` would preserve the stale timestamp across the `False → False` transition and the recovered phase would trip the deadline on the next reconcile). Each retry emits a `RetryRequested` Normal Event so the recovery is visible in `kubectl describe`.

See [Observability](observability.md) for the full Event reason and
metrics list.
