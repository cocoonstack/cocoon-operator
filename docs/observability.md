# Observability

Reconciler failures surface as K8s Events on the CR plus Prometheus counters on the controller-runtime `/metrics` endpoint:

| Event reason (CocoonHibernation) | Type |
|---|---|
| `HibernateTimedOut`, `WakeTimedOut` | Warning |
| `Hibernated`, `WokenActive`, `RetryRequested` | Normal |

| Event reason (CocoonSet) | Type |
|---|---|
| `PodLifecycleFailed`, `MainAgentFailed`, `SubAgentDeadLetter` | Warning |
| `SubAgentRebuilding`, `RecoveredFromFailure` | Normal |

Metrics:

```
cocoon_operator_subagent_rebuild_total{namespace, cocoonset}
cocoon_operator_subagent_dead_letter_total{namespace, cocoonset}
cocoon_operator_hibernate_phase_duration_seconds{result}    # result=ok|timeout
cocoon_operator_wake_phase_duration_seconds{result}
cocoon_operator_lifecycle_state_failed_observed_total{phase}
```

`CocoonSet` consumes the `vm.cocoonstack.io/lifecycle-state=Failed` annotation that vk-cocoon writes on terminal failures (hibernate, wake, post-clone, SAC); the operator treats it as terminal on every owned pod role (main, sub-agent, toolbox) so reconciliation reacts immediately instead of waiting for `Pod.Status.Phase` to follow. `triageSubAgent` rebuilds a terminal sub pod up to four times with `0/1/5/30 s` exponential backoff between attempts, then marks the pod `cocoonset.cocoonstack.io/dead-letter=true` and leaves it in place so a permanently broken slot stops consuming the apiserver budget. Rebuild count persists in the `cocoonset.cocoonstack.io/rebuild-history` annotation on the CocoonSet so the count survives the pod delete; entries for slots beyond the current `spec.agent.replicas` are garbage-collected on every write.
