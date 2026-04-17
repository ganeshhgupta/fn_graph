# 05 - Failure and Retry

## What can go wrong

```mermaid
flowchart TD
    A[node starts running] --> B{what happens?}
    B -->|success| C[DONE]
    B -->|function throws exception| D[FAILED]
    B -->|executor times out\ncontainer crash\nLambda timeout| E[FAILED]
    B -->|artifact store write fails| F[FAILED]
```

All three map to the same FAILED state. The retry and failure logic handles them identically.

---

## Retry logic

Retry config lives per node in the YAML.

```yaml
nodes:
  model:
    executor: lambda
    retry:
      max_attempts: 3
      delay_seconds: 5
      backoff: exponential
```

```mermaid
flowchart TD
    A[node FAILED] --> B{attempts left?}
    B -->|yes| C[wait\n5s, 10s, 20s\nexponential backoff]
    C --> D[requeue as READY]
    D --> E[execute again]
    E --> F{result?}
    F -->|success| G[DONE]
    F -->|failure| B
    B -->|no| H[BLOCKED\nmax retries exhausted]
```

Exponential backoff matters for Lambda and network calls. Immediate retry on a timeout will just timeout again.

---

## What happens to downstream nodes when a node is BLOCKED

They never enter the ready queue. The join condition requires all predecessors to be DONE. A BLOCKED node never reaches DONE. So everything downstream is implicitly cancelled.

```mermaid
flowchart TD
    A[split_data BLOCKED] --> B[training_features\nnever enters ready queue]
    A --> C[training_target\nnever enters ready queue]
    A --> D[test_features\nnever enters ready queue]
    A --> E[test_target\nnever enters ready queue]
    B --> F[model\nnever enters ready queue]
    C --> F
    F --> G[predictions\nnever enters ready queue]
    G --> H[classification_metrics\nnever enters ready queue]
    G --> I[confusion_matrix\nnever enters ready queue]

    style A fill:#f8d7da,stroke:#721c24
    style B fill:#fff3cd,stroke:#856404
    style C fill:#fff3cd,stroke:#856404
    style D fill:#fff3cd,stroke:#856404
    style E fill:#fff3cd,stroke:#856404
    style F fill:#fff3cd,stroke:#856404
    style G fill:#fff3cd,stroke:#856404
    style H fill:#fff3cd,stroke:#856404
    style I fill:#fff3cd,stroke:#856404
```

Red: blocked. Yellow: implicitly cancelled. No special logic needed. Falls out naturally from the join condition.

---

## Pipeline-level failure behavior

What happens to the rest of the pipeline when a node is blocked is controlled by one setting.

```yaml
on_failure: fail_fast
```

```mermaid
flowchart TD
    A[on_failure setting] --> B{which mode?}
    B -->|fail_fast| C[cancel all running nodes\nstop immediately\nreport which node failed]
    B -->|finish_running| D[let currently running nodes finish\ndo not start any new nodes\nreport at end]
    B -->|continue| E[skip failed branch\nrun all independent branches\nreport partial results]
```

When to use each:

- `fail_fast`: downstream results are meaningless without the failed node. Stop early, fix the issue.
- `finish_running`: some branches are independent and their results are still useful. Let them finish.
- `continue`: failed node is optional enrichment. Rest of the pipeline is still valid.

---

## Mid-pipeline resume

Because of memoization and run IDs, resume is free. No special logic.

With **stage-based execution**, resume is coarser but faster — an entire stage is skipped if all its boundary output nodes already exist in the artifact store. A single failed node in a stage means the whole stage re-runs on resume (only boundary outputs and leaf outputs are persisted; intra-stage progress is not saved).

```mermaid
flowchart TD
    A[pipeline crashes at node 7] --> B[fix the issue]
    B --> C[rerun with same run_id]
    C --> D{artifact_store.exists\nfor each stage boundary?}
    D -->|stage fully memoized| E[skip entire stage]
    D -->|boundary outputs missing| F[run stage from scratch]
    E --> G[pipeline continues from first non-memoized stage]
    F --> G
```

---

## Full failure flow with retry and resume

```mermaid
sequenceDiagram
    participant Orchestrator
    participant Executor
    participant ArtifactStore

    Orchestrator->>Executor: execute("model", fn, kwargs)
    Executor-->>Orchestrator: FAILED attempt 1
    Note over Orchestrator: wait 5s
    Orchestrator->>Executor: execute("model", fn, kwargs)
    Executor-->>Orchestrator: FAILED attempt 2
    Note over Orchestrator: wait 10s
    Orchestrator->>Executor: execute("model", fn, kwargs)
    Executor-->>Orchestrator: FAILED attempt 3
    Note over Orchestrator: max retries exhausted\nmark model BLOCKED\napply on_failure policy

    Note over Orchestrator,ArtifactStore: next run, same run_id

    Orchestrator->>ArtifactStore: exists("preprocess_data")?
    ArtifactStore-->>Orchestrator: yes — skip preprocessing stage
    Orchestrator->>ArtifactStore: exists("training_features"), exists("test_features")...?
    ArtifactStore-->>Orchestrator: yes — skip splitting stage
    Note over Orchestrator: skips memoized stages
    Orchestrator->>Executor: execute("model", fn, kwargs)
    Executor-->>Orchestrator: success
    ArtifactStore-->>Orchestrator: model.pkl written
    Note over Orchestrator: continues from training stage onward
```

---

## Node state transitions including failure

```mermaid
stateDiagram-v2
    [*] --> PENDING
    PENDING --> READY: all dependencies DONE
    READY --> RUNNING: dispatched to executor
    RUNNING --> DONE: success\nartifact written
    RUNNING --> FAILED: exception or timeout
    FAILED --> READY: retry, attempts remaining
    FAILED --> BLOCKED: max retries exhausted
    BLOCKED --> [*]: downstream cascade cancelled
    DONE --> [*]: downstream nodes unblocked
```

---

## Key Notes

- Retry config is per node. A Lambda function that frequently times out can have more retries than a local in-memory function that almost never fails.
- Downstream cancellation is automatic. No code needed to propagate failure. The join condition handles it.
- Resume is free because of memoization. Same run_id, artifacts that exist get skipped.
- With stages, resume granularity is per stage, not per node. A stage either fully re-runs or is fully skipped.
- `on_failure: continue` is useful when the pipeline has genuinely independent branches where partial results are still valuable.
- Always log which nodes were DONE, which were BLOCKED, and which were implicitly cancelled at the end of a failed run. Makes debugging fast.
