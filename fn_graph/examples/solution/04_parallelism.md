# 04 - Parallelism

## Two levels of parallelism

The system supports parallelism at two granularities:

1. **Stage-level parallelism** — independent stages dispatch simultaneously via `ThreadPoolExecutor`
2. **Node-level parallelism** — within a stage (or without stages), independent nodes run concurrently

Both are automatic. Neither requires manual configuration. The DAG decides.

---

## The problem with a simple loop

Right now nodes execute one at a time in a flat loop.

```mermaid
flowchart LR
    A[iris] --> B[data] --> C[preprocess_data] --> D[split_data] --> E[training_features] --> F[training_target] --> G[test_features] --> H[test_target] --> I[model] --> J[predictions] --> K[classification_metrics] --> L[confusion_matrix]
```

After `split_data` finishes, these four nodes have no dependency on each other:

```mermaid
flowchart TD
    A[split_data] --> B[training_features]
    A --> C[training_target]
    A --> D[test_features]
    A --> E[test_target]
```

In the current setup they run one after the other anyway. That is wasted time.

---

## How parallelism is detected

We do not configure parallelism manually. The DAG already tells us which nodes are independent. Any node whose dependencies are all in the done set can run immediately.

```mermaid
flowchart TD
    A[node X] --> B{all predecessors\nin done set?}
    B -->|yes| C[add to ready queue]
    B -->|no| D[stay pending]
```

This is automatic for every pipeline, not just the iris example.

---

## Stage-level parallel dispatch

When stages are defined, the orchestrator builds a stage-level DAG from boundary node relationships. Independent stages dispatch simultaneously.

```mermaid
flowchart TD
    A[stage DAG built from\nboundary analysis] --> B[find stages with\nno unmet dependencies]
    B --> C[dispatch all ready stages\nvia ThreadPoolExecutor]
    C --> D{stage finishes}
    D --> E[check which downstream\nstages just unblocked]
    E --> F[dispatch newly ready stages]
    F --> D
```

In the ML pipeline the stages are linear (preprocessing → splitting → training → evaluation), so they run sequentially. In a pipeline with independent branches, those branches would dispatch simultaneously.

---

## Node-level queue system

Within a stage (or when no stages are configured), the orchestrator runs a ready queue over individual nodes.

```mermaid
flowchart TD
    A[start] --> B[find nodes with no deps\nadd to ready queue]
    B --> C{ready queue\nnot empty?}
    C -->|yes| D[pull node\ndispatch to executor\nmark RUNNING]
    D --> E{result?}
    E -->|success| F[mark DONE\nput artifact in store\ncheck which nodes just unblocked]
    F --> G[add newly unblocked nodes\nto ready queue]
    G --> C
    E -->|failure| H[see 05_failure_and_retry.md]
    C -->|no, but nodes still RUNNING| I[wait for a running node to finish]
    I --> E
    C -->|no, nothing running| J[pipeline done]
```

---

## Node states

Every node tracks its own state throughout the pipeline run.

```mermaid
stateDiagram-v2
    [*] --> PENDING
    PENDING --> READY: all dependencies done
    READY --> RUNNING: dispatched to executor
    RUNNING --> DONE: success
    RUNNING --> FAILED: exception
    FAILED --> READY: retry attempt
    FAILED --> BLOCKED: max retries exceeded
    BLOCKED --> [*]
    DONE --> [*]
```

---

## How the iris pipeline actually runs with parallelism

```mermaid
sequenceDiagram
    participant Orchestrator
    participant iris_executor
    participant data_executor
    participant split_data_executor
    participant tf_executor
    participant tt_executor
    participant tsf_executor
    participant tst_executor
    participant model_executor

    Orchestrator->>iris_executor: execute iris
    iris_executor-->>Orchestrator: done
    Orchestrator->>data_executor: execute data
    data_executor-->>Orchestrator: done
    Note over Orchestrator: preprocess_data, split_data run sequentially
    Orchestrator->>split_data_executor: execute split_data
    split_data_executor-->>Orchestrator: done

    par all four unblocked simultaneously
        Orchestrator->>tf_executor: training_features
        Orchestrator->>tt_executor: training_target
        Orchestrator->>tsf_executor: test_features
        Orchestrator->>tst_executor: test_target
    end

    tf_executor-->>Orchestrator: done
    tt_executor-->>Orchestrator: done
    tsf_executor-->>Orchestrator: done
    tst_executor-->>Orchestrator: done

    Orchestrator->>model_executor: execute model
    model_executor-->>Orchestrator: done
    Note over Orchestrator: predictions, metrics run in order
```

---

## Join condition

A node enters the ready queue only when every single one of its predecessors is in the done set. Not some. All.

```mermaid
flowchart TD
    A[training_features DONE] --> D{model: all deps done?}
    B[training_target DONE] --> D
    C[model_type DONE] --> D
    D -->|yes| E[model -> READY]
    D -->|no, waiting on others| F[model stays PENDING]
```

---

## What parallel execution looks like in the iris pipeline

```mermaid
gantt
    title iris pipeline execution with parallelism
    dateFormat X
    axisFormat %s

    section sequential
    iris           :0, 1
    data           :1, 2
    preprocess     :2, 3
    split_data     :3, 4

    section parallel
    training_features :4, 5
    training_target   :4, 5
    test_features     :4, 5
    test_target       :4, 5

    section sequential
    model              :5, 6
    predictions        :6, 7

    section parallel
    classification_metrics :7, 8
    confusion_matrix       :7, 8
```

`classification_metrics` and `confusion_matrix` both depend only on `predictions` and `test_target`. They also run in parallel.

---

## Key Notes

- Parallelism is automatic at both stage and node level. Never configure it. The DAG decides.
- The join condition is strict: all predecessors must be DONE, not just started.
- Worker threads dispatch to executors. Each executor handles its own network call or subprocess independently.
- Artifact store must handle concurrent writes safely. LocalFS uses atomic rename. S3 `put_object` is atomic by default.
- Stage-level parallelism is coarser but reduces artifact store I/O — only boundary output nodes and leaf output nodes (final requested outputs) are persisted; all intra-stage intermediates stay in memory.
