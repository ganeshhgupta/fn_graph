# 01 - Extending fn_graph

## What fn_graph gives us

You write plain Python functions. fn_graph reads their argument names and builds a dependency graph automatically.

```python
def data(iris):          # depends on iris
def model(training_features, training_target, model_type):  # depends on these three
```

```mermaid
flowchart LR
    A[iris] --> B[data]
    B --> C[preprocess_data]
    P1([do_preprocess]) --> C
    C --> D[split_data]
    D --> E[training_features]
    D --> F[training_target]
    D --> G[test_features]
    D --> H[test_target]
    E --> I[model]
    F --> I
    P2([model_type]) --> I
    G --> J[predictions]
    I --> J
    J --> K[classification_metrics]
    J --> L[confusion_matrix]
    H --> K
    H --> L
```

No explicit wiring. Argument names are the wiring.

---

## Three things we use from fn_graph

```mermaid
flowchart TD
    A[Composer] --> B["dag()\nthe dependency graph\nnodes + edges"]
    A --> C["functions()\nname -> callable\nfor every registered function"]
    A --> D["parameters()\nname -> value\nmodel_type=ols, do_preprocess=True"]
```

These three methods are the only surface we touch. Everything inside fn_graph stays untouched.

---

## Topological sort

`dag()` gives us the graph. We run topological sort on it to get a flat execution order where every node appears after all its dependencies.

```mermaid
flowchart LR
    A["dag()"] -->|nx.topological_sort| B["[model_type, do_preprocess,\niris, data, preprocess_data,\nsplit_data, training_features,\ntraining_target, test_features,\ntest_target, model, predictions,\nclassification_metrics, confusion_matrix]"]
```

This guarantees that when a node runs, all its inputs already exist.

---

## The problem with the current setup

`run_isolated.py` does everything itself. One script, one hardwired path.

```mermaid
flowchart TD
    A[run_isolated.py] --> B[read DAG]
    B --> C[topological sort]
    C --> D[loop through nodes]
    D --> E[subprocess.run\ndocker run worker.py]
    E --> F[wait for container to exit]
    F --> G[check pkl file appeared]
    G --> D
```

Want to test without Docker? Can't. Want Lambda? Rewrite the script. There is no seam between "what order nodes run" and "how each node executes."

---

## The fix: PipelineComposer

We subclass fn_graph's `Composer`. It inherits everything. The only thing we override is what happens when it is time to run a node.

```mermaid
classDiagram
    class Composer {
        +dag()
        +functions()
        +parameters()
        +update()
        +update_parameters()
        +calculate()
    }
    class PipelineComposer {
        +execution_config
        +run()
        -_execute_node()
    }
    Composer <|-- PipelineComposer
    note for PipelineComposer "inherits all DAG logic\nonly overrides node execution"
```

Currently:

```python
run_node_in_docker(node_name, funcs[node_name])
```

With PipelineComposer:

```python
executor.execute(node_name, fn, kwargs)
```

Which executor runs which node is controlled by a config file. The orchestration logic never changes regardless of where nodes run.

---

## Before vs after

```mermaid
flowchart TD
    subgraph BEFORE
        A[run_isolated.py] -->|hardwired\nsubprocess docker run| B[Docker only]
    end

    subgraph AFTER
        C[PipelineComposer] --> D{per-node\nexecutor config}
        D --> E[InMemoryExecutor]
        D --> F[DockerExecutor]
        D --> G[LambdaExecutor]
    end
```

---

## What PipelineComposer does step by step

```mermaid
sequenceDiagram
    participant run_isolated.py
    participant PipelineComposer
    participant ArtifactStore
    participant Executor

    run_isolated.py->>PipelineComposer: run()
    PipelineComposer->>PipelineComposer: dag(), functions(), parameters()
    Note over PipelineComposer: topo order: iris -> data -> ... -> model -> predictions

    PipelineComposer->>ArtifactStore: put("model_type", "ols")
    PipelineComposer->>ArtifactStore: put("do_preprocess", True)

    loop each node in topo order
        PipelineComposer->>ArtifactStore: exists(node_name)?
        ArtifactStore-->>PipelineComposer: yes -> skip / no -> continue
        PipelineComposer->>ArtifactStore: get inputs for this node only
        ArtifactStore-->>PipelineComposer: kwargs
        PipelineComposer->>Executor: execute(node_name, fn, kwargs)
        Executor-->>PipelineComposer: result
        PipelineComposer->>ArtifactStore: put(node_name, result)
    end

    PipelineComposer-->>run_isolated.py: results
```

---

## What we own vs what fn_graph owns

```mermaid
block-beta
    columns 2
    A["fn_graph owns\n\nDAG construction\ntopological sort\nfunction introspection\nparameter resolution"]:1
    B["we own\n\nPipelineComposer\nexecutor dispatch\nartifact store reads/writes\nper-node executor config\nmemoization\nparallelism\nretry and failure handling"]:1
    style A fill:#d4edda,stroke:#28a745
    style B fill:#cce5ff,stroke:#004085
```

---

## Folder layout

```mermaid
flowchart TD
    A[pipeline/] --> B[pipeline_config.yaml\nexecutor and store config]
    A --> C[composer.py\nPipelineComposer]
    A --> D[executor/\nsee 02_executor.md]
    A --> E[artifact_store/\nsee 03_artifact_store.md]
    A --> F[config.py\nreads YAML\nreturns right executor per node]
```

---

## Key Notes

- We call three methods on fn_graph: `dag()`, `functions()`, `parameters()`. That is the entire dependency surface. fn_graph version changes are unlikely to break anything on our side.
- `PipelineComposer` replaces `Composer()` in `run_isolated.py`. Nothing else in that file changes.
- Executor and ArtifactStore are each covered in their own docs. This file is only about the composer layer.
