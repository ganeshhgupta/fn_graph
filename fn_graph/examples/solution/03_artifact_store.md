# 03 - Artifact Store

## What the artifact store does

Every node produces an output. The next node needs it. Something has to hold it in between.

```mermaid
flowchart LR
    A[iris node] -->|put iris, result| B[(Artifact Store)]
    B -->|get iris| C[data node]
    C -->|put data, result| B
    B -->|get data| D[preprocess_data node]
    D -->|put preprocess_data, result| B
    B --> E[...]
```

Right now that is a folder of pkl files on disk. Works locally. Breaks the moment two nodes run on different machines, different containers, or Lambda functions with no shared filesystem.

---

## Base interface

```mermaid
classDiagram
    class BaseArtifactStore {
        +get(key) object
        +put(key, value)
        +delete(key)
        +exists(key) bool
        +metadata(key) dict
    }
    class LocalFSArtifactStore {
        +get(key) object
        +put(key, value)
        +delete(key)
        +exists(key) bool
        +metadata(key) dict
    }
    class S3ArtifactStore {
        +get(key) object
        +put(key, value)
        +delete(key)
        +exists(key) bool
        +metadata(key) dict
    }
    class ElastiCacheArtifactStore {
        +get(key) object
        +put(key, value)
        +delete(key)
        +exists(key) bool
        +metadata(key) dict
    }
    BaseArtifactStore <|-- LocalFSArtifactStore
    BaseArtifactStore <|-- S3ArtifactStore
    BaseArtifactStore <|-- ElastiCacheArtifactStore
```

Five methods. Every backend implements these five. The composer only ever calls these five. Whether the data lands on disk or in S3 is invisible to everything else.

---

## Three backends

```mermaid
flowchart TD
    A[BaseArtifactStore] --> B[LocalFSArtifactStore\nbuild now]
    A --> C[S3ArtifactStore\nnext]
    A --> D[ElastiCacheArtifactStore\nlater]
    style B fill:#d4edda,stroke:#155724
    style C fill:#fff3cd,stroke:#856404
    style D fill:#f8d7da,stroke:#721c24
```

Get the interface right now. Adding S3 later is filling in those five methods. Nothing else changes.

---

## LocalFSArtifactStore

```mermaid
flowchart LR
    A["put('model', obj)"] --> B["cloudpickle.dump\n./artifacts/run-id/model.pkl"]
    C["get('model')"] --> D["cloudpickle.load\n./artifacts/run-id/model.pkl"]
    E["exists('model')"] --> F["os.path.exists()"]
    G["metadata('model')"] --> H["os.stat()\nsize, mtime"]
```

---

## S3ArtifactStore

Same interface, boto3 underneath.

```mermaid
sequenceDiagram
    participant PipelineComposer
    participant S3ArtifactStore
    participant S3

    PipelineComposer->>S3ArtifactStore: put("model", trained_model)
    S3ArtifactStore->>S3ArtifactStore: cloudpickle serialize
    S3ArtifactStore->>S3: PUT s3://bucket/run-id/model.pkl
    S3-->>S3ArtifactStore: 200 OK

    PipelineComposer->>S3ArtifactStore: get("model")
    S3ArtifactStore->>S3: GET s3://bucket/run-id/model.pkl
    S3-->>S3ArtifactStore: bytes
    S3ArtifactStore-->>PipelineComposer: trained_model
```

---

## Run IDs

Every pipeline run gets a unique ID. Artifacts are stored under it.

```mermaid
flowchart TD
    A[artifacts/] --> B[run_abc123/]
    A --> C[run_def456/]
    B --> D[iris.pkl]
    B --> E[data.pkl]
    B --> F[model.pkl]
    C --> G[iris.pkl]
    C --> H[...]
```

Multiple runs never overwrite each other. Debugging a failed run means reading its artifacts without touching the current one.

Pass the same `run_id` to reuse outputs from a previous run even if the executor changes.

```mermaid
flowchart LR
    A[run_abc123\nmodel ran on Lambda\nmodel.pkl in S3] -->|same run_id\nnew run on local| B[predictions runs locally\nreads model.pkl from S3\nskips rerunning model]
```

---

## Artifact path convention

Path is derived from convention, not config. Artifact store backend decides where files go based on the environment.

```mermaid
flowchart TD
    A["artifact_store.put('model', result)"] --> B{which backend?}
    B -->|LocalFS| C["./artifacts/{run_id}/model.pkl"]
    B -->|S3| D["s3://bucket/{run_id}/model.pkl"]
    B -->|ElastiCache| E["cache key: {run_id}:model"]
```

Same call everywhere. Backend resolves the physical location. No paths in config.

---

## Memoization

Before running any node, check if its output already exists.

```mermaid
flowchart TD
    A[next node in topo order] --> B{artifact_store.exists\nnode_name?}
    B -->|yes| C[skip execution\nload from store]
    B -->|no| D[run node\nput result in store]
    C --> E[next node]
    D --> E
```

If a pipeline crashes at node 7 of 12, the next run with the same `run_id` picks up from node 8. Nodes that already succeeded are skipped because their artifacts exist.

---

## What gets written to the store (stage-based execution)

With stage partitioning, not every node writes to the store. Only two categories are persisted:

```mermaid
flowchart TD
    A[node result computed] --> B{category?}
    B -->|boundary output\nsuccessor in different stage| C[artifact_store.put]
    B -->|leaf output\nout-degree 0 in DAG| C
    B -->|internal node\nboth predecessor and successor\nin the same stage| D[stays in memory\nnever serialized]
```

For a chain A → B → C within one stage where only C is consumed by the next stage: A and B produce zero pkl files. Only C is written to disk.

Pass `--debug-artifacts` to override this and persist every node — useful for inspecting intermediate results without modifying the pipeline or YAML.

---

## Switching backends

```mermaid
flowchart LR
    A[pipeline_config.yaml] --> B[ArtifactStoreFactory]
    B -->|artifact_store: fs| C[LocalFSArtifactStore]
    B -->|artifact_store: s3| D[S3ArtifactStore]
    B -->|artifact_store: elasticache| E[ElastiCacheArtifactStore]
```

```yaml
artifact_store:
  default: s3
  bucket: my-pipeline-bucket
```

One setting for the whole pipeline. Artifact store is not per node. Executor is per node. These two are intentionally separate.

---

## Serialization

```mermaid
flowchart LR
    A[Python object\nDataFrame, model, array] -->|put| B[cloudpickle\nprotocol 4]
    B --> C[(backend)]
    C -->|get| D[deserialize]
    D --> E[Python object]
```

Protocol 4, not 5. Protocol 5 is Python 3.8+. If any environment runs an older version, protocol 5 breaks silently. Protocol 4 is safe across versions.

Lambda has a 6MB payload limit. Cloudpickle bytes from a large model can exceed that. When that becomes a real problem, the store swaps serialization per backend. Callers do not change.

---

## Key Notes

- Artifact store is one setting for the whole pipeline, not per node. Executor varies per node. Artifact store does not.
- `put()` writes atomically: temp file then rename. Never write in chunks without a finalization step. S3 `put_object` is atomic by default.
- `metadata()` returns artifact size. The orchestrator can use this to make routing decisions, for example not caching a 500MB model in ElastiCache which has a 1GB per-key limit.
- The `exists()` check is what makes resume and memoization work. Build this correctly from day one.
