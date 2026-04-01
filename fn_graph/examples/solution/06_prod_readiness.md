# 06 - Production Readiness

## Context

The design in docs 01-05 was built and validated against the iris ML pipeline: 12 lightweight nodes, small data, fast execution. The production target is 80 heavy CNN modules doing inference on chest X-ray datasets. That difference changes the picture significantly.

This doc covers what stands between the current design and production.

---

## What breaks first

### Model loading per container

Right now each container starts fresh, runs the function, exits. For logistic regression that is fine. For a CNN checkpoint that is 200MB-2GB, loading from scratch per node per run means:

```mermaid
flowchart LR
    A[container starts] --> B[pull model from S3\n30-60 seconds]
    B --> C[run inference\n2-5 seconds]
    C --> D[container exits]
```

80 nodes. Each loading a model cold. Pipeline startup alone takes 40+ minutes before any real work happens.

### The pkl file assumption

Cloudpickle was designed for Python objects like DataFrames and sklearn models. CNN inference output is a tensor of shape `(batch, H, W, C)`. Chest X-rays are 2-50MB each. Across 80 nodes that is potentially hundreds of gigabytes of intermediate data sitting in pkl files.

Naive cloudpickle serialization on tensors is slow, memory-hungry, and will instantly hit Lambda's 6MB payload limit.

### Uncontrolled parallelism

The parallel queue dispatches all unblocked nodes simultaneously. With 80 CNN nodes and no resource awareness, all of them will compete for GPUs at once. The result is OOM crashes or severe thrashing.

---

## The 10 missing pieces

### 1. Warm container pools

CNN models cannot load cold per node. Need long-lived containers with models already in GPU memory.

```mermaid
flowchart TD
    A[DockerExecutor] --> B{warm container\navailable?}
    B -->|yes| C[send work directly\n0s model load]
    B -->|no| D[start container\nload model\n30-60s one-time cost]
    D --> E[add to warm pool]
    C --> F[run inference]
    E --> F
```

On Lambda this means provisioned concurrency. On Docker this means a persistent worker pool, not `--rm` containers. The `--rm` flag that cleans up containers after each node needs to become configurable.

---

### 2. Resource-aware scheduler

The parallel queue has no concept of GPU or memory constraints. Every node needs to declare what it requires. Scheduler only dispatches when those resources are actually free.

```yaml
nodes:
  cnn_module_1:
    executor: docker
    resources:
      gpu: 1
      memory_gb: 16
  cnn_module_2:
    executor: docker
    resources:
      gpu: 2
      memory_gb: 32
```

```mermaid
flowchart TD
    A[node enters ready queue] --> B{required resources\navailable?}
    B -->|yes| C[dispatch to executor\nreserve resources]
    B -->|no| D[wait in ready queue\ndo not block other nodes]
    C --> E[node finishes\nrelease resources]
    E --> F[check waiting nodes\ndispatch if now possible]
```

Without this, 80 CNN nodes dispatched simultaneously will crash the cluster.

---

### 3. Format-aware artifact serialization

Cloudpickle is the wrong tool for large tensors and model checkpoints. ArtifactStore needs pluggable serializers per artifact type.

```mermaid
flowchart TD
    A["artifact_store.put('cnn_output', tensor)"] --> B{what type?}
    B -->|small metadata, scalars| C[cloudpickle\nfine as-is]
    B -->|numpy arrays, tensors| D[numpy .npy or zarr\n10-100x faster]
    B -->|chest X-ray images| E[DICOM or HDF5]
    B -->|model checkpoints| F[torch.save\nnot cloudpickle]
```

Declared per node in config:

```yaml
nodes:
  cnn_module_1:
    output_format: zarr
  preprocessing:
    output_format: dicom
```

---

### 4. Batch processing

Right now the pipeline processes one input at a time. 1000 chest X-rays through 80 CNN modules one by one is not viable. Need a batch dimension where the same DAG runs over N inputs with dynamic batching per node based on available GPU memory.

```mermaid
flowchart LR
    A["1000 chest X-rays"] --> B[PipelineComposer\nbatch mode]
    B --> C[cnn_module_1\nbatch_size=32\nauto-tuned to GPU memory]
    C --> D[cnn_module_2\nbatch_size=16\nlarger model, smaller batch]
    D --> E[...]
```

Each node declares its preferred batch size. Orchestrator manages batching and reassembling results.

---

### 5. Data lineage and audit trail

Medical imaging is regulated. Every output needs a full traceable history.

```mermaid
flowchart LR
    A[input X-ray] -->|checksum logged| B[cnn_module_1]
    B -->|model hash logged\ncode commit logged\ntimestamp logged| C[output tensor]
    C --> D[audit store\nqueryable after the fact]
```

Run IDs alone are not enough. Need:
- Input data checksum per node
- Model checkpoint hash per run
- Code commit hash tied to each run
- Full query interface: "what produced this output?"

---

### 6. Observability

80 nodes, some on Lambda, some on Docker, some retrying. Print statements scattered across container stdouts are not enough.

```mermaid
flowchart TD
    A[every node execution] --> B[OpenTelemetry\ndistributed tracing]
    A --> C[per-node metrics\nlatency, memory, GPU utilization]
    A --> D[centralized logs\nnot scattered container stdout]
    B & C & D --> E[monitoring dashboard\nalerting on anomalies]
```

Specifically missing:
- Distributed tracing across nodes so you can see the full execution timeline in one place
- Per-node GPU utilization so you know which CNN modules are memory-bound
- Alerting when a node's latency spikes beyond a threshold

---

### 7. DAG partitioning by node weight

Lambda has a 15-minute timeout. Heavy CNN inference will exceed it. Some nodes belong on Lambda. Some belong on GPU EC2. The YAML config handles which executor per node, but right now that is manual.

Need automatic partitioning logic that groups nodes by compute profile and assigns executors accordingly.

```mermaid
flowchart TD
    A[80 CNN nodes] --> B[profiler\nestimate compute per node]
    B --> C{node weight}
    C -->|lightweight\npostprocessing| D[Lambda]
    C -->|medium\nclassification heads| E[Docker CPU]
    C -->|heavy\nfull CNN inference| F[GPU EC2]
```

---

### 8. Schema validation between nodes

Right now nodes accept whatever the upstream node wrote. No validation. If CNN module 12 outputs shape `(batch, 512, 512, 1)` but module 13 expects `(batch, 256, 256, 3)`, the pipeline crashes deep inside inference with a cryptic shape mismatch.

Need output schema declared per node, validated by the artifact store on `put()` before the next node ever reads it.

```yaml
nodes:
  cnn_module_12:
    output_schema:
      type: tensor
      shape: [batch, 512, 512, 1]
      dtype: float32
```

```mermaid
flowchart LR
    A[cnn_module_12 output] --> B[ArtifactStore.put\nvalidate against declared schema]
    B -->|schema mismatch| C[fail immediately\nclear error message]
    B -->|schema ok| D[write artifact\nnext node reads safely]
```

Catch shape mismatches at the boundary, not inside the next CNN module.

---

### 9. Secrets management

DICOM datasets, hospital S3 buckets, model registries. Containers and Lambda functions need credentials. No mechanism exists for this yet.

```mermaid
flowchart LR
    A[AWS Secrets Manager\nor Vault] --> B[ExecutorFactory]
    B --> C[injected into container\nas env vars at runtime]
    C --> D[never in config files\nnever in code\nnever in logs]
```

Credentials must never appear in the YAML config, in logs, or in artifact metadata.

---

### 10. Cost accounting

80 Lambda invocations per run, potentially hundreds of runs per day. No visibility into which nodes are expensive.

```mermaid
flowchart LR
    A[every node execution] --> B[log duration\nexecutor type\nresources used]
    B --> C[cost model\nper executor type]
    C --> D[per-node cost estimate\nper-run total cost\ncost trend over time]
```

Without this, you have no way to know whether CNN module 34 costs $0.001 or $2.00 per run, or whether switching it from Lambda to EC2 saves money.

---

## Priority order

The first four are blockers. The pipeline will not run on 80 CNN nodes without them.

```mermaid
flowchart TD
    A[Critical blockers] --> B[1. Warm container pools\nmodel load time]
    A --> C[2. Resource-aware scheduler\nGPU allocation]
    A --> D[3. Format-aware serialization\ntensor and DICOM support]
    A --> E[4. Batch processing\nthroughput]

    F[High priority] --> G[5. Data lineage\nregulatory requirement]
    F --> H[6. Observability\n80 nodes need tracing]
    F --> I[7. DAG partitioning\nLambda timeout risk]
    F --> J[8. Schema validation\nshape mismatches]

    K[Medium priority] --> L[9. Secrets management]
    K --> M[10. Cost accounting]
```

---

## Key Notes

- Docs 01-05 describe the correct architecture. Nothing in those docs needs to change. These 10 items are additions on top of that foundation, not replacements.
- Items 1-4 should be scoped into the first production milestone. Skipping any of them means the pipeline will not complete a full run on real chest X-ray data.
- Data lineage (item 5) may be a hard regulatory requirement depending on how the pipeline outputs are used clinically. Confirm this early.
