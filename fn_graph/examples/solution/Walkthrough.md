# Code Walkthrough

## The Big Picture

This project is a **pluggable pipeline execution system** built on top of `fn_graph`. The idea: take any `fn_graph` pipeline (a DAG of Python functions), and run it with configurable executors (in-memory, Docker, persistent Docker, Lambda), a persistent artifact store, and optional stage partitioning — without changing the pipeline definition at all.

The system has two execution modes:

- **Node-level** (fallback): every node writes to and reads from the artifact store individually.
- **Stage-based** (when `stages:` is defined in config): nodes are grouped into stages. Results pass in memory within a stage — only boundary nodes (those consumed by a different stage) are persisted to disk. Stages dispatch in parallel where the DAG allows.

The orchestration code lives in `solution/`. The Docker worker image and compose files live in `deploy/` — kept separate because they are infrastructure, not orchestration logic.

---

## File 1: `fn_graph/examples/machine_learning.py` — The Pipeline Definition

This is a pure data science pipeline — no orchestration knowledge whatsoever. Just Python functions where **argument names declare dependencies**:

```python
def model(training_features, training_target, model_type):
    ...
def predictions(model, test_features):
    ...
def confusion_matrix(predictions, test_target):
    ...
```

At the bottom, a `Composer()` is built by calling `.update()` with all the functions and `.update_parameters()` for inputs (`model_type`, `do_preprocess`). `fn_graph` infers the DAG automatically from the function signatures.

**This file never changes.** The orchestration layer wraps it from the outside.

---

## File 2: `machine_learning_*.yaml` — Runtime Configuration

Two representative configs:

**`machine_learning_local.yaml`** — all stages run in-memory, no Docker:
```yaml
pipeline:
  run_id: ml_run_local_001
  on_failure: finish_running

artifact_store:
  type: fs
  base_dir: ./artifacts

stages:
  preprocessing:
    executor: memory
    nodes: [iris, data, preprocess_data, investigate_data]
  splitting:
    executor: memory
    nodes: [split_data, training_features, training_target, test_features, test_target]
  training:
    executor: memory
    nodes: [model]
  evaluation:
    executor: memory
    nodes: [predictions, classification_metrics, confusion_matrix]
```

**`machine_learning_multi_persistent.yaml`** — each stage routes to its own pre-hosted Docker container:
```yaml
pipeline:
  run_id: ml_run_multi_persistent_001
  on_failure: finish_running

artifact_store:
  type: fs
  base_dir: ./artifacts

stages:
  preprocessing:
    executor: persistent_docker
    url: http://localhost:8001
    nodes: [iris, data, preprocess_data, investigate_data]
  splitting:
    executor: persistent_docker
    url: http://localhost:8002
    nodes: [split_data, training_features, training_target, test_features, test_target]
  training:
    executor: persistent_docker
    url: http://localhost:8003
    nodes: [model]
  evaluation:
    executor: persistent_docker
    url: http://localhost:8004
    nodes: [predictions, classification_metrics, confusion_matrix]
```

The `stages:` block activates stage-based execution — nodes in the same stage share memory, only boundary nodes touch disk. The `nodes:` block is the fallback for configs without stages.

---

## File 3: `config.py` — The Factory / Wiring Layer

Four functions, each with one job:

**`load_config(yaml_path)`** — reads and parses the YAML file, prints run_id and store type.

**`get_executor(node_config)`** — instantiates the right executor from a node config dict:
- `executor == "memory"` → `InMemoryExecutor()`
- `executor == "docker"` → `DockerExecutor(image=...)`
- `executor == "persistent_docker"` → `PersistentDockerExecutor(url=..., timeout=...)`
- `executor == "lambda"` → `LambdaExecutor(function_name=..., region=...)` *(lazy import — boto3 not required at startup)*

**`get_artifact_store(config)`** — instantiates the right store:
- `type == "fs"` → `LocalFSArtifactStore(base_dir, run_id)`
- `type == "s3"` → `S3ArtifactStore(bucket, run_id, region)` *(lazy import — boto3 not required at startup)*

**`get_node_config(config, node_name)`** — resolves which executor config applies to a node. Checks for a specific node name first, then falls back to the `"*"` wildcard, then defaults to memory.

**`load_stages(config)`** *(added with stage partitioning)* — reads the `stages:` key from config and returns a dict of stage definitions. Returns empty dict if no stages are defined, so the caller falls back to node-level execution transparently.

---

## File 4: `executor/base.py` + `executor/memory.py` + `executor/docker.py` + `executor/persistent_docker.py` — The Executor Abstraction

`BaseExecutor` defines a single method contract:

```python
def execute(self, node_name, fn, kwargs) -> Any
```

`InMemoryExecutor` just calls `fn(**kwargs)` directly in-process.

`DockerExecutor` is the ephemeral-container variant:
1. Picks a free port with `_free_port()`
2. Spins up a Docker container: `docker run -d -p {port}:8000`
3. Polls `GET /health` every 0.5s until ready (30s timeout)
4. Serializes inputs with `cloudpickle`, sends `POST /execute` with `{node_name, module, kwargs_b64}`
5. Deserializes the result from the HTTP response body
6. Stops and removes the container in a `finally` block (always cleaned up, even on error)

`PersistentDockerExecutor` is the pre-hosted variant — no container lifecycle management:
1. Assumes the target container is already running (started externally via `docker compose up`)
2. Sends `POST /execute` with `{node_name, module, kwargs_b64}` directly to `self.url`
3. The container resolves the function via `importlib.import_module(module)` + `getattr(mod, node_name)` — no source code travels over the wire
4. Deserializes and returns the result
5. On HTTP 500, logs the full traceback from the response body and raises `RuntimeError`

Every executor looks identical to the orchestrator — it just calls `.execute()`. Swapping between executors needs zero changes to orchestration logic.

---

## File 5: `executor/stage_executor.py` — Runs a Whole Stage In-Memory

*(Added with stage partitioning)*

`StageExecutor` receives a stage definition and runs all its nodes without touching the artifact store for internal results.

**`__init__`**: takes stage_name, stage_def, node_functions, stage_inputs (values already loaded from the store by the orchestrator), resolve_predecessors_fn (fn_graph's own `_resolve_predecessors`), artifact_store, stage_output_nodes (the boundary set), and ancestor_dag.

**`run()`** — the main method:
1. Determines topological order for nodes in this stage by filtering the ancestor DAG to only this stage's nodes.
2. Initialises an in-memory results dict with the provided stage inputs.
3. For each node in order:
   - Looks up its predecessors using fn_graph's `_resolve_predecessors` (so namespace resolution is handled correctly).
   - Loads kwargs from the in-memory results dict — no artifact store reads for internal nodes.
   - Determines the executor from the stage's executor config.
   - Calls `executor.execute(node_name, fn, kwargs)`.
   - Stores the result in-memory.
   - If the node is a stage output (boundary), also writes it to the artifact store so the next stage can load it.

**Key point:** The artifact store is written to only for `stage_output_nodes`. Everything else stays in the `results` dict and is garbage-collected when the stage finishes.

---

## File 6: `artifact_store/base.py` + `artifact_store/fs.py` — The Artifact Store Abstraction

`BaseArtifactStore` defines: `get`, `put`, `exists`, `delete`, `metadata`.

`LocalFSArtifactStore` stores each node's output as a `cloudpickle` file:

```
artifacts/{run_id}/{node_name}.pkl
```

It does an **atomic write** (write to `.tmp`, then `os.replace`) to avoid partial reads if the process crashes mid-write. There is also an S3 version (`artifact_store/s3.py`) with the exact same interface.

`exists()` is what enables memoization — at the node level it checks per-node, at the stage level the orchestrator checks all of a stage's boundary outputs before deciding to skip the whole stage.

---

## File 7: `composer.py` — The Core: `PipelineComposer`

This is the heart of the system. It subclasses fn_graph's `Composer` and overrides `calculate()`.

**`__init__`**: takes `execution_config`, `artifact_store`, and `pipeline_config` (the full parsed YAML dict) on top of the standard Composer args. `pipeline_config` is needed so `calculate()` can read the `stages:` key.

**`_copy`**: critical override — whenever fn_graph internally clones a composer (on `.update()`, `.link()`, etc.), this ensures `execution_config`, `artifact_store`, and `pipeline_config` all survive the copy. Without this they'd silently disappear.

**`_analyze_stage_boundaries(stages, ancestor_dag)`** *(added with stage partitioning)*:
- Builds a node→stage reverse map.
- For each stage, scans predecessors and successors of every node in the ancestor DAG.
  - Predecessor from a different stage → that predecessor is an **input** to this stage (must be loaded from store).
  - Successor in a different stage → the current node is a **boundary output** (must be saved to store).
  - Everything else is **internal** (memory only).
- Builds a stage-level NetworkX DiGraph from these boundary relationships.
- Prints the full analysis so it's visible in the log.

**`calculate()`** — the main algorithm:
1. Falls back to `super().calculate()` if no artifact_store (plain Composer behaviour preserved).
2. Calls `self.functions()` and `self.parameters()` from fn_graph's interface.
3. Trims the DAG with `self.ancestor_dag(outputs)`.
4. Seeds parameters into the artifact store.
5. **Branches**: if `stages:` are defined in config, calls `_calculate_with_stages()`. Otherwise runs the node-level parallel loop.

**`_calculate_with_stages(stages, ancestor_dag, ...)`** *(added with stage partitioning)*:
1. Calls `_analyze_stage_boundaries()`.
2. Finds all stages with no predecessors in the stage DAG — these are the initial ready set.
3. Submits all ready stages to `ThreadPoolExecutor` simultaneously.
4. Loops with `wait(FIRST_COMPLETED)`: as each stage finishes, checks its successors in the stage DAG. If all a successor's predecessors are done, dispatches it immediately.
5. Each stage runs via `run_stage()` inner function which checks memoization, loads inputs from the store, and hands off to `StageExecutor`.
6. Collects all results from the artifact store and returns them.

---

## File 8: `deploy/worker/server.py` — The FastAPI Worker (runs inside Docker)

Lives in `deploy/worker/` — separate from the orchestration layer in `solution/` because it is infrastructure, not orchestration logic.

```
GET  /health  → {"status": "ok"}
POST /execute → runs the node, returns result
               body: { node_name, module, kwargs_b64 }
```

`POST /execute` receives `node_name`, `module` (the dotted Python module path, e.g. `fn_graph.examples.machine_learning`), and `kwargs_b64` (cloudpickle-serialized inputs, base64-encoded).

The worker resolves and runs the function entirely via the Python import system:
1. `mod = importlib.import_module(module)` — imports the pre-installed pipeline module
2. `fn = getattr(mod, node_name)` — retrieves the function by name
3. `kwargs = cloudpickle.loads(base64.b64decode(kwargs_b64))` — deserializes inputs
4. `result = fn(**kwargs)` — runs it
5. Returns `{"result_b64": base64(cloudpickle.dumps(result))}`
6. On any exception, returns HTTP 500 with full traceback in the response body

**No `exec()`, no source code over the wire.** The pipeline module (`fn_graph`) is baked into the Docker image at build time (see `deploy/worker/Dockerfile`). The `Dockerfile` copies the `fn_graph/` package into `/app/fn_graph/` and sets `PYTHONPATH=/app`, so `importlib.import_module("fn_graph.examples.machine_learning")` works inside the container.

This means changing a pipeline function requires rebuilding the image — which is the correct tradeoff: the image is the deployment unit, not a string sent over HTTP.

---

## File 9: `run_pipeline.py` — The Entry Point

**`main()`**:
1. Parses `--pipeline` (dotted module path) and `--config` (YAML path).
2. Dynamically imports the pipeline module and grabs its `f` (Composer) object.
3. Applies a pandas compatibility patch if needed (finance pipeline).
4. Sets up file logging via the `_Tee` class — duplicates stdout to `logs/{pipeline}/{run_id}/{timestamp}.log`.
5. Calls `get_artifact_store(config)` and builds `execution_config` per node via `get_node_config`.
6. **Reconstructs** the plain `Composer` as a `PipelineComposer` by copying its internal state (`_functions`, `_parameters`, etc.) and injecting `execution_config`, `artifact_store`, and `pipeline_config=config`. This is what keeps the pipeline definition file untouched.
7. Finds **leaf nodes** (out-degree == 0, not parameters) — these are the final outputs.
8. Calls `pipeline.calculate(leaf_outputs)`.
9. Prints scalar/string results, saves matplotlib figures as PNGs.

---

## How It All Connects (stage-based flow)

```
run_pipeline.py
  → imports machine_learning.py                  (gets the DAG)
  → reads machine_learning_multi_persistent.yaml (gets stages, executor config, store config)
  → wraps Composer as PipelineComposer
  → calls calculate(leaf_outputs)
      → ancestor_dag + topological sort
      → seed parameters to artifact store
      → load_stages() → stages defined
      → _analyze_stage_boundaries()              (prints boundary analysis)
      → _calculate_with_stages()
          → ThreadPoolExecutor dispatches independent stages in parallel
          → for each stage (run_stage):
              → memoized? → skip entire stage
              → load boundary inputs from artifact store
              → StageExecutor.run()
                  → for each node in topo order (in-memory):
                      → _resolve_predecessors() (fn_graph's own wiring)
                      → executor.execute(fn, kwargs)
                          → memory: fn(**kwargs) directly
                          → docker: spin container → POST /execute → stop
                          → persistent_docker: POST /execute to pre-running container
                            (container resolves fn via importlib, no exec())
                      → store result in-memory
                      → if boundary node: artifact_store.put()
              → downstream stages unblocked → dispatch immediately
```

---

## The Design Principles Worth Highlighting

**1. Pipeline and infrastructure are completely decoupled.**
`machine_learning.py` has no knowledge of Docker, Lambda, artifact stores, or stages. You can take any existing fn_graph pipeline and add orchestration purely through config.

**2. Stage boundaries are the only serialization points.**
Within a stage, data flows as Python objects. Disk is only touched where one stage hands off to another — the natural checkpoints in the pipeline.

**3. The fn_graph interface contract is honoured throughout.**
`PipelineComposer` inherits all of `Composer`'s methods (`dag()`, `graphviz()`, `check()`, `update()`, `link()`, etc.) unchanged. The only overrides are `__init__`, `_copy`, and `calculate()`. All DAG traversal and dependency resolution is delegated to fn_graph's own methods (`ancestor_dag`, `_resolve_predecessors`).

**4. Infrastructure lives in `deploy/`, orchestration lives in `solution/`.**
The worker image, Dockerfiles, and compose files are deployment artifacts. They are built and managed independently from the orchestration code. Executor plugins (`persistent_docker.py`, `docker.py`, `lambda_executor.py`) live in `solution/executor/` because they are client-side HTTP clients, not infrastructure.

**5. No source code travels over the wire.**
`PersistentDockerExecutor` and `DockerExecutor` both send `{node_name, module, kwargs_b64}`. The worker resolves the function via the Python import system from its pre-installed package. No `exec()`, no string-to-code execution surface.
