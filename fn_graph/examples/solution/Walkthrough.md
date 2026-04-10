# Code Walkthrough

## The Big Picture

This project is a **pluggable pipeline execution system** built on top of `fn_graph`. The idea: take any `fn_graph` pipeline (a DAG of Python functions), and run it with configurable executors (in-memory, Docker, Lambda), a persistent artifact store, and optional stage partitioning — without changing the pipeline definition at all.

The system has two execution modes:

- **Node-level** (fallback): every node writes to and reads from the artifact store individually.
- **Stage-based** (when `stages:` is defined in config): nodes are grouped into stages. Results pass in memory within a stage — only boundary nodes (those consumed by a different stage) are persisted to disk. Stages dispatch in parallel where the DAG allows.

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

## File 2: `machine_learning_config.yaml` — Runtime Configuration

```yaml
pipeline:
  run_id: ml_run_001
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
    executor: docker
    image: fn_graph_worker_v2
    nodes: [model]
  evaluation:
    executor: memory
    nodes: [predictions, classification_metrics, confusion_matrix]

nodes:
  model:
    executor: docker
    image: fn_graph_worker_v2
  "*":
    executor: memory
```

This YAML controls **where** each node runs and **where** results are stored. The `stages:` block activates stage-based execution — nodes in the same stage share memory, only stage boundary nodes touch disk. The `nodes:` block is the fallback for configs without stages.

---

## File 3: `config.py` — The Factory / Wiring Layer

Four functions, each with one job:

**`load_config(yaml_path)`** — reads and parses the YAML file, prints run_id and store type.

**`get_executor(node_config)`** — instantiates the right executor from a node config dict:
- `type == "memory"` → `InMemoryExecutor`
- `type == "docker"` → `DockerExecutor(image=...)`
- `type == "lambda"` → `LambdaExecutor(function_name=..., region=...)`

**`get_artifact_store(config)`** — instantiates the right store:
- `type == "fs"` → `LocalFSArtifactStore(base_dir, run_id)`
- `type == "s3"` → `S3ArtifactStore(bucket, run_id, region)`

**`get_node_config(config, node_name)`** — resolves which executor config applies to a node. Checks for a specific node name first, then falls back to the `"*"` wildcard, then defaults to memory.

**`load_stages(config)`** *(added with stage partitioning)* — reads the `stages:` key from config and returns a dict of stage definitions. Returns empty dict if no stages are defined, so the caller falls back to node-level execution transparently.

---

## File 4: `executor/base.py` + `executor/memory.py` + `executor/docker.py` — The Executor Abstraction

`BaseExecutor` defines a single method contract:

```python
def execute(self, node_name, fn, kwargs) -> Any
```

`InMemoryExecutor` just calls `fn(**kwargs)` directly in-process.

`DockerExecutor` is the interesting one:
1. Picks a free port with `_free_port()`
2. Spins up a Docker container: `docker run -d -p {port}:8000`
3. Polls `GET /health` every 0.5s until ready (30s timeout)
4. Serializes the function source + inputs with `cloudpickle`, sends via `POST /execute`
5. Deserializes the result from the HTTP response body
6. Stops and removes the container in a `finally` block (always cleaned up, even on error)

Every executor looks identical to the orchestrator — it just calls `.execute()`. Swapping Docker for Lambda or a remote worker needs zero changes to the orchestration logic.

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

## File 8: `worker/server.py` — The FastAPI Worker (runs inside Docker)

```python
GET  /health  → {"status": "ok"}
POST /execute → runs the node, returns result
```

`POST /execute` receives `node_name`, `fn_source` (the function's source code as a string), and `kwargs_b64` (cloudpickle-serialized inputs, base64-encoded).

It does NOT import the pipeline module. Instead:
1. `exec(fn_source, namespace)` — executes the function source into a fresh namespace pre-populated with sklearn, pandas, numpy, matplotlib, etc.
2. `fn = namespace[node_name]` — pulls the function out by name.
3. `kwargs = cloudpickle.loads(base64.b64decode(kwargs_b64))` — deserializes inputs.
4. `result = fn(**kwargs)` — runs it.
5. Returns `{"result_b64": base64(cloudpickle.dumps(result))}`.
6. On any exception, returns HTTP 500 with full traceback in the response body.

This design means the worker image is completely pipeline-agnostic — the same image runs any node from any fn_graph pipeline.

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
  → imports machine_learning.py          (gets the DAG)
  → reads machine_learning_config.yaml   (gets stages, executor config, store config)
  → wraps Composer as PipelineComposer
  → calls calculate(leaf_outputs)
      → ancestor_dag + topological sort
      → seed parameters to artifact store
      → load_stages() → stages defined
      → _analyze_stage_boundaries()      (prints boundary analysis)
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
