# Code Walkthrough

## The Big Picture

This project is a **pluggable pipeline execution system** built on top of `fn_graph`. The idea: take any `fn_graph` pipeline (a DAG of Python functions), and run it with configurable executors (in-memory, Docker, Lambda) and a persistent artifact store — without changing the pipeline definition at all.

---

## File 1: `fn_graph/examples/machine_learning.py` — The Pipeline Definition

**What to show:** Lines 19–167

This is a pure data science pipeline — no orchestration knowledge whatsoever. Just Python functions where **argument names declare dependencies**:

```python
def model(training_features, training_target, model_type):
    ...
def predictions(model, test_features):
    ...
def confusion_matrix(predictions, test_target):
    ...
```

At the bottom (line 144), a `Composer()` is built by calling `.update()` with all the functions and `.update_parameters()` for inputs (`model_type`, `do_preprocess`). `fn_graph` infers the DAG automatically from the function signatures.

**Key point:** This file never changes. The orchestration layer wraps it from the outside.

---

## File 2: `fn_graph/examples/solution/machine_learning_config.yaml` — Runtime Configuration

**What to show:** The whole file (it's tiny)

```yaml
pipeline:
  run_id: ml_run_001

artifact_store:
  type: fs
  base_dir: ./artifacts

nodes:
  "*":
    executor: memory
```

This YAML controls **where** each node runs and **where** results are stored. The `"*"` wildcard means "all nodes use the memory executor". You can override individual nodes (e.g. `model: executor: docker`) to route heavy work to Docker or Lambda without changing any Python.

---

## File 3: `executor/base.py` + `executor/memory.py` + `executor/docker.py` — The Executor Abstraction

**What to show:** `base.py` first, then `memory.py`, then `docker.py`

`BaseExecutor` defines a single method contract:

```python
def execute(self, node_name, fn, kwargs) -> Any
```

`InMemoryExecutor` (`memory.py`) just calls `fn(**kwargs)` directly in-process. Simplest possible implementation.

`DockerExecutor` (`docker.py`) is the interesting one:
1. Spins up a Docker container (`docker run -d -p {port}:8000`)
2. Polls `/health` until it's ready
3. Serializes the function source + inputs with `cloudpickle`, sends via HTTP POST to `/execute`
4. Deserializes the result back
5. Stops and removes the container in a `finally` block

**Key point:** Every executor looks identical to `PipelineComposer` — it just calls `.execute()`. Swapping Docker for Lambda or a remote worker needs zero changes to the orchestration logic.

---

## File 4: `artifact_store/base.py` + `artifact_store/fs.py` — The Artifact Store Abstraction

**What to show:** `base.py` interface, then `fs.py` implementation

`BaseArtifactStore` defines: `get`, `put`, `exists`, `delete`, `metadata`.

`LocalFSArtifactStore` (`fs.py`) stores each node's output as a `cloudpickle` file:

```
artifacts/{run_id}/{node_name}.pkl
```

It does an **atomic write** (write to `.tmp`, then `os.replace`) to avoid partial reads. There is also an S3 version (`artifact_store/s3.py`) with the exact same interface for cloud deployments.

**Key point:** `exists()` is what enables memoization — if the `.pkl` is already there from a prior run, the node is skipped entirely.

---

## File 5: `fn_graph/examples/solution/config.py` — The Factory / Wiring Layer

**What to show:** `get_executor()` (line 25) and `get_artifact_store()` (line 41)

This reads the YAML config and instantiates the right executor/store objects. It's a simple factory:
- `type == "memory"` → `InMemoryExecutor`
- `type == "docker"` → `DockerExecutor`
- `type == "lambda"` → `LambdaExecutor`

`get_node_config()` (line 58) handles the wildcard fallback — checks for a specific node name first, then falls back to `"*"`, then defaults to `memory`.

---

## File 6: `fn_graph/examples/solution/composer.py` — The Core: `PipelineComposer`

**What to show:** `__init__`, `_copy`, and `calculate()` — the whole file

This is the heart of the system. It subclasses `fn_graph`'s `Composer` and overrides `calculate()`.

**`__init__`** (line 21): Takes `execution_config` (node → executor mapping) and `artifact_store` on top of the standard Composer args.

**`_copy`** (line 36): Critical override — whenever `fn_graph` internally clones a composer (on `.update()`, `.link()`, etc.), this ensures `execution_config` and `artifact_store` survive the copy. Without this, they'd silently disappear.

**`calculate()`** (line 54) — the main algorithm:
1. **Trims the DAG** to only the subgraph needed for requested outputs (`ancestor_dag`)
2. **Seeds parameters** into the artifact store so all nodes load inputs uniformly
3. **Memoization check** — if `artifact_store.exists(node_name)`, skip it entirely
4. **Parallel execution** — uses `ThreadPoolExecutor` + `wait(FIRST_COMPLETED)` to implement topological wave execution: a node fires as soon as all its predecessors are done, not in serial order
5. **Dispatches** each node to its configured executor via the `execute_node()` inner function

---

## File 7: `fn_graph/examples/solution/run_pipeline.py` — The Entry Point

**What to show:** `main()` from line 62

This is the CLI glue:
1. Parses `--pipeline` (dotted module path) and `--config` (YAML path)
2. **Dynamically imports** the pipeline module and grabs its `f` (Composer) object
3. Applies a pandas compatibility patch if needed (line 89)
4. Sets up **file logging** via the `_Tee` class — duplicates stdout to a log file under `logs/{pipeline}/{run_id}/`
5. **Reconstructs** the plain `Composer` as a `PipelineComposer` by copying its internal state (`_functions`, `_parameters`, etc.) — this is what keeps the pipeline definition file untouched
6. Finds **leaf nodes** (out-degree == 0, not parameters) — these are the final outputs
7. Calls `pipeline.calculate(leaf_outputs)` and saves figures as PNGs

---

## How It All Connects (the flow)

```
run_pipeline.py
  → imports machine_learning.py        (gets the DAG)
  → reads machine_learning_config.yaml (gets executor/store config)
  → wraps Composer as PipelineComposer
  → calls calculate(leaf_outputs)
      → for each node in topological order (parallel):
          → artifact_store.exists()? → skip (memoization)
          → get_executor(node_config).execute(fn, inputs)
              → memory: fn(**kwargs) directly
              → docker: spin up container, HTTP POST, deserialize
          → artifact_store.put(result)
```

---

## The Design Principle Worth Highlighting

The pipeline (`machine_learning.py`) and the infrastructure (`executor/`, `artifact_store/`, `config.yaml`) are **completely decoupled**. You can take any existing `fn_graph` pipeline and run it on Docker, Lambda, or locally just by changing the YAML — no code changes needed in the pipeline itself.
