# fn_graph — Isolated Docker Runner

This example runs every node of a function graph in its **own separate Docker
container**.  Nodes communicate only through serialised files in a shared
directory (`fn_graph_context/`).  No node can see another node's memory,
imports, or process state.

It is a learning example: by the end you will understand

* what fn_graph is and how it builds a DAG from plain Python functions,
* how a topological sort turns a graph into a safe execution order,
* how data is handed between containers using cloudpickle,
* why functions are shipped as source code (not bytecode) when Python versions
  differ across environments, and
* how Docker bind-mounts let containers share files with the host.

---

## What the example computes

A classic machine-learning pipeline on the [Iris dataset](https://scikit-learn.org/stable/auto_examples/datasets/plot_iris_dataset.html):

```
iris
 └─► data
      └─► preprocess_data ◄── do_preprocess (parameter)
           └─► split_data
                ├─► training_features ─┐
                ├─► training_target   ─┼─► model ◄── model_type (parameter)
                ├─► test_features    ──┼──► predictions
                └─► test_target      ──┴──► classification_metrics
                                           confusion_matrix
```

Each box is one Python function.  Each function runs in a **fresh Docker
container** that boots, does its one job, writes a `.pkl` file, and exits.

---

## Prerequisites

### 1  Install Docker Desktop

Docker Desktop is the easiest way to run containers on Windows or macOS.

1. Go to https://www.docker.com/products/docker-desktop and download the
   installer for your OS.
2. Run the installer and follow the prompts (default options are fine).
3. Launch Docker Desktop.  Wait until the whale icon in your system tray
   shows **"Docker Desktop is running"**.
4. Verify the install worked:
   ```
   docker --version
   ```
   You should see something like `Docker version 27.x.x`.

> **Linux users** — install Docker Engine instead:
> https://docs.docker.com/engine/install/
> Then add yourself to the `docker` group so you can run containers without
> `sudo`: `sudo usermod -aG docker $USER` (log out and back in after).

### 2  Install Python packages on the host

The orchestrator (`run_isolated.py`) runs on your machine, not inside a
container.  It needs fn_graph, cloudpickle, and networkx:

```bash
pip install fn_graph cloudpickle networkx
```

> You do **not** need scikit-learn, pandas, or numpy on the host — those only
> run inside the containers.

---

## Running the example

All commands are run from this directory:

```bash
cd fn_graph/examples/docker_isolated
```

### Step 1 — Build the worker image

The `Dockerfile` describes the Linux environment every container will use.
You only need to do this once (or again if you change `worker.py` or
`requirements.txt`):

```bash
docker build -t fn_graph_worker .
```

What this does:

* Downloads `python:3.11-slim` (a minimal Debian image with Python 3.11).
* Installs the Python packages listed in `requirements.txt` into the image.
* Copies `worker.py` into the image at `/app/worker.py`.
* Tags the finished image as `fn_graph_worker`.

This takes a minute or two the first time.  Subsequent builds are fast because
Docker caches the `pip install` layer.

### Step 2 — Run the pipeline

```bash
python run_isolated.py
```

You will see the DAG structure, then one block of output per container as
each node executes in topological order.  The final output is:

```
CLASSIFICATION METRICS:
              precision    recall  f1-score   support
           0       1.00      1.00      1.00        14
           ...

CONFUSION MATRIX:
[[14  0  0]
 [ 0 16  0]
 [ 0  0  8]]
```

---

## File-by-file walkthrough

```
docker_isolated/
├── Dockerfile          # blueprint for every worker container
├── requirements.txt    # packages installed into the image
├── worker.py           # the script that runs INSIDE each container
└── run_isolated.py     # the orchestrator that runs on YOUR machine
```

### `Dockerfile`

```dockerfile
FROM python:3.11-slim          # start from a minimal Python 3.11 image
WORKDIR /app                   # all subsequent commands happen in /app
COPY requirements.txt .        # copy the dependency list into the image
RUN pip install -r requirements.txt   # install dependencies (cached layer)
COPY worker.py .               # copy the worker script into the image
```

The image is built once and reused for every container.  The only thing that
changes between containers is the arguments passed to `worker.py`.

### `requirements.txt`

The packages installed inside the container.  The host machine does not need
these — only the containers do.

### `worker.py`

This script runs **inside each container**.  It receives three command-line
arguments:

| argv | example | meaning |
|------|---------|---------|
| `argv[1]` | `/ctx` | path to the shared context directory |
| `argv[2]` | `split_data` | name of the node to execute |
| `argv[3]` | `/ctx/_fn_split_data.py` | source file containing the function |

Its job:

1. Read the function's source code from `argv[3]` and `exec()` it to bring
   the function into scope.
2. Inspect the function's signature to find its input argument names.
3. Load each input from `/ctx/<name>.pkl` using cloudpickle.
4. Call the function with those inputs.
5. Serialise the return value to `/ctx/<node_name>.pkl` using cloudpickle.

**Why source code instead of a pickled function?**
Python's bytecode format (the compiled instructions stored inside `.pyc` files
and `code` objects) changes between minor versions.  cloudpickle serialises
a function *including its bytecode*.  If the host runs Python 3.14 and the
container runs Python 3.11, loading the pickle causes a segfault — no
exception, just a crash — because 3.11 encounters 3.14 opcodes it does not
understand.  Shipping plain `.py` source text avoids this entirely: the
container compiles the source itself with its own Python.

### `run_isolated.py`

This is the orchestrator.  It runs on your machine.  Here is what it does,
phase by phase.

#### Phase 1 — define the DAG with fn_graph

```python
f = (
    Composer()
    .update_parameters(model_type="ols", do_preprocess=True)
    .update(iris, data, preprocess_data, ...)
)
```

`Composer` inspects each function's **argument names** to work out
dependencies.  `data(iris)` depends on `iris`.  `model(training_features,
training_target, model_type)` depends on those three.  No explicit wiring is
needed — the argument names *are* the wiring.

#### Phase 2 — topological sort

```python
topo_order = list(nx.topological_sort(dag))
# → ['model_type', 'do_preprocess', 'iris', 'data', 'preprocess_data', ...]
```

A topological sort orders the nodes so that every node appears **after all its
dependencies**.  This guarantees that when a container runs, all its input
`.pkl` files already exist.

If the graph had a cycle (A depends on B which depends on A) a topological
sort is impossible — that is how fn_graph detects circular dependencies.

#### Phase 3 — seed parameters

Parameters (`model_type = "ols"`, `do_preprocess = True`) are not computed by
any function; they are fixed values supplied by the caller.  The orchestrator
writes them directly into the context directory before any container starts:

```
fn_graph_context/
  model_type.pkl   ← contains the string "ols"
  do_preprocess.pkl ← contains the boolean True
```

Later, when the `model` container runs, it finds `model_type.pkl` there just
like any other input.  From the container's perspective, there is no
difference between a parameter and the output of another node.

#### Phase 4 — one container per node

For each non-parameter node (in topological order):

```python
# 1. write the function's source to the context directory
source = inspect.getsource(fn)
with open(f"fn_graph_context/_fn_{node_name}.py", "w") as fh:
    fh.write(source)

# 2. run a container
subprocess.run([
    "docker", "run", "--rm",
    "-v", "/host/path/fn_graph_context:/ctx",  # bind-mount
    "fn_graph_worker",
    "python", "worker.py", "/ctx", node_name, f"/ctx/_fn_{node_name}.py"
])
```

`-v /host/path:/ctx` is a **bind-mount**: it makes the host directory visible
inside the container at `/ctx`.  Files written to `/ctx` by the container
immediately appear on the host at the real path, and vice versa.  This is the
only communication channel between containers — there is no network, no
shared memory, no database.

`--rm` tells Docker to delete the container as soon as it exits, keeping your
system tidy.

After the container exits, the orchestrator checks that the expected
`<node_name>.pkl` file was written.  If it is missing (or if the container
exited with a non-zero code) the pipeline stops immediately with an error.

#### Phase 5 — collect results

Once all containers have finished, the orchestrator reads back the final
`.pkl` files (`classification_metrics.pkl`, `confusion_matrix.pkl`, etc.) and
prints them.

---

## The context directory

After a successful run, `fn_graph_context/` contains one file per node:

```
fn_graph_context/
  model_type.pkl
  do_preprocess.pkl
  _fn_iris.py              ← source shipped to iris container
  iris.pkl                 ← output of iris container
  _fn_data.py
  data.pkl
  _fn_preprocess_data.py
  preprocess_data.pkl
  _fn_split_data.py
  split_data.pkl
  _fn_training_features.py
  training_features.pkl
  ... (and so on for all 12 nodes)
```

You can inspect any `.pkl` file from Python:

```python
import cloudpickle
with open("fn_graph_context/data.pkl", "rb") as f:
    df = cloudpickle.load(f)
print(df.head())
```

---

## Concepts at a glance

| Concept | What it does here |
|---------|-------------------|
| **fn_graph `Composer`** | Builds a DAG by reading function argument names |
| **Topological sort** | Guarantees correct execution order |
| **cloudpickle** | Serialises Python data (DataFrames, models, arrays) to files |
| **Docker bind-mount** | Shares a host directory with a container at `/ctx` |
| **`--rm`** | Auto-deletes the container after it exits |
| **Source-code shipping** | Avoids Python bytecode version mismatch |
| **Parameter seeding** | Pre-writes fixed values so containers treat them like any input |

---

## Changing parameters

To use an SVM instead of logistic regression, change the `update_parameters`
call in `run_isolated.py`:

```python
.update_parameters(model_type="svm", do_preprocess=True)
```

Delete the old context files so stale values are not reused:

```bash
rm -rf fn_graph_context
python run_isolated.py
```

---

## Troubleshooting

**`docker: command not found`**
Docker Desktop is not installed or not running.  Start Docker Desktop and wait
for it to report "running".

**`docker build` fails on `pip install`**
You may be behind a proxy or have no internet access.  Check your Docker
Desktop network settings.

**A container exits with a non-zero code**
The orchestrator prints everything the container wrote to stdout and stderr.
Read the `[worker:<node>] FAILED:` line and the traceback below it.

**`ERROR: missing input '<name>'`**
A predecessor node did not produce its output `.pkl`.  Check that node's
container output for errors.

**The pipeline hangs**
A container may be waiting on a resource (rare).  Press Ctrl-C, run
`docker ps` to list running containers, and `docker kill <id>` to stop them.

**Stale results from a previous run**
Delete `fn_graph_context/` and re-run.  The directory accumulates `.pkl` files
across runs; old files from a changed pipeline can confuse things.
