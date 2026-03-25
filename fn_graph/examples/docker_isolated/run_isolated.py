"""
fn_graph isolated Docker runner.

Each DAG node runs in its own Docker container.
Outputs are shared via cloudpickle files in a local context directory
that is bind-mounted into every container at /ctx.

Key design decision: functions are shipped as SOURCE CODE (.py files), not
cloudpickle bytecode, so the host Python version (3.14) and the container
Python version (3.11) can differ without causing segfaults.
"""
import inspect
import os
import sys
import subprocess
import textwrap
import cloudpickle
import networkx as nx
from fn_graph import Composer

# Pickle protocol 4 is supported by Python 3.4+ — safe for cross-version
# transfer (host Python 3.14 → container Python 3.11).
PICKLE_PROTOCOL = 4


# ---------------------------------------------------------------------------
# DAG functions — all imports are LOCAL for self-contained exec in containers
# ---------------------------------------------------------------------------

def iris():
    import sklearn.datasets
    return sklearn.datasets.load_iris()


def data(iris):
    import pandas as pd
    df = pd.DataFrame(iris.data, columns=[f"feature{i}" for i in range(4)])
    return df.assign(y=iris.target)


def preprocess_data(data, do_preprocess):
    import sklearn.preprocessing
    processed = data.copy()
    if do_preprocess:
        processed.iloc[:, :-1] = sklearn.preprocessing.scale(processed.iloc[:, :-1])
    return processed


def split_data(preprocess_data):
    from sklearn.model_selection import train_test_split
    return dict(zip(
        ("training_features", "test_features", "training_target", "test_target"),
        train_test_split(preprocess_data.iloc[:, :-1], preprocess_data["y"]),
    ))


def training_features(split_data):
    return split_data["training_features"]


def training_target(split_data):
    return split_data["training_target"]


def test_features(split_data):
    return split_data["test_features"]


def test_target(split_data):
    return split_data["test_target"]


def model(training_features, training_target, model_type):
    import sklearn.linear_model
    import sklearn.svm
    if model_type == "ols":
        m = sklearn.linear_model.LogisticRegression()
    elif model_type == "svm":
        m = sklearn.svm.SVC()
    else:
        raise ValueError(f"invalid model_type={model_type!r}, choose 'ols' or 'svm'")
    m.fit(training_features, training_target)
    return m


def predictions(model, test_features):
    return model.predict(test_features)


def classification_metrics(predictions, test_target):
    import sklearn.metrics
    return sklearn.metrics.classification_report(test_target, predictions)


def confusion_matrix(predictions, test_target):
    import sklearn.metrics
    return sklearn.metrics.confusion_matrix(test_target, predictions)


# ---------------------------------------------------------------------------
# Build the composer
# ---------------------------------------------------------------------------

f = (
    Composer()
    .update_parameters(model_type="ols", do_preprocess=True)
    .update(
        iris, data, preprocess_data, split_data,
        training_features, training_target, test_features, test_target,
        model, predictions, classification_metrics, confusion_matrix,
    )
)

# ---------------------------------------------------------------------------
# Context store — a local directory bind-mounted at /ctx in every container
# ---------------------------------------------------------------------------

CONTEXT_DIR = os.path.abspath("./fn_graph_context")
os.makedirs(CONTEXT_DIR, exist_ok=True)


def _docker_volume_path(host_path: str) -> str:
    """Convert a Windows path to Docker-friendly forward-slash form."""
    if sys.platform == "win32":
        host_path = host_path.replace("\\", "/")
        if len(host_path) >= 2 and host_path[1] == ":":
            # C:/Users/... -> /c/Users/...
            host_path = "/" + host_path[0].lower() + host_path[2:]
    return host_path


# ---------------------------------------------------------------------------
# Per-node container runner
# ---------------------------------------------------------------------------

def run_node_in_docker(node_name: str, fn):
    """Write the function's source to /ctx, spawn a container, verify output."""

    # Write function source (not a pickle) to avoid cross-Python bytecode issues
    source = textwrap.dedent(inspect.getsource(fn))
    src_path = os.path.join(CONTEXT_DIR, f"_fn_{node_name}.py")
    with open(src_path, "w", encoding="utf-8") as fh:
        fh.write(source)

    ctx_volume = _docker_volume_path(CONTEXT_DIR)
    cmd = [
        "docker", "run", "--rm",
        "-v", f"{ctx_volume}:/ctx",
        "fn_graph_worker",
        "python", "worker.py", "/ctx", node_name, f"/ctx/_fn_{node_name}.py",
    ]

    print(f"  [docker] starting container for node: {node_name}", flush=True)
    result = subprocess.run(cmd, capture_output=True, text=True)

    # Always show stdout (worker prints everything there)
    if result.stdout.strip():
        for line in result.stdout.strip().splitlines():
            print(f"  [container] {line}", flush=True)

    # Always show stderr (any unexpected low-level crash output)
    if result.stderr.strip():
        print(f"  [container stderr] ---", flush=True)
        for line in result.stderr.strip().splitlines():
            print(f"  [container stderr] {line}", flush=True)

    if result.returncode != 0:
        print(f"  [docker] CONTAINER FAILED (exit {result.returncode}) for node: {node_name}", flush=True)
        sys.exit(1)

    out_pkl = os.path.join(CONTEXT_DIR, f"{node_name}.pkl")
    if not os.path.exists(out_pkl):
        print(f"  [docker] ERROR: container exited 0 but {node_name}.pkl was NOT written!", flush=True)
        sys.exit(1)

    print(f"  [docker] container finished OK for node: {node_name}", flush=True)


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------

def run_dag_isolated(composer):
    dag    = composer.dag()
    funcs  = composer.functions()
    # parameters() returns {name: (type, value)} — extract just the value
    params = {name: val for name, (_, val) in composer.parameters().items()}

    # ── Step 1: show the DAG ────────────────────────────────────────────────
    print("\n" + "=" * 60)
    print("STEP 1: DAG STRUCTURE")
    print("=" * 60)
    topo_order = list(nx.topological_sort(dag))
    print(f"  Nodes ({len(dag.nodes)}): {list(dag.nodes)}")
    print(f"  Edges ({len(dag.edges)}): {list(dag.edges)}")
    print(f"  Execution order: {topo_order}")

    # ── Step 2: seed parameters ─────────────────────────────────────────────
    print("\n" + "=" * 60)
    print("STEP 2: SEEDING PARAMETERS INTO CONTEXT STORE")
    print("=" * 60)
    for name, value in params.items():
        pkl_path = os.path.join(CONTEXT_DIR, f"{name}.pkl")
        with open(pkl_path, "wb") as fh:
            cloudpickle.dump(value, fh, protocol=PICKLE_PROTOCOL)
        print(f"  [context] seeded parameter: {name} = {value!r}", flush=True)

    # ── Step 3: one container per node ──────────────────────────────────────
    print("\n" + "=" * 60)
    print("STEP 3: EXECUTING NODES — ONE DOCKER CONTAINER PER NODE")
    print("=" * 60)
    for i, node_name in enumerate(topo_order):
        if node_name in params:
            print(f"\n  [{i+1}/{len(topo_order)}] SKIP  '{node_name}' (parameter — already in context)")
            continue

        deps = list(dag.predecessors(node_name))
        print(f"\n  [{i+1}/{len(topo_order)}] NODE  '{node_name}'")
        print(f"  [dag]    depends on: {deps if deps else '(none)'}")
        run_node_in_docker(node_name, funcs[node_name])
        print(f"  [status] '{node_name}' COMPLETE", flush=True)

    # ── Step 4: collect results ─────────────────────────────────────────────
    print("\n" + "=" * 60)
    print("STEP 4: COLLECTING RESULTS FROM CONTEXT STORE")
    print("=" * 60)
    results = {}
    for node_name in funcs:
        pkl = os.path.join(CONTEXT_DIR, f"{node_name}.pkl")
        if os.path.exists(pkl):
            with open(pkl, "rb") as fh:
                results[node_name] = cloudpickle.load(fh)
            print(f"  [loaded] {node_name}.pkl")
    return results


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    print("=" * 60)
    print("fn_graph ISOLATED DOCKER RUNNER")
    print("Each DAG node runs in its own Docker container.")
    print(f"Host Python:  {sys.version.split()[0]}")
    print(f"Context store: {CONTEXT_DIR}")
    print("=" * 60)

    results = run_dag_isolated(f)

    print("\n" + "=" * 60)
    print("STEP 5: FINAL OUTPUT")
    print("=" * 60)
    print("\nCLASSIFICATION METRICS:")
    print(results["classification_metrics"])
    print("\nCONFUSION MATRIX:")
    print(results["confusion_matrix"])
    print("\nMODEL USED:", results["model"])
    print("\nDone. All 12 function nodes executed in isolated Docker containers.")
