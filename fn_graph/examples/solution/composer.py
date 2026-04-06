import threading
from concurrent.futures import ThreadPoolExecutor, wait, FIRST_COMPLETED

import networkx as nx

from fn_graph import Composer
from config import get_executor


class PipelineComposer(Composer):
    """
    Subclass of fn_graph's Composer that overrides calculate() to route each node
    through a pluggable executor and a persistent artifact store for memoization.

    Behaves identically to a regular Composer from the outside — all the same
    methods (update, update_parameters, link, dag, graphviz, etc.) work unchanged.
    Only the execution path differs: instead of calling functions directly,
    calculate() dispatches to whichever executor is configured per-node.
    """

    def __init__(self, *, execution_config=None, artifact_store=None, **kwargs):
        """
        Args:
            execution_config: {node_name: {executor: ..., image: ..., ...}, '*': {...}}
                              The '*' key is the fallback for nodes not explicitly listed.
            artifact_store:   BaseArtifactStore instance used for memoization and
                              passing intermediate results between nodes/executors.
            **kwargs:         Forwarded to Composer.__init__ (_functions, _parameters, etc.)
        """
        super().__init__(**kwargs)
        # Per-node executor config from the YAML; '*' acts as a wildcard fallback
        self.execution_config = execution_config or {}
        # Persists node outputs across steps and across runs (memoization)
        self.artifact_store = artifact_store

    def _copy(self, **kwargs):
        """
        Overrides Composer._copy so that execution_config and artifact_store are
        preserved whenever fn_graph internally creates a derived composer
        (e.g. update(), update_parameters(), link(), precalculate(), etc.).
        Without this, the extra attributes would be silently dropped on every copy.
        """
        return type(self)(
            _functions=self._functions,
            _cache=self._cache,
            _parameters=self._parameters,
            _tests=self._tests,
            _source_map=self._source_map,
            execution_config=self.execution_config,
            artifact_store=self.artifact_store,
            **kwargs,
        )

    def calculate(self, outputs, perform_checks=True, intermediates=False, progress_callback=None):
        """
        Overrides Composer.calculate() with three additions:
        1. Memoization  — nodes already in the artifact store are skipped entirely.
        2. Pluggable executors — each node is dispatched to the executor in execution_config.
        3. Automatic parallelism — nodes fire as soon as all their predecessors finish,
           using a ThreadPoolExecutor driven by the DAG topology.

        Falls back to the base Composer.calculate() when no artifact_store is configured,
        so PipelineComposer can still be used as a plain Composer in tests.
        """
        if self.artifact_store is None:
            return super().calculate(outputs, perform_checks, intermediates, progress_callback)

        print("\n[PipelineComposer] pipeline run started", flush=True)
        print("[PipelineComposer] subclass of fn_graph Composer confirmed", flush=True)

        funcs = self.functions()
        # Parameters are stored as {name: (type, value)} tuples — extract just the values
        params = {name: val for name, (_, val) in self.parameters().items()}

        # Limit the execution graph to only the subgraph needed for the requested outputs
        ancestor_dag = self.ancestor_dag(outputs)
        topo_order = list(nx.topological_sort(ancestor_dag))

        print(f"[PipelineComposer] outputs requested: {outputs}", flush=True)
        print(f"[PipelineComposer] execution order: {topo_order}", flush=True)
        print(f"[PipelineComposer] parameters: {list(params.keys())}", flush=True)

        # Write parameter values into the artifact store so every node loads
        # its inputs uniformly — whether they come from params or upstream nodes
        print("\n[PipelineComposer] seeding parameters into artifact store", flush=True)
        for name, value in params.items():
            if name in ancestor_dag.nodes():
                self.artifact_store.put(name, value)
                print(f"[PipelineComposer] seeded: {name} = {value}", flush=True)

        # Parameters are already "done"; this set grows as nodes complete
        done_set = set(params.keys())
        done_lock = threading.Lock()
        results = {}

        def execute_node(node_name):
            """
            Run one node end-to-end:
            1. Return immediately if the artifact store already has a result (memoization).
            2. Load all inputs from the artifact store.
            3. Dispatch to the configured executor (memory, Docker, Lambda, …).
            4. Persist the result so downstream nodes and future runs can use it.
            """
            thread_id = threading.get_ident()
            print(f"[PipelineComposer] {node_name} running in thread {thread_id}", flush=True)

            # Memoization: reuse a result persisted in a prior pipeline run
            if self.artifact_store.exists(node_name):
                print(f"[PipelineComposer] output exists, skipping: {node_name}", flush=True)
                return self.artifact_store.get(node_name)

            # Use fn_graph's own predecessor resolution — correctly handles namespaced
            # node names, optional parameters, and *args/**kwargs patterns
            predecessors = list(self._resolve_predecessors(node_name))
            kwargs = {param: self.artifact_store.get(node) for param, node in predecessors}

            # Look up executor config; fall back to '*' wildcard, then default to memory
            node_config = (
                self.execution_config.get(node_name)
                or self.execution_config.get("*", {"executor": "memory"})
            )
            executor = get_executor(node_config)
            print(
                f"[PipelineComposer] dispatching {node_name} to {type(executor).__name__}",
                flush=True,
            )

            result = executor.execute(node_name, funcs[node_name], kwargs)
            self.artifact_store.put(node_name, result)
            print(f"[PipelineComposer] {node_name} complete", flush=True)
            return result

        # Seed the initial wave: non-param nodes whose predecessors are all parameters
        # (i.e. they have no computed dependencies and can start immediately)
        ready = [
            n for n in topo_order
            if n not in params
            and all(p in params for p in ancestor_dag.predecessors(n))
        ]
        print(f"[PipelineComposer] initial ready nodes: {ready}", flush=True)

        # submitted tracks nodes already queued to avoid double-dispatch
        submitted = set(ready)

        with ThreadPoolExecutor(max_workers=8) as pool:
            future_to_node = {pool.submit(execute_node, n): n for n in ready}

            while future_to_node:
                # Block until at least one future completes, then handle all that finished
                done_futures, _ = wait(future_to_node.keys(), return_when=FIRST_COMPLETED)

                for future in done_futures:
                    node_name = future_to_node.pop(future)
                    # Raises immediately if the node function threw an exception,
                    # which propagates out of calculate() to the caller
                    result = future.result()
                    results[node_name] = result

                    with done_lock:
                        done_set.add(node_name)

                    print(
                        f"[PipelineComposer] {node_name} done, checking unblocked nodes",
                        flush=True,
                    )

                    # Check each direct successor to see if it just became unblocked
                    for downstream in ancestor_dag.successors(node_name):
                        if downstream in params or downstream in submitted:
                            continue
                        preds = set(ancestor_dag.predecessors(downstream))
                        with done_lock:
                            all_done = preds.issubset(done_set)
                        if all_done:
                            print(
                                f"[PipelineComposer] unblocked: {downstream}, dispatching",
                                flush=True,
                            )
                            submitted.add(downstream)
                            future_to_node[pool.submit(execute_node, downstream)] = downstream

        print("\n[PipelineComposer] pipeline complete", flush=True)

        # Return all artifacts from the store, not just the requested outputs,
        # so the caller can inspect intermediate results (matches original run() behaviour)
        all_results = {}
        for name in funcs:
            if self.artifact_store.exists(name):
                all_results[name] = self.artifact_store.get(name)
        return all_results
