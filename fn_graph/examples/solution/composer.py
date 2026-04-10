import threading
from concurrent.futures import ThreadPoolExecutor, wait, FIRST_COMPLETED

import networkx as nx

from fn_graph import Composer
from config import get_executor, load_stages


class PipelineComposer(Composer):
    """
    Subclass of fn_graph's Composer that overrides calculate() to route each node
    through a pluggable executor and a persistent artifact store for memoization.

    Behaves identically to a regular Composer from the outside — all the same
    methods (update, update_parameters, link, dag, graphviz, etc.) work unchanged.
    Only the execution path differs: instead of calling functions directly,
    calculate() dispatches to whichever executor is configured per-node.
    """

    def __init__(self, *, execution_config=None, artifact_store=None, pipeline_config=None, **kwargs):
        """
        Args:
            execution_config: {node_name: {executor: ..., image: ..., ...}, '*': {...}}
                              The '*' key is the fallback for nodes not explicitly listed.
            artifact_store:   BaseArtifactStore instance used for memoization and
                              passing intermediate results between nodes/executors.
            pipeline_config:  The full parsed YAML config dict. Used to load stage
                              definitions for stage-based execution.
            **kwargs:         Forwarded to Composer.__init__ (_functions, _parameters, etc.)
        """
        super().__init__(**kwargs)
        # Per-node executor config from the YAML; '*' acts as a wildcard fallback
        self.execution_config = execution_config or {}
        # Persists node outputs across steps and across runs (memoization)
        self.artifact_store = artifact_store
        # Full config dict — used to read `stages:` key for stage-based execution
        self._config = pipeline_config or {}

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
            pipeline_config=self._config,
            **kwargs,
        )

    def _analyze_stage_boundaries(self, stages: dict, ancestor_dag) -> dict:
        """
        For each stage, determine which nodes are at boundaries:
          - inputs:   nodes produced by another stage that this stage needs
                      (must be loaded from artifact store at stage start)
          - outputs:  nodes in this stage whose results are consumed by another stage
                      (must be written to artifact store after execution)
          - internal: nodes whose results only flow to other nodes within the same stage
                      (can remain in memory throughout the stage)

        Also builds and returns a stage-level DAG (which stages depend on which others)
        so we can dispatch stages in parallel when their inputs are all ready.

        Returns a dict:
        {
          "stage_name": {
            "inputs":   set of node names this stage needs from prior stages,
            "outputs":  set of node names this stage must persist for later stages,
            "internal": set of node names whose results stay in-memory within the stage,
          },
          "__stage_dag__": networkx.DiGraph of stage-to-stage dependencies,
        }
        """
        # Reverse map: node_name → stage_name
        node_to_stage = {}
        for stage_name, stage_def in stages.items():
            for node in stage_def.get("nodes", []):
                node_to_stage[node] = stage_name

        analysis = {}
        for stage_name, stage_def in stages.items():
            stage_nodes = set(stage_def.get("nodes", []))
            inputs = set()    # nodes from other stages needed as inputs here
            outputs = set()   # nodes from this stage consumed by other stages

            for node in stage_nodes:
                if node not in ancestor_dag.nodes:
                    continue

                # Predecessors in a different stage → we need their outputs as inputs
                for pred in ancestor_dag.predecessors(node):
                    pred_stage = node_to_stage.get(pred)
                    if pred_stage is not None and pred_stage != stage_name:
                        inputs.add(pred)

                # Successors in a different stage → this node's output crosses a boundary
                for succ in ancestor_dag.successors(node):
                    succ_stage = node_to_stage.get(succ)
                    if succ_stage is not None and succ_stage != stage_name:
                        outputs.add(node)

            internal = stage_nodes - inputs - outputs
            analysis[stage_name] = {
                "inputs":   inputs,
                "outputs":  outputs,
                "internal": internal,
            }

        # Build a stage-level DAG so we can see which stages block which others
        stage_dag = nx.DiGraph()
        stage_dag.add_nodes_from(stages.keys())
        for stage_name, info in analysis.items():
            for input_node in info["inputs"]:
                producer_stage = node_to_stage.get(input_node)
                if producer_stage and producer_stage != stage_name:
                    stage_dag.add_edge(producer_stage, stage_name)

        analysis["__stage_dag__"] = stage_dag

        print("\n[PipelineComposer] === Stage Boundary Analysis ===", flush=True)
        for stage_name, info in analysis.items():
            if stage_name == "__stage_dag__":
                continue
            print(f"[PipelineComposer] stage '{stage_name}':", flush=True)
            print(f"  inputs   (load from store): {sorted(info['inputs'])}", flush=True)
            print(f"  outputs  (save to store):   {sorted(info['outputs'])}", flush=True)
            print(f"  internal (memory only):     {sorted(info['internal'])}", flush=True)

        print(f"\n[PipelineComposer] stage-level DAG edges: {list(stage_dag.edges())}", flush=True)
        stage_order = list(nx.topological_sort(stage_dag))
        print(f"[PipelineComposer] stage execution order (serial fallback): {stage_order}", flush=True)

        return analysis

    def calculate(self, outputs, perform_checks=True, intermediates=False, progress_callback=None):
        """
        Overrides Composer.calculate() with three additions:
        1. Memoization  — nodes already in the artifact store are skipped entirely.
        2. Pluggable executors — each node is dispatched to the executor in execution_config.
        3. Automatic parallelism — nodes fire as soon as all their predecessors finish,
           using a ThreadPoolExecutor driven by the DAG topology.

        When `stages:` are defined in config, uses stage-based execution:
        - Runs all nodes within a stage in topological order in-memory
        - Only persists boundary nodes (those consumed by another stage) to the artifact store
        - Dispatches stages in parallel using a stage-level DAG + ThreadPoolExecutor

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

        # ── Stage-based execution path ────────────────────────────────────────────
        # When stages are defined, import and use StageExecutor to keep intra-stage
        # results in memory and only persist nodes at stage boundaries. Stages that
        # have no un-met dependencies are dispatched in parallel.
        stages = load_stages(self._config) if hasattr(self, "_config") else {}

        if stages:
            return self._calculate_with_stages(
                stages, ancestor_dag, topo_order, funcs, params
            )
        # ── Node-level execution path (fallback) ──────────────────────────────────

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

    def _calculate_with_stages(self, stages, ancestor_dag, topo_order, funcs, params):
        """
        Stage-based execution: runs nodes within each stage in-memory, only persisting
        nodes at stage boundaries. Dispatches independent stages in parallel via
        ThreadPoolExecutor + FIRST_COMPLETED wait, same pattern as node-level execution.

        A stage is ready to dispatch when all its input artifacts exist in the artifact store
        (meaning the upstream stages that produce them have finished).
        """
        from executor.stage_executor import StageExecutor

        # Boundary analysis tells us which nodes cross stage lines
        analysis = self._analyze_stage_boundaries(stages, ancestor_dag)
        stage_dag = analysis["__stage_dag__"]

        # Build reverse map for quick lookup
        node_to_stage = {}
        for sname, sdef in stages.items():
            for node in sdef.get("nodes", []):
                node_to_stage[node] = sname

        # Track which stages are done (set of stage names)
        done_stages = set()
        done_lock = threading.Lock()

        def stage_inputs_ready(stage_name):
            """A stage can run when every artifact it needs from prior stages is in the store."""
            for input_node in analysis[stage_name]["inputs"]:
                if not self.artifact_store.exists(input_node):
                    return False
            return True

        def run_stage(stage_name):
            """
            Execute one stage using StageExecutor:
            1. Check memoization — if all boundary outputs already exist, skip entire stage.
            2. Load stage inputs from artifact store.
            3. Run all stage nodes in topological order in-memory.
            4. Persist only boundary output nodes.
            """
            print(f"\n[PipelineComposer] === dispatching stage: '{stage_name}' ===", flush=True)
            stage_def = stages[stage_name]
            stage_info = analysis[stage_name]

            # Memoization: skip entire stage if all its boundary outputs are already stored
            output_nodes = stage_info["outputs"]
            if output_nodes and all(self.artifact_store.exists(n) for n in output_nodes):
                print(f"[PipelineComposer] stage '{stage_name}' fully memoized — skipping", flush=True)
                return

            # Also check for terminal stages (no outputs to other stages) — skip if all
            # nodes in the stage are already in the store
            stage_nodes_in_dag = [n for n in stage_def.get("nodes", []) if n in ancestor_dag.nodes]
            if not output_nodes and all(self.artifact_store.exists(n) for n in stage_nodes_in_dag):
                print(f"[PipelineComposer] stage '{stage_name}' fully memoized (terminal) — skipping", flush=True)
                return

            # Load inputs produced by other stages from the artifact store
            stage_inputs = {}
            for input_node in stage_info["inputs"]:
                stage_inputs[input_node] = self.artifact_store.get(input_node)
                print(f"[PipelineComposer]   loaded input '{input_node}' from store", flush=True)

            # Also include parameter values that feed into this stage
            for node in stage_nodes_in_dag:
                for pred in ancestor_dag.predecessors(node):
                    if pred in params:
                        stage_inputs[pred] = self.artifact_store.get(pred)

            executor = StageExecutor(
                stage_name=stage_name,
                stage_def=stage_def,
                node_functions=funcs,
                stage_inputs=stage_inputs,
                resolve_predecessors_fn=self._resolve_predecessors,
                artifact_store=self.artifact_store,
                stage_output_nodes=stage_info["outputs"],
                ancestor_dag=ancestor_dag,
            )
            executor.run()
            print(f"[PipelineComposer] stage '{stage_name}' complete", flush=True)

        # Identify stages with no predecessors — can start immediately
        ready_stages = [s for s in stages if stage_dag.in_degree(s) == 0]
        print(f"\n[PipelineComposer] initial ready stages: {ready_stages}", flush=True)

        submitted_stages = set(ready_stages)

        with ThreadPoolExecutor(max_workers=4) as pool:
            future_to_stage = {pool.submit(run_stage, s): s for s in ready_stages}
            print(f"[PipelineComposer] dispatching {len(ready_stages)} stage(s) in parallel: {ready_stages}", flush=True)

            while future_to_stage:
                done_futures, _ = wait(future_to_stage.keys(), return_when=FIRST_COMPLETED)

                for future in done_futures:
                    stage_name = future_to_stage.pop(future)
                    # Propagate exceptions immediately
                    future.result()

                    with done_lock:
                        done_stages.add(stage_name)

                    print(f"[PipelineComposer] stage '{stage_name}' finished, checking downstream stages", flush=True)

                    # Check successors in the stage DAG
                    for downstream_stage in stage_dag.successors(stage_name):
                        if downstream_stage in submitted_stages:
                            continue
                        # All predecessor stages must be done before we can dispatch
                        pred_stages = set(stage_dag.predecessors(downstream_stage))
                        with done_lock:
                            all_preds_done = pred_stages.issubset(done_stages)
                        if all_preds_done:
                            print(f"[PipelineComposer] stage '{downstream_stage}' unblocked — dispatching", flush=True)
                            submitted_stages.add(downstream_stage)
                            future_to_stage[pool.submit(run_stage, downstream_stage)] = downstream_stage

        print("\n[PipelineComposer] all stages complete", flush=True)

        # Collect all results from the artifact store for the caller
        all_results = {}
        for name in funcs:
            if self.artifact_store.exists(name):
                all_results[name] = self.artifact_store.get(name)
        return all_results
