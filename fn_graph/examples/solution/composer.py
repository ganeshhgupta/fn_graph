import logging
import threading
from concurrent.futures import ThreadPoolExecutor, wait, FIRST_COMPLETED

import networkx as nx

from fn_graph import Composer
from config import get_executor, load_stages

log = logging.getLogger(__name__)


class PipelineComposer(Composer):
    """
    Subclass of fn_graph's Composer that overrides calculate() to route each node
    through a pluggable executor and a persistent artifact store for memoization.
    """

    def __init__(self, *, execution_config=None, artifact_store=None, pipeline_config=None, **kwargs):
        super().__init__(**kwargs)
        self.execution_config = execution_config or {}
        self.artifact_store = artifact_store
        self._config = pipeline_config or {}

    def _copy(self, **kwargs):
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
        For each stage, determine which nodes are boundary inputs, boundary outputs,
        or internal (memory-only). Also builds a stage-level DAG for parallel dispatch.
        """
        node_to_stage = {}
        for stage_name, stage_def in stages.items():
            for node in stage_def.get("nodes", []):
                node_to_stage[node] = stage_name

        analysis = {}
        for stage_name, stage_def in stages.items():
            stage_nodes = set(stage_def.get("nodes", []))
            inputs = set()
            outputs = set()

            for node in stage_nodes:
                if node not in ancestor_dag.nodes:
                    continue
                for pred in ancestor_dag.predecessors(node):
                    pred_stage = node_to_stage.get(pred)
                    if pred_stage is not None and pred_stage != stage_name:
                        inputs.add(pred)
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

        stage_dag = nx.DiGraph()
        stage_dag.add_nodes_from(stages.keys())
        for stage_name, info in analysis.items():
            for input_node in info["inputs"]:
                producer_stage = node_to_stage.get(input_node)
                if producer_stage and producer_stage != stage_name:
                    stage_dag.add_edge(producer_stage, stage_name)

        analysis["__stage_dag__"] = stage_dag

        log.debug("[PipelineComposer] === Stage Boundary Analysis ===")
        for stage_name, info in analysis.items():
            if stage_name == "__stage_dag__":
                continue
            log.debug(f"[PipelineComposer] stage '{stage_name}':")
            log.debug(f"  inputs   (load from store): {sorted(info['inputs'])}")
            log.debug(f"  outputs  (save to store):   {sorted(info['outputs'])}")
            log.debug(f"  internal (memory only):     {sorted(info['internal'])}")

        log.debug(f"[PipelineComposer] stage-level DAG edges: {list(stage_dag.edges())}")
        stage_order = list(nx.topological_sort(stage_dag))
        log.debug(f"[PipelineComposer] stage execution order (serial fallback): {stage_order}")

        return analysis

    def calculate(self, outputs, perform_checks=True, intermediates=False, progress_callback=None):
        if self.artifact_store is None:
            return super().calculate(outputs, perform_checks, intermediates, progress_callback)

        log.info("[PipelineComposer] pipeline run started")
        log.debug("[PipelineComposer] subclass of fn_graph Composer confirmed")

        funcs = self.functions()
        params = {name: val for name, (_, val) in self.parameters().items()}

        ancestor_dag = self.ancestor_dag(outputs)
        topo_order = list(nx.topological_sort(ancestor_dag))

        log.info(f"[PipelineComposer] outputs requested: {outputs}")
        log.debug(f"[PipelineComposer] execution order: {topo_order}")
        log.debug(f"[PipelineComposer] parameters: {list(params.keys())}")

        log.debug("[PipelineComposer] seeding parameters into artifact store")
        for name, value in params.items():
            if name in ancestor_dag.nodes():
                self.artifact_store.put(name, value)
                log.debug(f"[PipelineComposer] seeded: {name} = {value}")

        stages = load_stages(self._config) if hasattr(self, "_config") else {}

        if stages:
            return self._calculate_with_stages(
                stages, ancestor_dag, topo_order, funcs, params, leaf_outputs=set(outputs)
            )

        # ── Node-level execution path (fallback) ──────────────────────────────
        done_set = set(params.keys())
        done_lock = threading.Lock()
        results = {}

        def execute_node(node_name):
            thread_id = threading.get_ident()
            log.debug(f"[PipelineComposer] {node_name} running in thread {thread_id}")

            if self.artifact_store.exists(node_name):
                log.info(f"[PipelineComposer] output exists, skipping: {node_name}")
                return self.artifact_store.get(node_name)

            predecessors = list(self._resolve_predecessors(node_name))
            kwargs = {param: self.artifact_store.get(node) for param, node in predecessors}

            node_config = (
                self.execution_config.get(node_name)
                or self.execution_config.get("*", {"executor": "memory"})
            )
            executor = get_executor(node_config)
            log.debug(f"[PipelineComposer] dispatching {node_name} to {type(executor).__name__}")

            result = executor.execute(node_name, funcs[node_name], kwargs)
            self.artifact_store.put(node_name, result)
            log.info(f"[PipelineComposer] {node_name} complete")
            return result

        ready = [
            n for n in topo_order
            if n not in params
            and all(p in params for p in ancestor_dag.predecessors(n))
        ]
        log.debug(f"[PipelineComposer] initial ready nodes: {ready}")

        submitted = set(ready)

        with ThreadPoolExecutor(max_workers=8) as pool:
            future_to_node = {pool.submit(execute_node, n): n for n in ready}

            while future_to_node:
                done_futures, _ = wait(future_to_node.keys(), return_when=FIRST_COMPLETED)

                for future in done_futures:
                    node_name = future_to_node.pop(future)
                    result = future.result()
                    results[node_name] = result

                    with done_lock:
                        done_set.add(node_name)

                    log.debug(f"[PipelineComposer] {node_name} done, checking unblocked nodes")

                    for downstream in ancestor_dag.successors(node_name):
                        if downstream in params or downstream in submitted:
                            continue
                        preds = set(ancestor_dag.predecessors(downstream))
                        with done_lock:
                            all_done = preds.issubset(done_set)
                        if all_done:
                            log.debug(f"[PipelineComposer] unblocked: {downstream}, dispatching")
                            submitted.add(downstream)
                            future_to_node[pool.submit(execute_node, downstream)] = downstream

        log.info("[PipelineComposer] pipeline complete")

        all_results = {}
        for name in funcs:
            if self.artifact_store.exists(name):
                all_results[name] = self.artifact_store.get(name)
        return all_results

    def _calculate_with_stages(self, stages, ancestor_dag, topo_order, funcs, params, leaf_outputs=None):
        """
        Stage-based execution: runs nodes within each stage in-memory, only persisting
        nodes at stage boundaries. Dispatches independent stages in parallel.
        """
        from executor.stage_executor import StageExecutor

        leaf_outputs = leaf_outputs or set()
        debug_artifacts = self._config.get("pipeline", {}).get("debug_artifacts", False)

        analysis = self._analyze_stage_boundaries(stages, ancestor_dag)
        stage_dag = analysis["__stage_dag__"]

        node_to_stage = {}
        for sname, sdef in stages.items():
            for node in sdef.get("nodes", []):
                node_to_stage[node] = sname

        done_stages = set()
        done_lock = threading.Lock()

        def stage_inputs_ready(stage_name):
            for input_node in analysis[stage_name]["inputs"]:
                if not self.artifact_store.exists(input_node):
                    return False
            return True

        def run_stage(stage_name):
            log.info(f"[PipelineComposer] dispatching stage: '{stage_name}'")
            stage_def = stages[stage_name]
            stage_info = analysis[stage_name]

            output_nodes = stage_info["outputs"]
            if output_nodes and all(self.artifact_store.exists(n) for n in output_nodes):
                log.info(f"[PipelineComposer] stage '{stage_name}' fully memoized — skipping")
                return

            stage_nodes_in_dag = [n for n in stage_def.get("nodes", []) if n in ancestor_dag.nodes]
            if not output_nodes and all(self.artifact_store.exists(n) for n in stage_nodes_in_dag):
                log.info(f"[PipelineComposer] stage '{stage_name}' fully memoized (terminal) — skipping")
                return

            stage_inputs = {}
            for input_node in stage_info["inputs"]:
                stage_inputs[input_node] = self.artifact_store.get(input_node)
                log.debug(f"[PipelineComposer]   loaded input '{input_node}' from store")

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
                leaf_outputs=leaf_outputs,
                debug_artifacts=debug_artifacts,
            )
            executor.run()
            log.info(f"[PipelineComposer] stage '{stage_name}' complete")

        ready_stages = [s for s in stages if stage_dag.in_degree(s) == 0]
        log.debug(f"[PipelineComposer] initial ready stages: {ready_stages}")

        submitted_stages = set(ready_stages)

        with ThreadPoolExecutor(max_workers=4) as pool:
            future_to_stage = {pool.submit(run_stage, s): s for s in ready_stages}
            log.debug(f"[PipelineComposer] dispatching {len(ready_stages)} stage(s) in parallel: {ready_stages}")

            while future_to_stage:
                done_futures, _ = wait(future_to_stage.keys(), return_when=FIRST_COMPLETED)

                for future in done_futures:
                    stage_name = future_to_stage.pop(future)
                    future.result()

                    with done_lock:
                        done_stages.add(stage_name)

                    log.debug(f"[PipelineComposer] stage '{stage_name}' finished, checking downstream stages")

                    for downstream_stage in stage_dag.successors(stage_name):
                        if downstream_stage in submitted_stages:
                            continue
                        pred_stages = set(stage_dag.predecessors(downstream_stage))
                        with done_lock:
                            all_preds_done = pred_stages.issubset(done_stages)
                        if all_preds_done:
                            log.info(f"[PipelineComposer] stage '{downstream_stage}' unblocked — dispatching")
                            submitted_stages.add(downstream_stage)
                            future_to_stage[pool.submit(run_stage, downstream_stage)] = downstream_stage

        log.info("[PipelineComposer] all stages complete")

        all_results = {}
        for name in funcs:
            if self.artifact_store.exists(name):
                all_results[name] = self.artifact_store.get(name)
        return all_results
