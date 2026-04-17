"""
StageExecutor: runs all nodes within a pipeline stage in topological order,
keeping results in memory to avoid unnecessary serialization.

Exactly two categories of nodes are written to the artifact store:
  1. Boundary output nodes — consumed by a downstream stage (cross-stage handoff)
  2. Leaf output nodes    — the pipeline's final requested outputs (terminal stage)

Everything else stays in memory for the lifetime of the stage and is never
serialized to disk. This means intra-stage intermediate results (A→B→C where
only C crosses the boundary) produce zero pkl files.

Pass debug_artifacts=True (via --debug-artifacts CLI flag) to persist all nodes
for inspection without changing the pipeline or config.

The caller (PipelineComposer._calculate_with_stages) is responsible for:
  - Loading stage inputs from the artifact store before calling run()
  - Dispatching multiple StageExecutors in parallel via ThreadPoolExecutor
"""

import logging

import networkx as nx

from config import get_executor

log = logging.getLogger(__name__)


class StageExecutor:
    """
    Executes all nodes in a single pipeline stage.

    Nodes run in topological order within the stage. Results pass between
    nodes purely in-memory (a local dict). Only two categories of nodes are
    written to the artifact store:
      - boundary output nodes: consumed by a downstream stage
      - leaf output nodes: the pipeline's final requested outputs (terminal stage)

    All other intra-stage nodes stay in memory and are never serialized.

    Pass debug_artifacts=True to persist every node regardless of category —
    useful for inspecting intermediate results without changing the pipeline.
    """

    def __init__(
        self,
        *,
        stage_name: str,
        stage_def: dict,
        node_functions: dict,
        stage_inputs: dict,
        resolve_predecessors_fn,
        artifact_store,
        stage_output_nodes: set,
        ancestor_dag,
        leaf_outputs: set = None,
        debug_artifacts: bool = False,
    ):
        self.stage_name = stage_name
        self.stage_def = stage_def
        self.node_functions = node_functions
        self.memory = dict(stage_inputs)
        self.resolve_predecessors_fn = resolve_predecessors_fn
        self.artifact_store = artifact_store
        self.stage_output_nodes = stage_output_nodes
        self.ancestor_dag = ancestor_dag
        self.leaf_outputs = leaf_outputs or set()
        self.debug_artifacts = debug_artifacts
        self.executor = get_executor(stage_def)

    def run(self) -> dict:
        """
        Run all stage nodes in topological order.

        Returns a dict of {node_name: result} for the stage boundary outputs only.
        Side effect: writes boundary outputs to the artifact store.
        """
        stage_nodes = self.stage_def.get("nodes", [])
        active_nodes = [n for n in stage_nodes if n in self.ancestor_dag.nodes]

        if not active_nodes:
            log.debug(f"[StageExecutor:{self.stage_name}] no active nodes in ancestor DAG — skipping")
            return {}

        stage_subgraph = self.ancestor_dag.subgraph(active_nodes)
        try:
            ordered_nodes = list(nx.topological_sort(stage_subgraph))
        except nx.NetworkXUnfeasible:
            ordered_nodes = active_nodes

        log.info(f"[StageExecutor:{self.stage_name}] running {len(ordered_nodes)} nodes: {ordered_nodes}")

        boundary_outputs = {}

        for node_name in ordered_nodes:
            if self.artifact_store.exists(node_name):
                log.info(f"[StageExecutor:{self.stage_name}] memoized — loading {node_name} from store")
                result = self.artifact_store.get(node_name)
                self.memory[node_name] = result
                if node_name in self.stage_output_nodes:
                    boundary_outputs[node_name] = result
                continue

            predecessors = list(self.resolve_predecessors_fn(node_name))
            kwargs = {}
            for param, source_node in predecessors:
                if source_node in self.memory:
                    kwargs[param] = self.memory[source_node]
                else:
                    log.debug(
                        f"[StageExecutor:{self.stage_name}] '{source_node}' not in memory, loading from store"
                    )
                    kwargs[param] = self.artifact_store.get(source_node)

            log.info(
                f"[StageExecutor:{self.stage_name}] running '{node_name}' via {type(self.executor).__name__}"
            )
            result = self.executor.execute(node_name, self.node_functions[node_name], kwargs)
            self.memory[node_name] = result

            is_boundary = node_name in self.stage_output_nodes
            is_leaf     = node_name in self.leaf_outputs

            if self.debug_artifacts or is_boundary or is_leaf:
                if self.debug_artifacts:
                    reason = "debug-artifacts"
                elif is_boundary:
                    reason = "boundary output"
                else:
                    reason = "leaf output"
                log.debug(f"[StageExecutor:{self.stage_name}] persisting '{node_name}' ({reason})")
                self.artifact_store.put(node_name, result)
                if is_boundary:
                    boundary_outputs[node_name] = result
            else:
                log.debug(f"[StageExecutor:{self.stage_name}] '{node_name}' stays in memory (internal)")

        log.info(
            f"[StageExecutor:{self.stage_name}] stage complete, "
            f"boundary outputs: {list(boundary_outputs.keys())}"
        )
        return boundary_outputs
