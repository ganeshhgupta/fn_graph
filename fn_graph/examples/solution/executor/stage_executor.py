"""
StageExecutor: runs all nodes within a pipeline stage in topological order,
keeping results in memory to avoid unnecessary serialization.

Only nodes at stage boundaries (consumed by a different stage) are persisted
to the artifact store. This dramatically reduces I/O overhead compared to
saving every node output, especially for large intermediate tensors or DataFrames.

The caller (PipelineComposer._calculate_with_stages) is responsible for:
  - Loading stage inputs from the artifact store before calling run()
  - Dispatching multiple StageExecutors in parallel via ThreadPoolExecutor
"""

import networkx as nx

from config import get_executor


class StageExecutor:
    """
    Executes all nodes in a single pipeline stage.

    Nodes run in topological order within the stage. Results pass between
    nodes purely in-memory (a local dict). Only boundary output nodes are
    written to the artifact store, since those results are needed by later stages.
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
    ):
        """
        Args:
            stage_name:             Human-readable name of this stage (e.g. "training").
            stage_def:              Stage config dict from YAML — contains 'nodes', 'executor', etc.
            node_functions:         Map of node_name → callable for every node in the pipeline.
            stage_inputs:           Pre-loaded inputs from earlier stages (node_name → value).
            resolve_predecessors_fn: fn_graph's _resolve_predecessors — returns [(param, node), ...]
            artifact_store:         Used exclusively for writing boundary output nodes.
            stage_output_nodes:     Set of node names whose results must be persisted.
            ancestor_dag:           The full ancestor DAG; used to determine topological order.
        """
        self.stage_name = stage_name
        self.stage_def = stage_def
        self.node_functions = node_functions
        # Seed the in-memory result cache with pre-loaded inputs from other stages
        self.memory = dict(stage_inputs)
        self.resolve_predecessors_fn = resolve_predecessors_fn
        self.artifact_store = artifact_store
        self.stage_output_nodes = stage_output_nodes
        self.ancestor_dag = ancestor_dag

        # Determine executor for this stage from stage_def; default to memory
        self.executor = get_executor(stage_def)

    def run(self) -> dict:
        """
        Run all stage nodes in topological order.

        Returns a dict of {node_name: result} for the stage boundary outputs only.
        Side effect: writes boundary outputs to the artifact store.
        """
        stage_nodes = self.stage_def.get("nodes", [])

        # Restrict to nodes that actually appear in the ancestor DAG (avoid running
        # nodes that aren't needed for the requested outputs)
        active_nodes = [n for n in stage_nodes if n in self.ancestor_dag.nodes]

        if not active_nodes:
            print(f"[StageExecutor:{self.stage_name}] no active nodes in ancestor DAG — skipping", flush=True)
            return {}

        # Determine execution order within the stage via topological sort of the subgraph
        stage_subgraph = self.ancestor_dag.subgraph(active_nodes)
        try:
            ordered_nodes = list(nx.topological_sort(stage_subgraph))
        except nx.NetworkXUnfeasible:
            # Cycle detected — fall back to original YAML order (shouldn't happen with fn_graph)
            ordered_nodes = active_nodes

        print(f"[StageExecutor:{self.stage_name}] running {len(ordered_nodes)} nodes: {ordered_nodes}", flush=True)

        boundary_outputs = {}

        for node_name in ordered_nodes:
            # Memoization check: if the artifact store already has this node's output,
            # load it rather than re-running. This happens on the second pipeline run.
            if self.artifact_store.exists(node_name):
                print(f"[StageExecutor:{self.stage_name}] memoized — loading {node_name} from store", flush=True)
                result = self.artifact_store.get(node_name)
                self.memory[node_name] = result
                if node_name in self.stage_output_nodes:
                    boundary_outputs[node_name] = result
                continue

            # Gather inputs from the in-memory cache (populated by stage_inputs + prior nodes)
            predecessors = list(self.resolve_predecessors_fn(node_name))
            kwargs = {}
            for param, source_node in predecessors:
                if source_node in self.memory:
                    kwargs[param] = self.memory[source_node]
                else:
                    # Fallback: try loading from artifact store (e.g. parameter values)
                    print(
                        f"[StageExecutor:{self.stage_name}] '{source_node}' not in memory, loading from store",
                        flush=True,
                    )
                    kwargs[param] = self.artifact_store.get(source_node)

            print(
                f"[StageExecutor:{self.stage_name}] running '{node_name}' via {type(self.executor).__name__}",
                flush=True,
            )
            result = self.executor.execute(node_name, self.node_functions[node_name], kwargs)
            # Keep result in-memory so downstream nodes in this stage can use it immediately
            self.memory[node_name] = result

            # Only persist to the artifact store if this node is a stage boundary —
            # i.e. a downstream stage needs its output, or it is a terminal leaf node
            # that should be available for the caller's results collection
            if node_name in self.stage_output_nodes:
                print(
                    f"[StageExecutor:{self.stage_name}] persisting boundary node '{node_name}' to store",
                    flush=True,
                )
                self.artifact_store.put(node_name, result)
                boundary_outputs[node_name] = result
            else:
                print(f"[StageExecutor:{self.stage_name}] '{node_name}' stays in memory (internal)", flush=True)

        # Persist terminal (leaf) nodes so the caller can retrieve final results.
        # These nodes have no downstream consumers at all (in_stage or cross_stage),
        # but we still want their values available for display / metrics.
        for node_name in ordered_nodes:
            if node_name not in self.stage_output_nodes and node_name in self.memory:
                if not self.artifact_store.exists(node_name):
                    print(
                        f"[StageExecutor:{self.stage_name}] persisting terminal/internal '{node_name}' for results",
                        flush=True,
                    )
                    self.artifact_store.put(node_name, self.memory[node_name])

        print(f"[StageExecutor:{self.stage_name}] stage complete, boundary outputs: {list(boundary_outputs.keys())}", flush=True)
        return boundary_outputs
