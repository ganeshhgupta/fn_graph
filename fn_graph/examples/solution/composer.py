import inspect
from inspect import Parameter

import networkx as nx

from artifact_store.base import BaseArtifactStore
from config import get_executor


class PipelineComposer:
    """
    Wraps an fn_graph Composer and orchestrates execution with pluggable
    executors per node and an artifact store for memoization.
    """

    def __init__(self, execution_config: dict, artifact_store: BaseArtifactStore):
        """
        Args:
            execution_config: mapping of node_name -> node config dict
            artifact_store: an instance of BaseArtifactStore
        """
        self.execution_config = execution_config
        self.artifact_store = artifact_store

    def run(self, composer_obj) -> dict:
        print("\n[PipelineComposer] starting pipeline run", flush=True)

        dag = composer_obj.dag()
        funcs = composer_obj.functions()
        # Parameters come back as {name: (type, value)} — extract just the values
        params = {name: val for name, (_, val) in composer_obj.parameters().items()}

        topo_order = list(nx.topological_sort(dag))
        total = len(topo_order)

        print(f"[PipelineComposer] execution order: {topo_order}", flush=True)
        print(f"[PipelineComposer] parameters: {list(params.keys())}", flush=True)

        print("\n[PipelineComposer] seeding parameters into artifact store", flush=True)
        for name, value in params.items():
            self.artifact_store.put(name, value)
            print(f"[PipelineComposer] seeded: {name} = {value}", flush=True)

        print("\n[PipelineComposer] beginning node execution", flush=True)

        for i, node_name in enumerate(topo_order):
            print(f"\n[PipelineComposer] --- node: {node_name} ({i + 1}/{total}) ---", flush=True)

            if node_name in params:
                print(f"[PipelineComposer] skipping parameter node: {node_name}", flush=True)
                continue

            if self.artifact_store.exists(node_name):
                print(f"[PipelineComposer] output exists, skipping: {node_name}", flush=True)
                continue

            deps = list(dag.predecessors(node_name))
            print(f"[PipelineComposer] dependencies: {deps}", flush=True)

            # Use fn_graph's own resolver: yields (param_name, node_name) pairs,
            # correctly excluding defaulted params that have no corresponding node.
            predecessors = list(composer_obj._resolve_predecessors(node_name))
            input_names = [param for param, _ in predecessors]
            print(f"[PipelineComposer] loading inputs: {input_names}", flush=True)

            kwargs = {param: self.artifact_store.get(node) for param, node in predecessors}

            node_config = self.execution_config.get(node_name) or self.execution_config.get("*")
            executor = get_executor(node_config)
            print(f"[PipelineComposer] executor for {node_name}: {type(executor).__name__}", flush=True)

            result = executor.execute(node_name, funcs[node_name], kwargs)
            self.artifact_store.put(node_name, result)
            print(f"[PipelineComposer] node {node_name} done", flush=True)

        print("\n[PipelineComposer] pipeline complete, collecting results", flush=True)
        results = {
            name: self.artifact_store.get(name)
            for name in funcs
            if self.artifact_store.exists(name)
        }
        print(f"[PipelineComposer] results collected: {list(results.keys())}", flush=True)
        return results
