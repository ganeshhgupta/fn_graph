import networkx as nx

from artifact_store.base import BaseArtifactStore
from config import get_executor


class PipelineComposer:
    """
    Wraps an fn_graph Composer and orchestrates execution with:
    - pluggable per-node executors (memory / docker / lambda)
    - artifact store memoization (skip already-completed nodes)
    - retry support (configured per-node in YAML)
    - on_failure: finish_running — keeps going if a node fails
    """

    def __init__(self, execution_config: dict, artifact_store: BaseArtifactStore, on_failure: str = "stop"):
        """
        Args:
            execution_config: node_name -> node config dict
            artifact_store:   BaseArtifactStore instance
            on_failure:       "stop" (default) or "finish_running"
        """
        self.execution_config = execution_config
        self.artifact_store = artifact_store
        self.on_failure = on_failure

    def run(self, composer_obj) -> dict:
        print("\n[PipelineComposer] starting pipeline run", flush=True)

        dag = composer_obj.dag()
        funcs = composer_obj.functions()
        params = {name: val for name, (_, val) in composer_obj.parameters().items()}

        topo_order = list(nx.topological_sort(dag))
        total = len(topo_order)

        print(f"[PipelineComposer] execution order: {topo_order}", flush=True)
        print(f"[PipelineComposer] parameters: {list(params.keys())}", flush=True)
        print(f"[PipelineComposer] on_failure: {self.on_failure}", flush=True)

        # Seed pipeline parameters into artifact store
        print("\n[PipelineComposer] seeding parameters into artifact store", flush=True)
        for name, value in params.items():
            self.artifact_store.put(name, value)
            print(f"[PipelineComposer] seeded: {name} = {value}", flush=True)

        failed_nodes = set()
        print("\n[PipelineComposer] beginning node execution", flush=True)

        for i, node_name in enumerate(topo_order):
            print(f"\n[PipelineComposer] --- node: {node_name} ({i + 1}/{total}) ---", flush=True)

            # Skip parameter nodes
            if node_name in params:
                print(f"[PipelineComposer] skipping parameter node: {node_name}", flush=True)
                continue

            # Skip if already cached (memoization)
            if self.artifact_store.exists(node_name):
                print(f"[PipelineComposer] output exists, skipping: {node_name}", flush=True)
                continue

            # Skip if any upstream dependency failed
            deps = list(dag.predecessors(node_name))
            blocked_by = [d for d in deps if d in failed_nodes]
            if blocked_by:
                print(f"[PipelineComposer] skipping {node_name}: upstream failed: {blocked_by}", flush=True)
                failed_nodes.add(node_name)
                continue

            # Resolve inputs using fn_graph's own predecessor resolver
            predecessors = list(composer_obj._resolve_predecessors(node_name))
            input_names = [param for param, _ in predecessors]
            print(f"[PipelineComposer] loading inputs: {input_names}", flush=True)
            kwargs = {param: self.artifact_store.get(node) for param, node in predecessors}

            # Get executor for this node
            node_config = self.execution_config.get(node_name) or self.execution_config.get("*")
            executor = get_executor(node_config)
            print(f"[PipelineComposer] executor for {node_name}: {type(executor).__name__}", flush=True)

            try:
                result = executor.execute(node_name, funcs[node_name], kwargs)
                self.artifact_store.put(node_name, result)
                print(f"[PipelineComposer] node {node_name} done", flush=True)

            except Exception as e:
                print(f"[PipelineComposer] ERROR in node '{node_name}': {e}", flush=True)
                failed_nodes.add(node_name)
                if self.on_failure == "finish_running":
                    print(f"[PipelineComposer] on_failure=finish_running, continuing...", flush=True)
                    continue
                else:
                    print(f"[PipelineComposer] on_failure=stop, aborting pipeline", flush=True)
                    raise

        if failed_nodes:
            print(f"\n[PipelineComposer] pipeline finished with failures: {failed_nodes}", flush=True)
        else:
            print(f"\n[PipelineComposer] pipeline complete", flush=True)

        # Collect all results that exist in the artifact store
        results = {
            name: self.artifact_store.get(name)
            for name in funcs
            if self.artifact_store.exists(name)
        }
        print(f"[PipelineComposer] results collected: {list(results.keys())}", flush=True)
        return results
