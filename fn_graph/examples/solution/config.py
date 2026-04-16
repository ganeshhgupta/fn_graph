from pathlib import Path

import yaml

from executor.memory import InMemoryExecutor
from executor.docker import DockerExecutor
from executor.lambda_executor import LambdaExecutor
from executor.persistent_docker import PersistentDockerExecutor
from artifact_store.fs import LocalFSArtifactStore
from artifact_store.s3 import S3ArtifactStore


def load_config(yaml_path: str) -> dict:
    print(f"[config] loading config from {yaml_path}", flush=True)
    with open(yaml_path, "r") as f:
        config = yaml.safe_load(f)
    run_id = config["pipeline"]["run_id"]
    store_type = config["artifact_store"]["type"]
    nodes = config.get("nodes", {})
    print(f"[config] run_id: {run_id}", flush=True)
    print(f"[config] artifact store type: {store_type}", flush=True)
    print(f"[config] nodes configured: {list(nodes.keys())}", flush=True)
    return config


def get_executor(node_config: dict):
    executor_type = node_config.get("executor", "memory")
    print(f"[config] creating executor: {executor_type}", flush=True)
    if executor_type == "memory":
        return InMemoryExecutor()
    elif executor_type == "docker":
        return DockerExecutor(image=node_config.get("image", "fn_graph_worker_v2"))
    elif executor_type == "lambda":
        return LambdaExecutor(
            function_name=node_config["function_name"],
            region=node_config["region"],
        )
    elif executor_type == "persistent_docker":
        return PersistentDockerExecutor(
            url=node_config["url"],
            timeout=node_config.get("timeout", 300),
        )
    else:
        raise ValueError(f"Unknown executor type: '{executor_type}'")


def get_artifact_store(config: dict):
    store_cfg = config["artifact_store"]
    store_type = store_cfg["type"]
    run_id = config["pipeline"]["run_id"]
    print(f"[config] creating artifact store: {store_type}", flush=True)
    if store_type == "fs":
        return LocalFSArtifactStore(base_dir=store_cfg["base_dir"], run_id=run_id)
    elif store_type == "s3":
        return S3ArtifactStore(
            bucket=store_cfg["bucket"],
            run_id=run_id,
            region=store_cfg.get("region", "us-east-1"),
        )
    else:
        raise ValueError(f"Unknown artifact store type: '{store_type}'")


def get_node_config(config: dict, node_name: str) -> dict:
    nodes = config.get("nodes", {})
    return nodes.get(node_name) or nodes.get("*", {"executor": "memory"})


def load_stages(config: dict) -> dict:
    """
    Read the top-level `stages:` key from config and return a dict of stage definitions.

    Each stage definition contains at minimum:
      - executor: the executor type for all nodes in this stage
      - nodes: list of node names belonging to this stage

    If no `stages:` key exists (e.g. older configs), returns empty dict so the
    caller can fall back to node-level execution transparently.

    Example return value:
    {
      "preprocessing": {"executor": "memory", "nodes": ["iris", "data", "preprocess_data"]},
      "training":      {"executor": "docker",  "image": "fn_graph_worker_v2", "nodes": ["model"]},
    }
    """
    stages = config.get("stages", {})
    if stages:
        print(f"[config] stages defined: {list(stages.keys())}", flush=True)
        for stage_name, stage_def in stages.items():
            nodes = stage_def.get("nodes", [])
            executor = stage_def.get("executor", "memory")
            print(f"[config]   stage '{stage_name}': executor={executor}, nodes={nodes}", flush=True)
    else:
        print("[config] no stages defined — using node-level execution", flush=True)
    return stages
