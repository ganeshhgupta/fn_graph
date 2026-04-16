"""
PersistentDockerExecutor: sends work to a pre-hosted, long-running worker container
via HTTP API calls. The container is started externally (e.g. docker compose up) and
stays alive for the entire pipeline run — fn_graph never starts or stops containers.

Contrast with DockerExecutor, which spins up a fresh container per node, pays the
boot cost, then destroys it. PersistentDockerExecutor skips all of that and just
POSTs to the already-running container's /execute endpoint.

Worker contract (worker/server.py):
    POST /execute   { node_name, fn_source, kwargs_b64 }  → { result_b64 }
    GET  /health    → { status: "ok" }
"""

import base64
import inspect
from typing import Any, Callable

import cloudpickle
import requests

from .base import BaseExecutor


class PersistentDockerExecutor(BaseExecutor):
    """
    Executor that delegates node execution to a pre-running worker container.

    The container is expected to already be up and healthy before the pipeline
    starts. No container lifecycle management is performed here — start/stop is
    handled externally via docker compose (or any other means).

    Args:
        url: Base URL of the worker container, e.g. "http://localhost:8001".
             Must expose GET /health and POST /execute (see worker/server.py).
        timeout: HTTP timeout in seconds for the /execute call (default 300s).
    """

    def __init__(self, url: str, timeout: int = 300):
        self.url = url.rstrip("/")
        self.timeout = timeout

    def execute(self, node_name: str, fn: Callable, kwargs: dict) -> Any:
        print(
            f"[PersistentDockerExecutor] dispatching '{node_name}' → {self.url}",
            flush=True,
        )
        print(f"[PersistentDockerExecutor] inputs: {list(kwargs.keys())}", flush=True)

        fn_source = inspect.getsource(fn)
        kwargs_b64 = base64.b64encode(cloudpickle.dumps(kwargs, protocol=4)).decode()

        resp = requests.post(
            f"{self.url}/execute",
            json={
                "node_name": node_name,
                "fn_source": fn_source,
                "kwargs_b64": kwargs_b64,
            },
            timeout=self.timeout,
        )

        if resp.status_code == 500:
            payload = resp.json()
            print(
                f"[PersistentDockerExecutor] ERROR in node '{node_name}': {payload.get('error')}",
                flush=True,
            )
            print(
                f"[PersistentDockerExecutor] traceback:\n{payload.get('traceback', '')}",
                flush=True,
            )
            raise RuntimeError(
                f"Worker error in node '{node_name}': {payload.get('error')}"
            )

        resp.raise_for_status()
        payload = resp.json()
        result = cloudpickle.loads(base64.b64decode(payload["result_b64"]))
        print(
            f"[PersistentDockerExecutor] '{node_name}' complete, "
            f"output type: {type(result).__name__}",
            flush=True,
        )
        return result
