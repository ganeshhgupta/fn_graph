import base64
import inspect
import socket
import subprocess
import time
from typing import Any, Callable

import cloudpickle
import requests

from .base import BaseExecutor

_HEALTH_TIMEOUT = 30
_HEALTH_INTERVAL = 0.5


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("", 0))
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        return s.getsockname()[1]


class DockerExecutor(BaseExecutor):
    def __init__(self, image: str):
        self.image = image

    def execute(self, node_name: str, fn: Callable, kwargs: dict) -> Any:
        print(f"[DockerExecutor] starting container for node: {node_name}", flush=True)
        print(f"[DockerExecutor] image: {self.image}", flush=True)

        port = _free_port()
        print(f"[DockerExecutor] assigned port: {port}", flush=True)

        container_id = None
        try:
            result = subprocess.run(
                ["docker", "run", "-d", "-p", f"{port}:8000", self.image],
                capture_output=True,
                text=True,
                check=True,
            )
            container_id = result.stdout.strip()
            print(f"[DockerExecutor] container id: {container_id}", flush=True)

            # Poll /health until ready
            deadline = time.time() + _HEALTH_TIMEOUT
            while True:
                try:
                    resp = requests.get(f"http://localhost:{port}/health", timeout=2)
                    if resp.status_code == 200:
                        break
                except requests.exceptions.ConnectionError:
                    pass
                if time.time() > deadline:
                    raise TimeoutError(
                        f"Worker container did not become healthy within {_HEALTH_TIMEOUT}s"
                    )
                time.sleep(_HEALTH_INTERVAL)

            print(f"[DockerExecutor] container healthy, sending work", flush=True)

            fn_source = inspect.getsource(fn)
            kwargs_b64 = base64.b64encode(cloudpickle.dumps(kwargs, protocol=4)).decode()

            print(f"[DockerExecutor] posting to /execute, inputs: {list(kwargs.keys())}", flush=True)

            resp = requests.post(
                f"http://localhost:{port}/execute",
                json={"node_name": node_name, "fn_source": fn_source, "kwargs_b64": kwargs_b64},
                timeout=300,
            )
            print(f"[DockerExecutor] response received, status: {resp.status_code}", flush=True)

            if resp.status_code == 500:
                payload = resp.json()
                print(f"[DockerExecutor] ERROR in node '{node_name}': {payload.get('error')}", flush=True)
                print(f"[DockerExecutor] traceback:\n{payload.get('traceback', '')}", flush=True)
                raise RuntimeError(
                    f"Worker error in node '{node_name}': {payload.get('error')}"
                )

            resp.raise_for_status()
            payload = resp.json()
            result = cloudpickle.loads(base64.b64decode(payload["result_b64"]))
            print(f"[DockerExecutor] node {node_name} complete, output type: {type(result).__name__}", flush=True)
            return result
        finally:
            if container_id:
                subprocess.run(["docker", "stop", container_id], capture_output=True)
                subprocess.run(["docker", "rm", container_id], capture_output=True)
                print(f"[DockerExecutor] container stopped and removed", flush=True)
