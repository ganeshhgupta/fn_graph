import base64
import logging
import socket
import subprocess
import time
from typing import Any, Callable

import cloudpickle
import requests

from .base import BaseExecutor, gather_fn_source

log = logging.getLogger(__name__)

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
        log.info(f"[DockerExecutor] starting container for node: {node_name}")
        log.debug(f"[DockerExecutor] image: {self.image}")

        port = _free_port()
        log.debug(f"[DockerExecutor] assigned port: {port}")

        container_id = None
        try:
            result = subprocess.run(
                ["docker", "run", "-d", "-p", f"{port}:8000", self.image],
                capture_output=True,
                text=True,
                check=True,
            )
            container_id = result.stdout.strip()
            log.debug(f"[DockerExecutor] container id: {container_id}")

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

            log.info(f"[DockerExecutor] container healthy, sending work")

            fn_source = gather_fn_source(fn)
            kwargs_b64 = base64.b64encode(cloudpickle.dumps(kwargs, protocol=4)).decode()

            log.debug(f"[DockerExecutor] posting to /execute, inputs: {list(kwargs.keys())}")

            resp = requests.post(
                f"http://localhost:{port}/execute",
                json={"node_name": node_name, "fn_source": fn_source, "kwargs_b64": kwargs_b64},
                timeout=300,
            )
            log.debug(f"[DockerExecutor] response received, status: {resp.status_code}")

            if resp.status_code == 500:
                payload = resp.json()
                log.error(f"[DockerExecutor] ERROR in node '{node_name}': {payload.get('error')}")
                log.error(f"[DockerExecutor] traceback:\n{payload.get('traceback', '')}")
                raise RuntimeError(
                    f"Worker error in node '{node_name}': {payload.get('error')}"
                )

            resp.raise_for_status()
            payload = resp.json()
            result = cloudpickle.loads(base64.b64decode(payload["result_b64"]))
            log.info(f"[DockerExecutor] node {node_name} complete, output type: {type(result).__name__}")
            return result
        finally:
            if container_id:
                subprocess.run(["docker", "stop", container_id], capture_output=True)
                subprocess.run(["docker", "rm", container_id], capture_output=True)
                log.debug(f"[DockerExecutor] container stopped and removed")
