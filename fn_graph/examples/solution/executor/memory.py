import logging
from typing import Any, Callable

from .base import BaseExecutor

log = logging.getLogger(__name__)


class InMemoryExecutor(BaseExecutor):
    def execute(self, node_name: str, fn: Callable, kwargs: dict) -> Any:
        log.debug(f"[InMemoryExecutor] running node: {node_name} directly in process")
        log.debug(f"[InMemoryExecutor] inputs: {list(kwargs.keys())}")
        result = fn(**kwargs)
        log.info(f"[InMemoryExecutor] node {node_name} complete, output type: {type(result).__name__}")
        return result
