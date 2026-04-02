from abc import ABC, abstractmethod
from typing import Any, Callable


class BaseExecutor(ABC):
    @abstractmethod
    def execute(self, node_name: str, fn: Callable, kwargs: dict) -> Any:
        """Execute a node function with the given kwargs and return the result."""
