from abc import ABC, abstractmethod
from typing import Any, Callable


class BaseExecutor(ABC):
    @abstractmethod
    def execute(self, node_name: str, fn: Callable, kwargs: dict) -> Any: ...

    def health_check(self) -> bool:
        return True

    def teardown(self) -> None:
        pass
