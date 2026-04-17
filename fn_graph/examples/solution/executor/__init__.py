from .base import BaseExecutor
from .memory import InMemoryExecutor
from .docker import DockerExecutor
from .persistent_docker import PersistentDockerExecutor

def __getattr__(name):
    if name == "LambdaExecutor":
        from .lambda_executor import LambdaExecutor
        return LambdaExecutor
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
