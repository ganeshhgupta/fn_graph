import inspect
from abc import ABC, abstractmethod
from typing import Any, Callable


def gather_fn_source(fn: Callable) -> str:
    """
    Return the source code needed to exec fn in a fresh namespace.

    inspect.getsource(fn) only returns the single function body. If fn calls
    helpers defined in the same module (e.g. confusion_matrix calls
    plot_confusion_matrix), those helpers must also be sent so the exec'd
    namespace contains them.

    Strategy: collect every callable in fn.__globals__ that lives in the same
    module as fn, get its source, and prepend it before fn's own source.
    """
    module_name = getattr(fn, "__module__", None)
    fn_globals = getattr(fn, "__globals__", {})

    helper_sources = []
    for obj in fn_globals.values():
        if (
            obj is fn
            or not callable(obj)
            or not hasattr(obj, "__module__")
            or obj.__module__ != module_name
        ):
            continue
        try:
            helper_sources.append(inspect.getsource(obj))
        except (OSError, TypeError):
            pass

    fn_source = inspect.getsource(fn)
    if helper_sources:
        return "\n\n".join(helper_sources) + "\n\n" + fn_source
    return fn_source


class BaseExecutor(ABC):
    @abstractmethod
    def execute(self, node_name: str, fn: Callable, kwargs: dict) -> Any:
        """Execute a node function with the given kwargs and return the result."""
