from abc import ABC, abstractmethod
from typing import Any


class BaseArtifactStore(ABC):
    @abstractmethod
    def get(self, key: str) -> Any:
        """Retrieve an artifact by key."""

    @abstractmethod
    def put(self, key: str, value: Any) -> None:
        """Store an artifact under key."""

    @abstractmethod
    def exists(self, key: str) -> bool:
        """Return True if the artifact exists."""

    @abstractmethod
    def delete(self, key: str) -> None:
        """Delete the artifact at key."""

    @abstractmethod
    def metadata(self, key: str) -> dict:
        """Return metadata (size, mtime/last_modified) for the artifact."""
