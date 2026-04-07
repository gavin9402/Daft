"""Resource manager for downloading and caching task resources.

Before task execution,
the ResourceManager downloads added resources to the worker's local filesystem.
"""

from __future__ import annotations

import hashlib
import logging
import os
import shutil
import tempfile
from abc import ABC, abstractmethod

logger = logging.getLogger(__name__)


class ResourceManager(ABC):
    """Abstract base class for managing task resource dependencies.

    A ResourceManager is responsible for resolving added_resources
    (resource name -> timestamp) by downloading or fetching them to the
    worker's local filesystem before task execution.
    """

    @abstractmethod
    def resolve(self, added_resources: dict[str, int]) -> None:
        """Resolve and fetch resources to the local worker.

        Args:
            added_resources: Mapping of resource name/URI to Unix millisecond timestamp.
        """
        ...

    @abstractmethod
    def get_resource_path(self, name: str) -> str | None:
        """Get the local filesystem path for a previously resolved resource.

        Args:
            name: The resource name/URI used when adding the resource.

        Returns:
            Local path to the resource file, or None if not resolved.
        """
        ...


class DefaultResourceManager(ResourceManager):
    """Default resource manager that downloads resources to a local cache directory.

    Resources are cached by name and timestamp. If a resource with the same
    name and timestamp is already cached, it will not be re-downloaded.
    """

    def __init__(self, cache_dir: str | None = None) -> None:
        self._cache_dir = cache_dir or os.path.join(tempfile.gettempdir(), "daft_resources")
        self._resolved: dict[str, str] = {}  # resource name -> local path
        os.makedirs(self._cache_dir, exist_ok=True)

    @property
    def cache_dir(self) -> str:
        """Return the local cache directory for downloaded resources."""
        return self._cache_dir

    def resolve(self, added_resources: dict[str, int]) -> None:
        """Resolve added resources by downloading them to the local cache.

        Args:
            added_resources: Mapping of resource name/URI to Unix millisecond timestamp.
        """
        if not added_resources:
            return

        for name, timestamp in added_resources.items():
            if name in self._resolved:
                logger.debug("Resource '%s' already resolved, skipping", name)
                continue

            local_path = self._download_resource(name, timestamp)
            if local_path is not None:
                self._resolved[name] = local_path
                logger.info("Resolved resource '%s' -> %s", name, local_path)
            else:
                logger.warning("Failed to resolve resource '%s'", name)

    def get_resource_path(self, name: str) -> str | None:
        """Get the local path for a resolved resource.

        Args:
            name: The resource name/URI.

        Returns:
            Local filesystem path, or None if not resolved.
        """
        return self._resolved.get(name)

    def _download_resource(self, name: str, timestamp: int) -> str | None:
        """Download a single resource to the local cache.

        Supports local files/directories and remote URIs (S3, GCS, HTTP, Azure, etc.)
        via Daft's native IO layer.

        Args:
            name: Resource name, local path, or remote URI.
            timestamp: Unix millisecond timestamp for cache invalidation.

        Returns:
            Local path to the downloaded resource, or None on failure.
        """
        # Create a cache key based on resource name and timestamp
        cache_key = hashlib.sha256(f"{name}:{timestamp}".encode()).hexdigest()[:16]
        basename = os.path.basename(name) if "/" in name or "\\" in name else name
        local_path = os.path.join(self._cache_dir, f"{cache_key}_{basename}")

        # If already cached with same timestamp, reuse
        if os.path.exists(local_path):
            logger.debug("Resource '%s' found in cache at %s", name, local_path)
            self._resolved[name] = local_path
            return local_path

        # For local files/directories, copy them to the cache
        if os.path.exists(name):
            try:
                if os.path.isdir(name):
                    shutil.copytree(name, local_path)
                else:
                    shutil.copy2(name, local_path)
                return local_path
            except OSError as e:
                logger.warning("Failed to copy local resource '%s': %s", name, e)
                return None

        # For remote URIs (s3://, gs://, http://, https://, abfs://, hf://, etc.),
        # use Daft's native IO layer to download
        try:
            from daft.file import File

            remote_file = File(name)
            with remote_file.open() as f:
                data = f.read()
            with open(local_path, "wb") as out:
                out.write(data)
            return local_path
        except (OSError, ValueError, RuntimeError) as e:
            logger.warning("Failed to download remote resource '%s': %s", name, e)
            return None
