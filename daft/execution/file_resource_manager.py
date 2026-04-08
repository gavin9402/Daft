"""Resource manager for downloading and caching task resources.

Before task execution,
the ResourceManager downloads added resources to the worker's local filesystem.

Archive resources support the ``name#path`` format to specify an extraction
directory.  For example:

- ``archive.zip`` or ``archive.zip#`` — extract to the current working directory.
- ``archive.zip#/tmp/mydir`` — extract to ``/tmp/mydir``.
- ``s3://bucket/data.tar.gz#/opt/data`` — download and extract to ``/opt/data``.
"""

from __future__ import annotations

import hashlib
import logging
import os
import shutil
import tarfile
import tempfile
import zipfile
from typing import Literal

logger = logging.getLogger(__name__)

# Supported file extensions
_ARCHIVE_EXTENSIONS = (".tar", ".tar.gz", ".tgz", ".tar.bz2", ".zip", ".whl")
_DIRECT_EXTENSIONS = (".py", ".egg")


def _get_extension(name: str) -> str:
    """Extract the file extension, handling ``#path`` suffix and compound extensions."""
    # Strip #path suffix for archive extraction path
    actual_name = name.split("#")[0] if "#" in name else name
    lower = actual_name.lower()
    for ext in (".tar.gz", ".tar.bz2"):
        if lower.endswith(ext):
            return ext
    _, ext = os.path.splitext(lower)
    return ext


def _is_archive(name: str) -> bool:
    """Check whether the resource is an archive that needs extraction."""
    return _get_extension(name) in _ARCHIVE_EXTENSIONS


def _parse_resource_name(name: str) -> tuple[str, str | None]:
    """Parse a resource name that may contain a ``#path`` extraction directory.

    The ``#path`` suffix is only meaningful for archive types.  For non-archive
    files (``.py``, ``.egg``, etc.) the ``#`` is treated as part of the name.

    Args:
        name: Resource name, possibly containing ``#path``.

    Returns:
        A tuple of ``(actual_name, extract_path)``.  *extract_path* is ``None``
        when no explicit extraction directory is specified.

    Examples:
        >>> _parse_resource_name("archive.zip")
        ('archive.zip', None)
        >>> _parse_resource_name("archive.zip#")
        ('archive.zip', None)
        >>> _parse_resource_name("archive.zip#/tmp/dir")
        ('archive.zip', '/tmp/dir')
        >>> _parse_resource_name("s3://bucket/data.tar.gz#/opt/data")
        ('s3://bucket/data.tar.gz', '/opt/data')
        >>> _parse_resource_name("model.py#something")
        ('model.py#something', None)
    """
    if "#" not in name:
        return name, None

    # Split on the last '#'
    idx = name.rfind("#")
    candidate = name[:idx]

    # Only split if the part before '#' looks like an archive
    candidate_ext = _get_extension(candidate)
    if candidate_ext not in _ARCHIVE_EXTENSIONS:
        # Not an archive — treat '#' as part of the name
        return name, None

    path_part = name[idx + 1 :]
    return candidate, path_part if path_part else None


class FileResourceManager:
    """Resource manager that downloads file resources to the worker's local filesystem.

    Supported file types:
    - Python files: .py, .egg — downloaded to the current working directory.
    - Archives: .tar, .tar.gz, .tgz, .tar.bz2, .zip, .whl — downloaded and extracted
      to the current working directory (or to a custom directory via ``name#path``).

    Archive resources may use the ``name#path`` format to specify an extraction
    directory.  See :func:`_parse_resource_name` for details.

    Resources are tracked by name to avoid duplicate downloads within the same
    worker lifecycle.
    """

    def __init__(self) -> None:
        self._cache_dir = os.path.join(tempfile.gettempdir(), "daft_resources")
        self._resolved: dict[str, str] = {}  # resource name -> local path
        os.makedirs(self._cache_dir, exist_ok=True)

    @property
    def cache_dir(self) -> str:
        """Return the temporary download cache directory."""
        return self._cache_dir

    def resolve(self, added_resources: dict[str, int]) -> None:
        """Resolve added resources by downloading them to the worker.

        For .py / .egg files, the file is placed in the current working directory.
        For archive files, the archive is extracted into the current working directory
        unless a ``#path`` suffix specifies an alternative extraction directory.

        Args:
            added_resources: Mapping of resource name/URI to Unix millisecond timestamp.
                Archive names may use ``name#path`` format.
        """
        if not added_resources:
            return

        for name, timestamp in added_resources.items():
            if name in self._resolved:
                logger.debug("Resource '%s' already resolved, skipping", name)
                continue

            actual_name, extract_path = _parse_resource_name(name)

            local_path = self._fetch_resource(actual_name, timestamp, extract_path)
            if local_path is not None:
                self._resolved[name] = local_path
                logger.info("Resolved resource '%s' -> %s", name, local_path)
            else:
                logger.warning("Failed to resolve resource '%s'", name)

    def get_resource_path(self, name: str) -> str | None:
        """Get the local path for a resolved resource.

        For archives, returns the directory they were extracted to.
        For .py / .egg files, returns the path in the working directory.

        Args:
            name: The resource name/URI.

        Returns:
            Local filesystem path, or None if not resolved.
        """
        return self._resolved.get(name)

    def _fetch_resource(self, name: str, timestamp: int, extract_path: str | None = None) -> str | None:
        """Fetch a resource: download to cache, then place or extract.

        Args:
            name: Resource name (without ``#path``), local path, or remote URI.
            timestamp: Unix millisecond timestamp for cache invalidation.
            extract_path: Optional extraction directory for archives.  When
                ``None``, archives are extracted to the current working directory.

        Returns:
            Final local path (file or extraction directory), or None on failure.
        """
        # Step 1: download to cache
        cached_path = self._download_to_cache(name, timestamp)
        if cached_path is None:
            return None

        # Step 2: place or extract
        if _is_archive(name):
            dest_dir = extract_path if extract_path is not None else os.getcwd()
            if extract_path is not None:
                os.makedirs(extract_path, exist_ok=True)
            return self._extract_archive(cached_path, name, dest_dir)
        else:
            # .py / .egg — copy to cwd
            cwd = os.getcwd()
            dest = os.path.join(cwd, os.path.basename(name))
            try:
                shutil.copy2(cached_path, dest)
                return dest
            except OSError as e:
                logger.warning("Failed to copy '%s' to working directory: %s", name, e)
                return None

    def _download_to_cache(self, name: str, timestamp: int) -> str | None:
        """Download a resource to the local cache directory.

        Supports local files and remote URIs (S3, GCS, HTTP, Azure, etc.)
        via Daft's native IO layer.

        Args:
            name: Resource name, local path, or remote URI.
            timestamp: Unix millisecond timestamp for cache invalidation.

        Returns:
            Path to the cached file, or None on failure.
        """
        cache_key = hashlib.sha256(f"{name}:{timestamp}".encode()).hexdigest()[:16]
        basename = os.path.basename(name) if "/" in name or "\\" in name else name
        cached_path = os.path.join(self._cache_dir, f"{cache_key}_{basename}")

        # Already in cache
        if os.path.exists(cached_path):
            logger.debug("Resource '%s' found in cache at %s", name, cached_path)
            return cached_path

        # Local file — copy to cache
        if os.path.isfile(name):
            try:
                shutil.copy2(name, cached_path)
                return cached_path
            except OSError as e:
                logger.warning("Failed to copy local resource '%s': %s", name, e)
                return None

        # Remote URI — download via Daft IO
        try:
            from daft.file import File

            remote_file = File(name)
            with remote_file.open() as f:
                data = f.read()
            with open(cached_path, "wb") as out:
                out.write(data)
            return cached_path
        except (OSError, ValueError, RuntimeError) as e:
            logger.warning("Failed to download remote resource '%s': %s", name, e)
            return None

    def _extract_archive(self, cached_path: str, name: str, dest_dir: str) -> str | None:
        """Extract an archive file into the destination directory.

        Args:
            cached_path: Local path to the downloaded archive.
            name: Original resource name (used for extension detection).
            dest_dir: Directory to extract into.

        Returns:
            The destination directory path, or None on failure.
        """
        ext = _get_extension(name)
        try:
            if ext in (".zip", ".whl"):
                with zipfile.ZipFile(cached_path, "r") as zf:
                    zf.extractall(dest_dir)
            elif ext in (".tar", ".tar.gz", ".tgz", ".tar.bz2"):
                mode: Literal["r:", "r:gz", "r:bz2"] = (
                    "r:gz" if ext in (".tar.gz", ".tgz") else "r:bz2" if ext == ".tar.bz2" else "r:"
                )
                with tarfile.open(cached_path, mode) as tf:
                    tf.extractall(dest_dir)
            else:
                logger.warning("Unknown archive format for '%s'", name)
                return None
            return dest_dir
        except (tarfile.TarError, zipfile.BadZipFile, OSError) as e:
            logger.warning("Failed to extract archive '%s': %s", name, e)
            return None


file_resource_manager = FileResourceManager()
