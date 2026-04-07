"""Tests for ResourceManager."""

from __future__ import annotations

import os

import pytest

from daft.execution.resource_manager import DefaultResourceManager, ResourceManager


class TestResourceManager:
    """Tests for the ResourceManager abstract interface."""

    def test_is_abstract(self):
        """ResourceManager cannot be instantiated directly."""
        with pytest.raises(TypeError):
            ResourceManager()

    def test_subclass_must_implement_resolve(self):
        """Subclasses must implement resolve."""

        class BadManager(ResourceManager):
            def get_resource_path(self, name):
                return None

        with pytest.raises(TypeError):
            BadManager()

    def test_subclass_must_implement_get_resource_path(self):
        """Subclasses must implement get_resource_path."""

        class BadManager(ResourceManager):
            def resolve(self, added_resources):
                pass

        with pytest.raises(TypeError):
            BadManager()


class TestDefaultResourceManager:
    """Tests for DefaultResourceManager."""

    def test_creation_default_cache_dir(self):
        """DefaultResourceManager creates a cache dir."""
        mgr = DefaultResourceManager()
        assert os.path.isdir(mgr.cache_dir)

    def test_creation_custom_cache_dir(self, tmp_path):
        """DefaultResourceManager uses custom cache dir."""
        cache_dir = str(tmp_path / "my_cache")
        mgr = DefaultResourceManager(cache_dir=cache_dir)
        assert mgr.cache_dir == cache_dir
        assert os.path.isdir(cache_dir)

    def test_resolve_empty(self):
        """Resolving empty resources is a no-op."""
        mgr = DefaultResourceManager()
        mgr.resolve({})

    def test_resolve_local_file(self, tmp_path):
        """Resolving a local file copies it to cache."""
        # Create a temp file
        src_file = tmp_path / "test_resource.txt"
        src_file.write_text("hello resource")

        cache_dir = str(tmp_path / "cache")
        mgr = DefaultResourceManager(cache_dir=cache_dir)
        mgr.resolve({str(src_file): 1000})

        local_path = mgr.get_resource_path(str(src_file))
        assert local_path is not None
        assert os.path.exists(local_path)
        with open(local_path) as f:
            assert f.read() == "hello resource"

    def test_resolve_local_directory(self, tmp_path):
        """Resolving a local directory copies it to cache."""
        src_dir = tmp_path / "test_dir"
        src_dir.mkdir()
        (src_dir / "file.txt").write_text("content")

        cache_dir = str(tmp_path / "cache")
        mgr = DefaultResourceManager(cache_dir=cache_dir)
        mgr.resolve({str(src_dir): 2000})

        local_path = mgr.get_resource_path(str(src_dir))
        assert local_path is not None
        assert os.path.isdir(local_path)

    def test_resolve_skips_already_resolved(self, tmp_path):
        """Resources already resolved are skipped."""
        src_file = tmp_path / "test.txt"
        src_file.write_text("data")

        cache_dir = str(tmp_path / "cache")
        mgr = DefaultResourceManager(cache_dir=cache_dir)
        mgr.resolve({str(src_file): 1000})
        path1 = mgr.get_resource_path(str(src_file))

        # Resolve again - should skip
        mgr.resolve({str(src_file): 1000})
        path2 = mgr.get_resource_path(str(src_file))
        assert path1 == path2

    def test_get_resource_path_unknown(self):
        """get_resource_path returns None for unknown resources."""
        mgr = DefaultResourceManager()
        assert mgr.get_resource_path("nonexistent") is None

    def test_resolve_nonexistent_resource(self, tmp_path):
        """Resolving a nonexistent resource warns but doesn't crash."""
        cache_dir = str(tmp_path / "cache")
        mgr = DefaultResourceManager(cache_dir=cache_dir)
        mgr.resolve({"/nonexistent/path/resource.bin": 3000})
        assert mgr.get_resource_path("/nonexistent/path/resource.bin") is None

    def test_resolve_multiple_resources(self, tmp_path):
        """Multiple resources can be resolved at once."""
        file_a = tmp_path / "a.txt"
        file_a.write_text("aaa")
        file_b = tmp_path / "b.txt"
        file_b.write_text("bbb")

        cache_dir = str(tmp_path / "cache")
        mgr = DefaultResourceManager(cache_dir=cache_dir)
        mgr.resolve({str(file_a): 1000, str(file_b): 2000})

        assert mgr.get_resource_path(str(file_a)) is not None
        assert mgr.get_resource_path(str(file_b)) is not None


class TestContextIntegration:
    """Tests for added_resources in DaftContext."""

    def test_added_resources_roundtrip(self):
        """Test that added_resources can be set and retrieved."""
        import daft

        ctx = daft.context.get_context()
        original = ctx.added_resources

        ctx.added_resources = {"test_res": 12345}
        assert ctx.added_resources == {"test_res": 12345}

        # Cleanup
        ctx.added_resources = original


class TestRemoteDownload:
    """Tests for remote URI download support."""

    def test_download_remote_via_daft_file(self, tmp_path, monkeypatch):
        """Test that remote URIs are downloaded via daft.File."""
        cache_dir = str(tmp_path / "cache")
        mgr = DefaultResourceManager(cache_dir=cache_dir)

        # Mock daft.File to avoid real network calls
        class MockFileHandle:
            def read(self, size=None):
                return b"remote file content"

            def __enter__(self):
                return self

            def __exit__(self, *args):
                pass

        class MockFile:
            def __init__(self, url, **kwargs):
                self.url = url

            def open(self):
                return MockFileHandle()

        import daft.file

        monkeypatch.setattr(daft.file, "File", MockFile)

        mgr.resolve({"s3://bucket/model.bin": 5000})
        local_path = mgr.get_resource_path("s3://bucket/model.bin")
        assert local_path is not None
        assert os.path.exists(local_path)
        with open(local_path, "rb") as f:
            assert f.read() == b"remote file content"

    def test_download_remote_failure_returns_none(self, tmp_path, monkeypatch):
        """Test that failed remote downloads return None gracefully."""
        cache_dir = str(tmp_path / "cache")
        mgr = DefaultResourceManager(cache_dir=cache_dir)

        class FailingFile:
            def __init__(self, url, **kwargs):
                pass

            def open(self):
                raise ConnectionError("Network unreachable")

        import daft.file

        monkeypatch.setattr(daft.file, "File", FailingFile)

        mgr.resolve({"s3://nonexistent/file.bin": 6000})
        assert mgr.get_resource_path("s3://nonexistent/file.bin") is None
