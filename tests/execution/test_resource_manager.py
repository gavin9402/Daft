"""Tests for FileResourceManager."""

from __future__ import annotations

import os
import tarfile
import zipfile

import pytest

from daft.execution.file_resource_manager import _get_extension, _is_archive, _is_supported, file_resource_manager


class TestHelpers:
    """Tests for module-level helper functions."""

    @pytest.mark.parametrize(
        "name,expected",
        [
            ("model.py", True),
            ("lib.egg", True),
            ("archive.zip", True),
            ("data.tar", True),
            ("data.tar.gz", True),
            ("data.tgz", True),
            ("data.tar.bz2", True),
            ("package.whl", True),
            ("model.bin", False),
            ("data.csv", False),
            ("image.png", False),
            ("s3://bucket/model.py", True),
            ("s3://bucket/data.tar.gz", True),
            ("https://example.com/file.txt", False),
            ("archive.zip#/tmp/dir", True),
            ("data.tar.gz#/opt", True),
            ("model.py#something", True),  # '#' is part of name, still .py
        ],
    )
    def test_is_supported(self, name, expected):
        assert _is_supported(name) == expected

    @pytest.mark.parametrize(
        "name,expected",
        [
            ("archive.zip", True),
            ("data.tar", True),
            ("data.tar.gz", True),
            ("data.tgz", True),
            ("data.tar.bz2", True),
            ("package.whl", True),
            ("model.py", False),
            ("lib.egg", False),
            ("archive.zip#/tmp/dir", True),
            ("data.tar.gz#/opt", True),
            ("model.py#something", False),
        ],
    )
    def test_is_archive(self, name, expected):
        assert _is_archive(name) == expected

    @pytest.mark.parametrize(
        "name,expected",
        [
            ("file.py", ".py"),
            ("lib.egg", ".egg"),
            ("data.tar.gz", ".tar.gz"),
            ("data.tar.bz2", ".tar.bz2"),
            ("data.tgz", ".tgz"),
            ("data.tar", ".tar"),
            ("archive.zip", ".zip"),
            ("package.whl", ".whl"),
            ("archive.zip#/tmp/dir", ".zip"),
            ("data.tar.gz#/opt", ".tar.gz"),
        ],
    )
    def test_get_extension(self, name, expected):
        assert _get_extension(name) == expected


class TestFileResourceManager:
    """Tests for FileResourceManager."""

    def test_resolve_empty(self):
        mgr = file_resource_manager
        mgr.resolve({})

    def test_resolve_py_file_to_cwd(self, tmp_path, monkeypatch):
        """A .py file should be downloaded to the current working directory."""
        src = tmp_path / "helper.py"
        src.write_text("print('hello')")

        work_dir = tmp_path / "workdir"
        work_dir.mkdir()
        monkeypatch.chdir(work_dir)

        mgr = file_resource_manager
        mgr.resolve({str(src): 1000})

        result = mgr.get_resource_path(str(src))
        assert result is not None
        assert result == str(work_dir / "helper.py")
        assert os.path.exists(result)
        with open(result) as f:
            assert f.read() == "print('hello')"

    def test_resolve_egg_file_to_cwd(self, tmp_path, monkeypatch):
        """A .egg file should be downloaded to the current working directory."""
        src = tmp_path / "mylib.egg"
        src.write_bytes(b"egg content")

        work_dir = tmp_path / "workdir"
        work_dir.mkdir()
        monkeypatch.chdir(work_dir)

        mgr = file_resource_manager
        mgr.resolve({str(src): 2000})

        result = mgr.get_resource_path(str(src))
        assert result is not None
        assert result == str(work_dir / "mylib.egg")

    def test_resolve_zip_extracts_to_cwd(self, tmp_path, monkeypatch):
        """A .zip archive should be extracted to the current working directory."""
        # Create a zip
        zip_path = tmp_path / "archive.zip"
        with zipfile.ZipFile(zip_path, "w") as zf:
            zf.writestr("inner/data.txt", "zip content")

        work_dir = tmp_path / "workdir"
        work_dir.mkdir()
        monkeypatch.chdir(work_dir)

        mgr = file_resource_manager
        mgr.resolve({str(zip_path): 3000})

        result = mgr.get_resource_path(str(zip_path))
        assert result is not None
        # The zip should be extracted to workdir
        assert os.path.exists(work_dir / "inner" / "data.txt")
        with open(work_dir / "inner" / "data.txt") as f:
            assert f.read() == "zip content"

    def test_resolve_tar_gz_extracts_to_cwd(self, tmp_path, monkeypatch):
        """A .tar.gz archive should be extracted to the current working directory."""
        # Create a tar.gz
        inner_file = tmp_path / "src_content.txt"
        inner_file.write_text("tar content")

        tar_path = tmp_path / "archive.tar.gz"
        with tarfile.open(tar_path, "w:gz") as tf:
            tf.add(inner_file, arcname="src_content.txt")

        work_dir = tmp_path / "workdir"
        work_dir.mkdir()
        monkeypatch.chdir(work_dir)

        mgr = file_resource_manager
        mgr.resolve({str(tar_path): 4000})

        result = mgr.get_resource_path(str(tar_path))
        assert result is not None
        assert os.path.exists(work_dir / "src_content.txt")
        with open(work_dir / "src_content.txt") as f:
            assert f.read() == "tar content"

    def test_resolve_tar_bz2_extracts_to_cwd(self, tmp_path, monkeypatch):
        """A .tar.bz2 archive should be extracted to cwd."""
        inner_file = tmp_path / "bz2_content.txt"
        inner_file.write_text("bz2 data")

        tar_path = tmp_path / "archive.tar.bz2"
        with tarfile.open(tar_path, "w:bz2") as tf:
            tf.add(inner_file, arcname="bz2_content.txt")

        work_dir = tmp_path / "workdir"
        work_dir.mkdir()
        monkeypatch.chdir(work_dir)

        mgr = file_resource_manager
        mgr.resolve({str(tar_path): 5000})

        assert os.path.exists(work_dir / "bz2_content.txt")

    def test_resolve_whl_extracts_to_cwd(self, tmp_path, monkeypatch):
        """A .whl file (zip format) should be extracted to cwd."""
        whl_path = tmp_path / "mypackage-1.0-py3-none-any.whl"
        with zipfile.ZipFile(whl_path, "w") as zf:
            zf.writestr("mypackage/__init__.py", "__version__ = '1.0'")

        work_dir = tmp_path / "workdir"
        work_dir.mkdir()
        monkeypatch.chdir(work_dir)

        mgr = file_resource_manager
        mgr.resolve({str(whl_path): 6000})

        result = mgr.get_resource_path(str(whl_path))
        assert result is not None
        assert os.path.exists(work_dir / "mypackage" / "__init__.py")
        with open(work_dir / "mypackage" / "__init__.py") as f:
            assert f.read() == "__version__ = '1.0'"

    def test_unsupported_type_skipped(self, tmp_path):
        """Unsupported file types are skipped with a warning."""
        src = tmp_path / "data.csv"
        src.write_text("a,b,c")

        mgr = file_resource_manager
        mgr.resolve({str(src): 1000})

        assert mgr.get_resource_path(str(src)) is None

    def test_unsupported_types_examples(self, tmp_path):
        """Various unsupported types are all rejected."""
        mgr = file_resource_manager

        for ext in (".csv", ".txt", ".bin", ".png", ".json", ".parquet"):
            src = tmp_path / f"file{ext}"
            src.write_text("x")
            mgr.resolve({str(src): 1000})
            assert mgr.get_resource_path(str(src)) is None

    def test_resolve_skips_already_resolved(self, tmp_path, monkeypatch):
        """Resources already resolved are skipped."""
        src = tmp_path / "script.py"
        src.write_text("x = 1")

        work_dir = tmp_path / "workdir"
        work_dir.mkdir()
        monkeypatch.chdir(work_dir)

        mgr = file_resource_manager
        mgr.resolve({str(src): 1000})
        path1 = mgr.get_resource_path(str(src))

        mgr.resolve({str(src): 1000})
        path2 = mgr.get_resource_path(str(src))
        assert path1 == path2

    def test_get_resource_path_unknown(self):
        mgr = file_resource_manager
        assert mgr.get_resource_path("nonexistent") is None

    def test_resolve_nonexistent_resource(self, tmp_path):
        """Resolving a nonexistent .py resource returns None."""
        mgr = file_resource_manager
        mgr.resolve({"/nonexistent/path/script.py": 3000})
        assert mgr.get_resource_path("/nonexistent/path/script.py") is None

    def test_resolve_multiple_resources(self, tmp_path, monkeypatch):
        """Multiple resources can be resolved at once."""
        file_a = tmp_path / "a.py"
        file_a.write_text("a")
        file_b = tmp_path / "b.py"
        file_b.write_text("b")

        work_dir = tmp_path / "workdir"
        work_dir.mkdir()
        monkeypatch.chdir(work_dir)

        mgr = file_resource_manager
        mgr.resolve({str(file_a): 1000, str(file_b): 2000})

        assert mgr.get_resource_path(str(file_a)) is not None
        assert mgr.get_resource_path(str(file_b)) is not None


class TestContextIntegration:
    """Tests for added_resources in DaftContext."""

    def test_added_resources_roundtrip(self):
        import daft

        ctx = daft.context.get_context()
        original = ctx.added_resources

        ctx.added_resources = {"test_res": 12345}
        assert ctx.added_resources == {"test_res": 12345}

        ctx.added_resources = original


class TestRemoteDownload:
    """Tests for remote URI download support."""

    def test_download_remote_py_file(self, tmp_path, monkeypatch):
        """Remote .py files are downloaded via daft.File and placed in cwd."""
        work_dir = tmp_path / "workdir"
        work_dir.mkdir()
        monkeypatch.chdir(work_dir)

        mgr = file_resource_manager

        class MockFileHandle:
            def read(self, size=None):
                return b"print('remote')"

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

        mgr.resolve({"s3://bucket/model.py": 5000})
        local_path = mgr.get_resource_path("s3://bucket/model.py")
        assert local_path is not None
        assert local_path == str(work_dir / "model.py")
        with open(local_path) as f:
            assert f.read() == "print('remote')"

    def test_download_remote_failure_returns_none(self, tmp_path, monkeypatch):
        mgr = file_resource_manager

        class FailingFile:
            def __init__(self, url, **kwargs):
                pass

            def open(self):
                raise ConnectionError("Network unreachable")

        import daft.file

        monkeypatch.setattr(daft.file, "File", FailingFile)

        mgr.resolve({"s3://nonexistent/script.py": 6000})
        assert mgr.get_resource_path("s3://nonexistent/script.py") is None

    def test_unsupported_remote_type_rejected(self, tmp_path):
        """Remote URIs with unsupported extensions are rejected."""
        mgr = file_resource_manager

        mgr.resolve({"s3://bucket/model.bin": 7000})
        assert mgr.get_resource_path("s3://bucket/model.bin") is None


class TestParseResourceName:
    """Tests for _parse_resource_name."""

    @pytest.mark.parametrize(
        "name,expected",
        [
            ("archive.zip", ("archive.zip", None)),
            ("archive.zip#", ("archive.zip", None)),
            ("archive.zip#/tmp/dir", ("archive.zip", "/tmp/dir")),
            ("s3://bucket/data.tar.gz#/opt/data", ("s3://bucket/data.tar.gz", "/opt/data")),
            ("model.py", ("model.py", None)),
            ("model.py#something", ("model.py#something", None)),  # non-archive, # is part of name
            ("data.tar.bz2#/extract", ("data.tar.bz2", "/extract")),
            ("package.whl#/wheels", ("package.whl", "/wheels")),
        ],
    )
    def test_parse_resource_name(self, name, expected):
        assert _parse_resource_name(name) == expected


class TestArchiveExtractPath:
    """Tests for archive extraction with #path."""

    def test_resolve_zip_extracts_to_custom_path(self, tmp_path, monkeypatch):
        """archive.zip#/path should extract to the specified path."""
        zip_path = tmp_path / "archive.zip"
        with zipfile.ZipFile(zip_path, "w") as zf:
            zf.writestr("inner/data.txt", "custom path content")

        cache_dir = str(tmp_path / "cache")
        work_dir = tmp_path / "workdir"
        work_dir.mkdir()
        monkeypatch.chdir(work_dir)

        extract_dir = str(tmp_path / "custom_extract")
        resource_name = f"{zip_path}#{extract_dir}"

        mgr = FileResourceManager(cache_dir=cache_dir)
        mgr.resolve({resource_name: 3000})

        result = mgr.get_resource_path(resource_name)
        assert result is not None
        assert result == extract_dir
        assert os.path.exists(os.path.join(extract_dir, "inner", "data.txt"))

    def test_resolve_zip_empty_hash_extracts_to_cwd(self, tmp_path, monkeypatch):
        """archive.zip# (empty path) should extract to cwd."""
        zip_path = tmp_path / "archive.zip"
        with zipfile.ZipFile(zip_path, "w") as zf:
            zf.writestr("data.txt", "cwd content")

        cache_dir = str(tmp_path / "cache")
        work_dir = tmp_path / "workdir"
        work_dir.mkdir()
        monkeypatch.chdir(work_dir)

        resource_name = f"{zip_path}#"

        mgr = FileResourceManager(cache_dir=cache_dir)
        mgr.resolve({resource_name: 3000})

        assert os.path.exists(work_dir / "data.txt")

    def test_resolve_tar_gz_extracts_to_custom_path(self, tmp_path, monkeypatch):
        """data.tar.gz#/path should extract to the specified path."""
        inner_file = tmp_path / "content.txt"
        inner_file.write_text("tar custom path")

        tar_path = tmp_path / "data.tar.gz"
        with tarfile.open(tar_path, "w:gz") as tf:
            tf.add(inner_file, arcname="content.txt")

        cache_dir = str(tmp_path / "cache")
        work_dir = tmp_path / "workdir"
        work_dir.mkdir()
        monkeypatch.chdir(work_dir)

        extract_dir = str(tmp_path / "tar_extract")
        resource_name = f"{tar_path}#{extract_dir}"

        mgr = FileResourceManager(cache_dir=cache_dir)
        mgr.resolve({resource_name: 4000})

        assert os.path.exists(os.path.join(extract_dir, "content.txt"))

    def test_resolve_same_archive_different_paths(self, tmp_path, monkeypatch):
        """Same archive with different #path should be treated as different resources."""
        zip_path = tmp_path / "archive.zip"
        with zipfile.ZipFile(zip_path, "w") as zf:
            zf.writestr("file.txt", "hello")

        cache_dir = str(tmp_path / "cache")
        work_dir = tmp_path / "workdir"
        work_dir.mkdir()
        monkeypatch.chdir(work_dir)

        dir_a = str(tmp_path / "dir_a")
        dir_b = str(tmp_path / "dir_b")

        mgr = FileResourceManager(cache_dir=cache_dir)
        mgr.resolve({f"{zip_path}#{dir_a}": 1000, f"{zip_path}#{dir_b}": 1000})

        assert mgr.get_resource_path(f"{zip_path}#{dir_a}") == dir_a
        assert mgr.get_resource_path(f"{zip_path}#{dir_b}") == dir_b
        assert os.path.exists(os.path.join(dir_a, "file.txt"))
        assert os.path.exists(os.path.join(dir_b, "file.txt"))
