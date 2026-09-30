from dataclasses import replace
from decimal import Decimal
from pathlib import Path
import hashlib
import io
import json
import os
import platform
import stat
import zipfile

import pytest
from packaging.tags import Tag

from crowdb_tpc_loader.errors import GenerationError
from crowdb_tpc_loader.generators import binary
from crowdb_tpc_loader.generators.tpch import build_command, native_environment, parse_version, TPCHGenerator
from crowdb_tpc_loader.generators.tpcds import sql_literal


def item(filename, **extra):
    return {"filename": filename, "packagetype": "bdist_wheel", "yanked": False, **extra}


def test_wheel_select_native_only():
    files = [item("tpchgen_cli-3.0.0.tar.gz", packagetype="sdist"),
             item("tpchgen_cli-3.0.0-py3-none-manylinux_2_17_x86_64.whl"),
             item("tpchgen_cli-3.0.0-py3-none-win_amd64.whl"),
             item("tpchgen_cli-2.0.0-py3-none-manylinux_2_17_x86_64.whl"),
             item("evil-3.0.0-py3-none-manylinux_2_17_x86_64.whl")]
    tags = [Tag("py3", "none", "manylinux_2_17_x86_64")]
    assert binary.choose_wheel(files, tags) == files[1]
    with pytest.raises(GenerationError, match="No source build"):
        binary.choose_wheel([files[0], files[2]], tags)


def test_yanked_wheel_refused():
    with pytest.raises(GenerationError):
        binary.choose_wheel([item("tpchgen_cli-3.0.0-py3-none-any.whl", yanked=True)], [Tag("py3","none","any")])


def test_extract_only_native_executable(tmp_path):
    wheel = tmp_path / "input.whl"
    executable = "tpchgen-cli.exe" if os.name == "nt" else "tpchgen-cli"
    with zipfile.ZipFile(wheel, "w") as archive:
        archive.writestr("tpchgen_cli-3.0.0.data/scripts/" + executable, b"FAKE TEST EXECUTABLE - NEVER EXECUTED")
        archive.writestr("../../evil.py", b"must not extract")
        archive.writestr("package/__init__.py", b"must not execute")
    binary.extract_binary(wheel, tmp_path / "binary")
    assert (tmp_path / "binary").read_bytes().startswith(b"FAKE TEST")
    assert {p.name for p in tmp_path.iterdir()} == {"input.whl", "binary"}


def test_symlink_executable_refused(tmp_path):
    wheel = tmp_path / "input.whl"
    executable = "tpchgen-cli.exe" if os.name == "nt" else "tpchgen-cli"
    with zipfile.ZipFile(wheel, "w") as archive:
        info = zipfile.ZipInfo("tpchgen_cli-3.0.0.data/scripts/" + executable)
        info.create_system = 3
        info.external_attr = (stat.S_IFLNK | 0o777) << 16
        archive.writestr(info, "/etc/passwd")
    with pytest.raises(GenerationError):
        binary.extract_binary(wheel, tmp_path / "binary")


def test_no_download_never_invokes_network(monkeypatch, tmp_path):
    monkeypatch.setenv("CROWDB_TPC_CACHE", str(tmp_path))
    monkeypatch.setattr(binary, "_request", lambda *args: pytest.fail("network must not run"))
    with pytest.raises(GenerationError, match="--no-download"):
        binary.get_binary(True, 1, lambda message: None)


def test_cached_binary_hash_checked(monkeypatch, tmp_path):
    monkeypatch.setenv("CROWDB_TPC_CACHE", str(tmp_path))
    root = tmp_path / "tpchgen-cli" / "3.0.0" / f"{platform.system()}-{platform.machine()}"
    root.mkdir(parents=True)
    path = root / ("tpchgen-cli.exe" if os.name == "nt" else "tpchgen-cli")
    content = b"fake-cache-never-executed"
    path.write_bytes(content)
    (root / "receipt.json").write_text(json.dumps({"version":"3.0.0", "binary_sha256":hashlib.sha256(content).hexdigest()}))
    assert binary.get_binary(True, 1, lambda m: None)[0] == path
    path.write_bytes(b"tampered")
    with pytest.raises(GenerationError, match="integrity"):
        binary.get_binary(True, 1, lambda m: None)


@pytest.mark.parametrize("text", ["tpchgen-cli 3.0.0", "tpchgen 3.1.2"])
def test_parse_binary_version(text):
    assert parse_version(text) == text.split()[-1]


@pytest.mark.parametrize("text", ["tpchgen-cli 2.0.0", "3.0.0", "random executable 3.0.0"])
def test_unrecognized_version(text):
    with pytest.raises(GenerationError):
        parse_version(text)


def test_command_has_partition_and_memory_boundaries(options):
    options = replace(options, sf=Decimal(25), threads=3)
    command = build_command(Path("/bin/tpchgen-cli"), options, Path("/tmp/out with spaces"), "--num-threads --no-progress")
    assert command[:2] == ["/bin/tpchgen-cli", "parquet"]
    assert command[command.index("--parts") + 1] == "3"
    assert command[command.index("--num-threads") + 1] == "3"
    assert "--row-group-bytes" in command and "--no-progress" in command
    assert "/tmp/out with spaces" in command  # direct argv, no shell splitting


def test_child_has_no_catalog_secrets(monkeypatch):
    monkeypatch.setenv("ICEBERG_TOKEN", "must-not-reach-child")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "must-not-reach-child")
    monkeypatch.setenv("SOME_NON_SECRET_CONFIG", "removed-due-to-key-name")
    env = native_environment(2)
    assert "ICEBERG_TOKEN" not in env and "AWS_SECRET_ACCESS_KEY" not in env
    assert env["RAYON_NUM_THREADS"] == "2" and env["NO_COLOR"] == "1"


def test_sql_literal_escaping():
    assert sql_literal("a'b/c") == "'a''b/c'"
