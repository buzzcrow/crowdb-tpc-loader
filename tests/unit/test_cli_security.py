from decimal import Decimal
import argparse
import json
import logging
import os
import subprocess
import sys
from pathlib import Path

import pytest

from crowdb_tpc_loader import cli
from crowdb_tpc_loader.errors import ArgumentError
from crowdb_tpc_loader.security import Redactor, RedactingFilter


def parse(argv):
    redactor = cli.initial_redactor(argv)
    return cli.to_options(cli.build_parser(redactor).parse_args(argv), redactor)


@pytest.mark.parametrize(
    "value", ["0", "-1", "NaN", "Infinity", "-Infinity", "1e999", "1e-999", "bad", "9" * 65]
)
def test_invalid_sf(value):
    with pytest.raises(argparse.ArgumentTypeError):
        cli.positive_sf(value)


@pytest.mark.parametrize("value", ["1", ".01", "2.5", "1e2"])
def test_valid_sf(value):
    assert cli.positive_sf(value) == Decimal(value)


@pytest.mark.parametrize("value", ["0", "-1", "x", "1.1"])
def test_invalid_positive_int(value):
    with pytest.raises(argparse.ArgumentTypeError):
        cli.positive_int(value)


@pytest.mark.parametrize("value", ["0", "-1", "NaN", "inf", "x"])
def test_invalid_timeout(value):
    with pytest.raises(argparse.ArgumentTypeError):
        cli.positive_timeout(value)


def test_defaults_and_env(monkeypatch):
    monkeypatch.setenv("ICEBERG_URI", "https://example.test/catalog/")
    monkeypatch.setenv("ICEBERG_TOKEN", "environment-secret")
    opts = parse(["load", "--benchmark", "tpcds"])
    assert opts.sf == 1 and opts.namespace == ("tpcds",)
    assert opts.catalog_uri == "https://example.test/catalog"
    assert opts.token == "environment-secret" and opts.on_exists == "error"
    assert opts.upload_workers == 8
    assert "environment-secret" not in repr(opts)


def test_cli_wins_over_env(monkeypatch):
    monkeypatch.setenv("ICEBERG_URI", "https://env.test")
    monkeypatch.setenv("ICEBERG_TOKEN", "env-secret")
    opts = parse(
        [
            "load",
            "--benchmark",
            "tpch",
            "--catalog-uri",
            "https://cli.test",
            "--token",
            "cli-secret",
            "--namespace",
            "bench.tpch_01",
        ]
    )
    assert opts.catalog_uri == "https://cli.test" and opts.token == "cli-secret"
    assert opts.namespace == ("bench", "tpch_01")


@pytest.mark.parametrize(
    "uri",
    [
        "file:///tmp/a",
        "/tmp/a",
        "http://user:pass@x",
        "http://x?token=a",
        "http://x#frag",
        "http://x:notaport",
        "ftp://host/dir",
    ],
)
def test_bad_catalog_uri(uri):
    with pytest.raises(ArgumentError):
        parse(["load", "--benchmark", "tpch", "--catalog-uri", uri])


@pytest.mark.parametrize(
    "extra",
    [
        ["--namespace", "a..b"],
        ["--namespace", "../evil"],
        ["--upload-buffer-mib", "65"],
        ["--upload-workers", "25"],
        ["--token", "a\rb"],
        ["--catalog-property", "token=bad"],
        ["--catalog-property", "bad"],
        ["--memory-limit", "0GB"],
        ["--memory-limit", "1GB; DROP"],
    ],
)
def test_invalid_load_options(extra):
    with pytest.raises(ArgumentError):
        parse(["load", "--benchmark", "tpch", "--catalog-uri", "https://x"] + extra)


def test_generate_needs_no_credentials(monkeypatch):
    monkeypatch.delenv("ICEBERG_URI", raising=False)
    opts = parse(["generate", "--benchmark", "tpch", "--output-dir", "out"])
    assert opts.output_dir == Path("out") and opts.catalog_uri is None


def test_explicit_fileio_properties():
    opts = parse(
        [
            "load",
            "--benchmark",
            "tpch",
            "--catalog-uri",
            "https://x",
            "--py-io-impl",
            "module.IO",
            "--catalog-property",
            "s3.region=eu-test-1",
        ]
    )
    assert opts.catalog_properties == {"py-io-impl": "module.IO", "s3.region": "eu-test-1"}


def test_help_imports_without_runtime_deps(tmp_path):
    source = Path(__file__).resolve().parents[2] / "src"
    # -S excludes ALL site-packages. The entry point must still display help/version.
    env = {**os.environ, "PYTHONPATH": str(source)}
    for args in (["--help"], ["--version"], ["load", "--help"], ["generate", "--help"]):
        result = subprocess.run(
            [sys.executable, "-S", "-m", "crowdb_tpc_loader", *args],
            env=env,
            cwd=tmp_path,
            capture_output=True,
            text=True,
            timeout=10,
        )
        assert result.returncode == 0, result.stderr
        assert "crowdb-tpc-loader" in result.stdout


def test_parse_error_redacts_cli_token(capsys):
    argv = ["load", "--benchmark", "tpch", "--token", "superSecret987", "--unknown", "superSecret987"]
    with pytest.raises(SystemExit) as exc:
        cli.build_parser(cli.initial_redactor(argv)).parse_args(argv)
    assert exc.value.code == 2
    assert "superSecret987" not in capsys.readouterr().err


def test_redactor_nested_and_uri():
    red = Redactor(["opaque&secret"])
    payload = {
        "message": "opaque&secret opaque%26secret https://u:p@h/path?sig=secret#frag",
        "s3.secret-access-key": "hidden",
        "nested": ["Bearer ABC123", "token=abc"],
        "run_id": "abc",
        "rows": 10,
    }
    text = json.dumps(red.data(payload))
    for secret in ("opaque&secret", "opaque%26secret", "u:p@", "?sig", "#frag", "ABC123", "hidden"):
        assert secret not in text
    assert "https://h/path" in text and '"rows": 10' in text
    assert red.text("\x1b[31mevil\x00") == "?[31mevil?"


def test_log_exception_redacted():
    red = Redactor(["hidden-password"])
    try:
        raise RuntimeError("hidden-password")
    except RuntimeError:
        record = logging.LogRecord(
            "test", logging.ERROR, __file__, 1, "bad %s", ("hidden-password",), sys.exc_info()
        )
    assert RedactingFilter(red).filter(record)
    assert "hidden-password" not in str(record.msg) + str(record.exc_text)
    assert record.exc_info is None


def test_secret_property_parse_failure():
    red = cli.initial_redactor(["--catalog-property", "http.token=secret987"])
    assert "secret987" not in red.text("request failed: secret987")


def test_inline_property_and_nested_secrets():
    red = cli.initial_redactor(["--catalog-property=header.X-Api-Key=inlineKey123"])
    assert "inlineKey123" not in red.text("failed with inlineKey123")
    red.learn({"auth": {"client-secret": "nestedSecret123"}})
    assert "nestedSecret123" not in red.text("failed with nestedSecret123")


def test_empty_explicit_namespace_refused():
    with pytest.raises(ArgumentError):
        parse(["load", "--benchmark", "tpch", "--catalog-uri", "https://x", "--namespace", ""])
