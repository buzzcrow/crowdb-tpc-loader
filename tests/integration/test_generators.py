"""Explicitly opt-in real native generation, with complete footer validation."""
from decimal import Decimal
from pathlib import Path
import os

import pytest

from crowdb_tpc_loader.generators import make_generator
from crowdb_tpc_loader.models import Options
from crowdb_tpc_loader.schemas import inventory
from crowdb_tpc_loader.security import Redactor
from crowdb_tpc_loader.validation import validate_dataset

pytestmark = [pytest.mark.integration, pytest.mark.generator]


@pytest.mark.parametrize("benchmark", ["tpch", "tpcds"])
@pytest.mark.parametrize("sf", [".01", "1"])
def test_complete_native_dataset(benchmark, sf, tmp_path):
    if os.getenv("CROWDB_TPC_GENERATOR_TESTS") != "1":
        pytest.skip("set CROWDB_TPC_GENERATOR_TESTS=1 to invoke real native generators")
    if sf == "1" and os.getenv("CROWDB_TPC_SF1_TESTS") != "1":
        pytest.skip("set CROWDB_TPC_SF1_TESTS=1 for complete SF1 generation")
    pytest.importorskip("pyarrow")
    if benchmark == "tpcds":
        pytest.importorskip("duckdb")
    executable = os.getenv("TPC_H_GENERATOR")
    options = Options("generate", benchmark, Decimal(sf), output_dir=tmp_path,
                      tpchgen=Path(executable) if executable else None)
    generator = make_generator(options, print, Redactor())
    scratch = tmp_path / "scratch"
    scratch.mkdir()
    try:
        generator.prepare(scratch)
        generated = generator.generate(tmp_path / "data", scratch)
        result = validate_dataset(tmp_path / "data", benchmark, Decimal(sf), generated, print)
        assert set(result) == set(inventory(benchmark))
    finally:
        generator.close()
