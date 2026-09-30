"""REAL Parquet tests: skipped, not simulated, when PyArrow is absent."""
from decimal import Decimal

import pytest

pa = pytest.importorskip("pyarrow")
pq = pytest.importorskip("pyarrow.parquet")

from crowdb_tpc_loader.errors import ValidationError
from crowdb_tpc_loader.models import Generated
from crowdb_tpc_loader.schemas import TPCH, TPCDS, arrow_schema, inventory
from crowdb_tpc_loader.validation import validate_schema, validate_dataset

pytestmark = pytest.mark.integration


@pytest.mark.parametrize("benchmark,table", [(b,t) for b in ("tpch","tpcds") for t in inventory(b)])
def test_all_32_empty_parquet_schemas(benchmark, table, tmp_path):
    schema = arrow_schema(benchmark, table)
    path = tmp_path / (table + ".parquet")
    pq.write_table(schema.empty_table(), path)
    footer = pq.read_metadata(path)
    assert footer.num_rows == 0
    actual = footer.schema.to_arrow_schema()
    validate_schema(actual, inventory(benchmark)[table], table)
    assert actual.equals(schema)
    assert pq.read_table(path).num_rows == 0


def test_real_tpcds_multipart_footer_aggregation(tmp_path):
    for name in TPCDS:
        folder = tmp_path / name
        folder.mkdir()
        schema = arrow_schema("tpcds", name)
        pq.write_table(schema.empty_table(), folder / "part-00000.parquet")
        pq.write_table(schema.empty_table(), folder / "part-00001.parquet")
    result = validate_dataset(tmp_path, "tpcds", Decimal(".01"),
                              Generated("explicit-empty-schema-fixture", "test", source_row_counts={n:0 for n in TPCDS}),
                              lambda message: None)
    assert len(result) == 24 and all(len(t.parts) == 2 and t.rows == 0 for t in result.values())


def test_decimal_precision_and_date_are_not_coerced():
    schema = arrow_schema("tpch", "orders")
    index = schema.get_field_index("o_totalprice")
    bad_decimal = schema.set(index, pa.field("o_totalprice", pa.decimal128(18,2)))
    with pytest.raises(ValidationError, match="o_totalprice"):
        validate_schema(bad_decimal, TPCH["orders"], "orders")
    index = schema.get_field_index("o_orderdate")
    bad_date = schema.set(index, pa.field("o_orderdate", pa.timestamp("us")))
    with pytest.raises(ValidationError, match="o_orderdate"):
        validate_schema(bad_date, TPCH["orders"], "orders")


def test_corrupt_parquet_footer(tmp_path):
    for name in TPCDS:
        pq.write_table(arrow_schema("tpcds", name).empty_table(), tmp_path / (name + ".parquet"))
    (tmp_path / "store_sales.parquet").write_bytes(b"not a parquet file")
    with pytest.raises(ValidationError, match="footer"):
        validate_dataset(tmp_path, "tpcds", Decimal(1),
                         Generated("fixture","test",source_row_counts={n:0 for n in TPCDS}), lambda m:None)
