"""Exercise decisions using explicit typed footer doubles; real formats live in integration/."""
from dataclasses import dataclass
from decimal import Decimal
from types import SimpleNamespace

import pytest

from crowdb_tpc_loader import validation as v
from crowdb_tpc_loader.errors import ValidationError
from crowdb_tpc_loader.models import Generated
from crowdb_tpc_loader.schemas import TPCDS, TPCH


@dataclass(frozen=True)
class Type:
    name: str
    precision: int = 0
    scale: int = 0


class Schema(list):
    @property
    def names(self): return [f.name for f in self]
    def equals(self, other, check_metadata=False):
        return [(f.name,f.type) for f in self] == [(f.name,f.type) for f in other]


@pytest.fixture
def arrow_double(monkeypatch):
    predicates = {"is_" + name: (lambda value, name=name: value.name == name)
                  for name in ("int32","int64","string","large_string","date32","decimal")}
    pa = SimpleNamespace(types=SimpleNamespace(**predicates))
    pq = SimpleNamespace()
    monkeypatch.setattr(v, "require_arrow", lambda: (pa,pq))
    return pa,pq


def schema_for(columns):
    mapping = {"I":Type("int64"),"L":Type("int64"),"S":Type("string"),"D":Type("date32")}
    fields = []
    for column in columns:
        kind = column.kind
        dtype = mapping.get(kind)
        if dtype is None:
            p,s = map(int, kind[1:].split("_"))
            dtype = Type("decimal",p,s)
        fields.append(SimpleNamespace(name=column.name,type=dtype,nullable=True,metadata={}))
    return Schema(fields)


@pytest.mark.parametrize("dtype,kind,expected", [(Type("int32"),"I",True),(Type("int64"),"I",True),
    (Type("int32"),"L",False),(Type("int64"),"L",True),(Type("string"),"S",True),
    (Type("large_string"),"S",True),(Type("date32"),"D",True),(Type("date64"),"D",False),
    (Type("float64"),"D15_2",False),(Type("decimal",15,2),"D15_2",True),
    (Type("decimal",18,2),"D15_2",False),(Type("decimal",15,3),"D15_2",False)])
def test_exact_kind_checks(arrow_double,dtype,kind,expected):
    assert v.compatible_kind(dtype,kind) is expected


def test_unknown_descriptor(arrow_double):
    with pytest.raises(ValueError):
        v.compatible_kind(Type("int64"),"unknown")


def test_column_order_and_embedded_field_ids(arrow_double):
    schema = schema_for(TPCH["region"])
    with pytest.raises(ValidationError,match="names/order"):
        v.validate_schema(Schema(reversed(schema)),TPCH["region"],"region")
    schema[0].metadata = {b"PARQUET:field_id": b"1"}
    with pytest.raises(ValidationError,match="field IDs"):
        v.validate_schema(schema,TPCH["region"],"region")


def setup_dataset(tmp_path, pq):
    lookup = {}
    for name,columns in TPCDS.items():
        path = tmp_path / name / "part-00000.parquet"
        path.parent.mkdir()
        path.write_bytes(b"explicit-footer-double")
        schema = schema_for(columns)
        lookup[path] = SimpleNamespace(num_rows=0, schema=SimpleNamespace(to_arrow_schema=lambda schema=schema:schema))
    pq.read_metadata = lambda path: lookup[path]
    return lookup


def test_empty_complete_dataset_validates(arrow_double,tmp_path):
    _,pq = arrow_double
    setup_dataset(tmp_path,pq)
    result = v.validate_dataset(tmp_path,"tpcds",Decimal(".01"),
        Generated("explicit-double","0",source_row_counts={n:0 for n in TPCDS}),lambda m:None)
    assert len(result) == 24 and all(t.rows == 0 for t in result.values())


def test_missing_source_counts_refused(arrow_double,tmp_path):
    _,pq = arrow_double
    setup_dataset(tmp_path,pq)
    with pytest.raises(ValidationError,match="all 24"):
        v.validate_dataset(tmp_path,"tpcds",Decimal(".01"),Generated("explicit-double","0"),lambda m:None)


def test_source_footer_count_mismatch(arrow_double,tmp_path):
    _,pq = arrow_double
    setup_dataset(tmp_path,pq)
    with pytest.raises(ValidationError,match="source database"):
        v.validate_dataset(tmp_path,"tpcds",Decimal(".01"),
            Generated("explicit-double","0",source_row_counts={"call_center":1}),lambda m:None)


def test_decimal_mismatch_stops_dataset(arrow_double,tmp_path):
    _,pq = arrow_double
    lookup = setup_dataset(tmp_path,pq)
    schema = lookup[tmp_path / "call_center/part-00000.parquet"].schema.to_arrow_schema()
    schema[-1].type = Type("decimal",18,2)
    with pytest.raises(ValidationError,match="No implicit"):
        v.validate_dataset(tmp_path,"tpcds",Decimal(".01"),Generated("explicit-double","0"),lambda m:None)


def test_fractional_tpch_lineitem_bounds():
    values = dict(region=5,nation=25,supplier=100,customer=1500,part=2000,partsupp=8000,orders=15000,lineitem=60000)
    tables = {k:SimpleNamespace(rows=x) for k,x in values.items()}
    v.validate_tpch_counts(tables,Decimal(".01"))
    tables["lineitem"].rows = 15000 * 7 + 1
    with pytest.raises(ValidationError,match="1–7"):
        v.validate_tpch_counts(tables,Decimal(".01"))
