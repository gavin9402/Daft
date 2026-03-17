# @Author  : yiyi.zt
# @File    : test_paimon.py
from __future__ import annotations

import os
import tempfile

import pyarrow as pa
import pytest
from pypaimon import CatalogFactory, Schema
from pypaimon.table.file_store_table import FileStoreTable

import daft
from daft import Catalog, Session, col
from daft.daft import PyPushdowns
from daft.io.paimon.paimon_predicate_visitor import PaimonPredicateVisitor

CATALOG_ALIAS = "_test_catalog_paimon"
DEFAULT_DB = "default"
DEFAULT_TBL = "test_append_only_parquet"
ident = f"{DEFAULT_DB}.{DEFAULT_TBL}"
tempdir = tempfile.mkdtemp()
warehouse = os.path.join(tempdir, "warehouse")
_catalog = CatalogFactory.create({"warehouse": warehouse})
_catalog.create_database(DEFAULT_DB, True)

pa_schema = pa.schema(
    [("user_id", pa.int32()), ("item_id", pa.int64()), ("behavior", pa.string()), ("dt", pa.string())]
)
expected = pa.Table.from_pydict(
    {
        "user_id": [1, 2, 3, 4, 5, 6, 7, 8],
        "item_id": [1001, 1002, 1003, 1004, 1005, 1006, 1007, 1008],
        "behavior": ["a", "b", "c", None, "e", "f", "g", "h"],
        "dt": ["p1", "p1", "p2", "p1", "p2", "p1", "p2", "p2"],
    },
    schema=pa_schema,
)
schema = Schema.from_pyarrow_schema(pa_schema, partition_keys=["dt"])
_catalog.create_table(f"{DEFAULT_DB}.{DEFAULT_TBL}", schema, False)


@pytest.fixture(scope="session")
def paimon_catalog():
    return _catalog


@pytest.fixture(scope="session")
def global_sess(paimon_catalog):
    daft.attach_catalog(paimon_catalog, alias=CATALOG_ALIAS)
    yield daft.current_session()
    daft.detach_catalog(alias=CATALOG_ALIAS)


@pytest.fixture(scope="session")
def sess(paimon_catalog):
    sess = Session()
    sess.attach_catalog(paimon_catalog, alias=CATALOG_ALIAS)
    yield sess
    sess.detach_catalog(alias=CATALOG_ALIAS)


@pytest.fixture(scope="session")
def catalog(paimon_catalog):
    return Catalog.from_paimon(paimon_catalog)


def test_read_ao(sess: Session):
    daft.context.with_extra_options({"daft.paimon.source.split.target-size": "32mb"})
    table = _catalog.get_table(f"{DEFAULT_DB}.{DEFAULT_TBL}")
    _write_test_table(table)

    table = sess.get_table(f"{DEFAULT_DB}.{DEFAULT_TBL}")
    print(table.schema())

    # df = sess.sql(f"select * from {DEFAULT_DB}.{DEFAULT_TBL} "
    #               f"where dt < 'p1' and dt > cast((abs(1) + 2) as string) and user_id > abs(1)")
    df = sess.read_table(ident).select("user_id", "item_id", "dt").filter("dt > 'p1'").filter("user_id > 1")
    print(df.collect())


def _write_test_table(table):
    write_builder = table.new_batch_write_builder()

    # first write
    table_write = write_builder.new_write()
    table_commit = write_builder.new_commit()
    data1 = {
        "user_id": [1, 2, 1, 4],
        "item_id": [1001, 1002, 1001, 1004],
        "behavior": ["a", "b", "c", None],
        "dt": ["p1", "p1", "p2", "p1"],
    }
    pa_table = pa.Table.from_pydict(data1, schema=pa_schema)
    table_write.write_arrow(pa_table)
    table_commit.commit(table_write.prepare_commit())
    table_write.close()
    table_commit.close()

    # second write
    table_write = write_builder.new_write()
    table_commit = write_builder.new_commit()
    data2 = {
        "user_id": [2, 3, 3, 4],
        "item_id": [1002, 1003, 1003, 1004],
        "behavior": ["e", "f", "g", "h"],
        "dt": ["p2", "p1", "p2", "p2"],
    }
    pa_table = pa.Table.from_pydict(data2, schema=pa_schema)
    table_write.write_arrow(pa_table)
    table_commit.commit(table_write.prepare_commit())
    table_write.close()
    table_commit.close()


def _read_test_table(read_builder):
    table_read = read_builder.new_read()
    splits = read_builder.new_scan().plan().splits()
    return table_read.to_arrow(splits)


def test_predicate():
    tbl: FileStoreTable = _catalog.get_table(ident)
    read_builder = tbl.new_read_builder()
    visitor = PaimonPredicateVisitor(read_builder)

    # expr = col("age") > 30
    partition_expr = col("dt") == "2023"
    pushdowns = PyPushdowns(partition_filters=partition_expr._expr)
    predicate = pushdowns.partition_filters.accept(visitor)
    print(predicate)
