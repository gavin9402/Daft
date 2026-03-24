from __future__ import annotations

from typing import TYPE_CHECKING

from pypaimon.schema.data_types import PyarrowFieldParser

from daft import Schema
from daft.io import DataSource, DataSourceTask
from daft.io.paimon.paimon_predicate_visitor import PaimonPredicateVisitor
from daft.io.partitioning import PartitionField, PartitionTransform
from daft.recordbatch import MicroPartition

if TYPE_CHECKING:
    from collections.abc import Iterator

    from pypaimon.read.split import Split
    from pypaimon.read.table_read import TableRead
    from pypaimon.schema.table_schema import TableSchema
    from pypaimon.table.file_store_table import FileStoreTable

    from daft.io.pushdowns import Pushdowns

from daft.dependencies import pa


class PaimonDataSource(DataSource):
    def __init__(self, paimon_table: FileStoreTable) -> None:
        super().__init__()
        self._table = paimon_table

        paimon_schema = self._table.table_schema
        self._pa_schema = PaimonDataSource.paimon_to_pa_schema(paimon_schema)
        self._schema = PaimonDataSource.paimon_to_daft_schema(paimon_schema)

        self._partition_keys = self._paimon_partition_keys_to_daft_partition_fields(
            paimon_schema, self._table.partition_keys
        )
        self._paimon_predicate_visitor = PaimonPredicateVisitor(self._table.new_read_builder())

    @property
    def name(self) -> str:
        return "PaimonDataSource"

    @property
    def schema(self) -> Schema:
        return self._schema

    def get_partition_fields(self) -> list[PartitionField]:
        return self._partition_keys

    def get_tasks(self, pushdowns: Pushdowns) -> Iterator[DataSourceTask]:
        reader_builder = self._table.new_read_builder()
        if pushdowns.partition_filters is not None:
            predicate = self._paimon_predicate_visitor.visit(pushdowns.partition_filters)
            reader_builder = reader_builder.with_filter(predicate)
        table_scan = reader_builder.new_scan()
        table_read = reader_builder.new_read()
        splits = table_scan.plan().splits()

        source_tasks = []
        for split in splits:
            source_tasks.append(PaimonDataSourceTask(table_read, split, self._pa_schema, self._schema))
        return iter(source_tasks)

    @staticmethod
    def paimon_to_daft_schema(paimon_schema: TableSchema) -> Schema:
        pa_schema = pa.schema([PyarrowFieldParser.from_paimon_field(field) for field in paimon_schema.fields])
        return Schema.from_pyarrow_schema(pa_schema)

    @staticmethod
    def paimon_to_pa_schema(paimon_schema: TableSchema) -> pa.Schema:
        return pa.schema([PyarrowFieldParser.from_paimon_field(field) for field in paimon_schema.fields])

    @staticmethod
    def _paimon_partition_keys_to_daft_partition_fields(
        paimon_schema: TableSchema, partition_keys: list[str]
    ) -> list[PartitionField]:
        daft_schema = PaimonDataSource.paimon_to_daft_schema(paimon_schema)
        fields_map = {}
        for field in daft_schema:
            fields_map[field.name] = field
        partition_fields = []
        for key_name in partition_keys:
            daft_field = fields_map[key_name]
            partition_fields.append(
                PartitionField.create(
                    field=daft_field,
                    source_field=daft_field,
                    transform=PartitionTransform.identity(),
                )
            )
        return partition_fields


class PaimonDataSourceTask(DataSourceTask):
    def __init__(self, table_read: TableRead, split: Split, pa_shema: pa.Schema, daft_schema: Schema) -> None:
        super().__init__()
        self._table_read = table_read
        self._split = split
        self._pa_schema = pa_shema
        self._schema = daft_schema

    @property
    def schema(self) -> Schema:
        return self._schema

    def get_micro_partitions(self) -> Iterator[MicroPartition]:
        def data_generator() -> Iterator[MicroPartition]:
            batch_reader = self._table_read.to_arrow_batch_reader([self._split])
            for batch in batch_reader:
                if batch.num_rows == 0:
                    continue
                yield MicroPartition.from_arrow_record_batches([batch], self._pa_schema)

        return data_generator()
