from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Iterator

    from pypaimon.read.split import Split
    from pypaimon.read.table_read import TableRead
    from pypaimon.schema.table_schema import TableSchema
    from pypaimon.table.file_store_table import FileStoreTable

import logging
from functools import reduce

from pypaimon.schema.data_types import PyarrowFieldParser

from daft.daft import (
    PyPartitionField,
    PyPartitionTransform,
    PyPushdowns,
    PyRecordBatch,
    ScanTask,
    StorageConfig,
)
from daft.dependencies import pa
from daft.io.paimon.paimon_predicate_visitor import PaimonPredicateVisitor
from daft.io.scan import ScanOperator, make_partition_field
from daft.logical.schema import Schema
from daft.recordbatch import RecordBatch

logger = logging.getLogger(__name__)


def paimon_data_reader(table_read: TableRead, split: Split, pa_shema: pa.Schema) -> Iterator[PyRecordBatch]:
    """返回 RecordBatch 迭代器.

    Args:
        table_read
        split: paimon Split
        pa_shema

    Returns:
        Iterator[PyRecordBatch]
    """

    def data_generator() -> Iterator[PyRecordBatch]:
        batch_reader = table_read.to_arrow_batch_reader([split])
        for batch in batch_reader:
            if batch.num_rows == 0:
                continue
            yield RecordBatch.from_arrow_record_batches([batch], pa_shema)._recordbatch

    return data_generator()


class PaimonScanOperator(ScanOperator):
    def __init__(self, paimon_table: FileStoreTable, storage_config: StorageConfig) -> None:
        super().__init__()
        self._table = paimon_table
        self._storage_config = storage_config

        paimon_schema = self._table.table_schema
        self._pa_schema = PaimonScanOperator.paimon_to_pa_schema(paimon_schema)
        self._schema = PaimonScanOperator.paimon_to_daft_schema(paimon_schema)

        self._partition_keys = self._paimon_partition_keys_to_daft_partition_fields(
            paimon_schema, self._table.partition_keys
        )
        self._paimon_predicate_visitor = PaimonPredicateVisitor(self._table.new_read_builder())

    def schema(self) -> Schema:
        return self._schema

    def name(self) -> str:
        return "PaimonScanOperator"

    def display_name(self) -> str:
        return f"PaimonScanOperator({self._table.identifier.get_full_name()})"

    def partitioning_keys(self) -> list[PyPartitionField]:
        return self._partition_keys

    def multiline_display(self) -> list[str]:
        return [
            f"Name = {self.display_name()}",
            f"Schema = {self._schema}",
            f"Partitioning keys = {self.partitioning_keys}",
            f"Storage config = {self._storage_config}",
        ]

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
    ) -> list[PyPartitionField]:
        daft_schema = PaimonScanOperator.paimon_to_daft_schema(paimon_schema)
        fields_map = {}
        for field in daft_schema:
            fields_map[field.name] = field
        partition_fields = []
        for key_name in partition_keys:
            daft_field = fields_map[key_name]
            partition_fields.append(
                make_partition_field(
                    field=daft_field,
                    source_field=daft_field,
                    transform=PyPartitionTransform.identity(),
                )
            )
        return partition_fields

    def to_scan_tasks(self, pushdowns: PyPushdowns) -> Iterator[ScanTask]:
        yield from self._create_paimon_scan_task(pushdowns)

    def _create_paimon_scan_task(self, pushdowns: PyPushdowns) -> Iterator[ScanTask]:
        reader_builder = self._table.new_read_builder()
        if pushdowns.partition_filters is not None:
            predicate = pushdowns.partition_filters.accept(self._paimon_predicate_visitor)
            reader_builder = reader_builder.with_filter(predicate)
        table_scan = reader_builder.new_scan()
        table_read = reader_builder.new_read()
        splits = table_scan.plan().splits()

        scan_tasks = []
        for split in splits:
            total_size = reduce(lambda x, y: x + y, [file.file_size for file in split.files])
            st = ScanTask.python_factory_func_scan_task(
                module=paimon_data_reader.__module__,
                func_name=paimon_data_reader.__name__,
                func_args=(table_read, split, self._pa_schema),
                schema=self.schema()._schema,
                num_rows=split.row_count,
                size_bytes=total_size,
                pushdowns=pushdowns,
                stats=None,
            )

            if st is not None:
                scan_tasks.append(st)
        return iter(scan_tasks)

    def supports_count_pushdown(self) -> bool:
        return False

    def can_absorb_filter(self) -> bool:
        return False

    def can_absorb_limit(self) -> bool:
        return False

    def can_absorb_select(self) -> bool:
        return False
