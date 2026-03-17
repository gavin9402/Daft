from __future__ import annotations

from typing import TYPE_CHECKING

from daft import context, runners
from daft.api_annotations import PublicAPI
from daft.daft import IOConfig, ScanOperatorHandle, StorageConfig
from daft.dataframe import DataFrame
from daft.logical.builder import LogicalPlanBuilder

if TYPE_CHECKING:
    from pypaimon.table.file_store_table import FileStoreTable


@PublicAPI
def read_paimon(
    table: str | FileStoreTable,
    io_config: IOConfig | None = None,
) -> DataFrame:
    from daft.io.paimon.paimon_scan import PaimonScanOperator

    if isinstance(table, str):
        raise NotImplementedError(
            "Reading Paimon table from path is not yet supported. Please provide a Paimon table object."
        )

    io_config = io_config or context.get_context().daft_planning_config.default_io_config

    multithreaded_io = runners.get_or_create_runner().name != "ray"
    storage_config = StorageConfig(multithreaded_io, io_config)

    paimon_operator = PaimonScanOperator(table, storage_config=storage_config)

    handle = ScanOperatorHandle.from_python_scan_operator(paimon_operator)
    builder = LogicalPlanBuilder.from_tabular_scan(scan_operator=handle)
    return DataFrame(builder)
