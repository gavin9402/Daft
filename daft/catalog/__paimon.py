"""WARNING! These APIs are internal; please use Catalog.from_paimon() and Table.from_paimon()."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    import pyarrow as pa

from pypaimon.catalog.catalog import Catalog as InnerCatalog
from pypaimon.catalog.catalog_exception import DatabaseNotExistException, TableNotExistException
from pypaimon.common.options import ConfigOption
from pypaimon.common.options.core_options import CoreOptions
from pypaimon.schema.data_types import DataField, PyarrowFieldParser
from pypaimon.table.file_store_table import FileStoreTable as InnerTable

import daft
from daft import read_paimon
from daft.catalog import Catalog, Identifier, NotFoundError, Properties, Schema, Table

if TYPE_CHECKING:
    from daft.dataframe import DataFrame
    from daft.io.partitioning import PartitionField


PAIMON_OPTION_PREFIX = "daft.paimon."


def context_paimon_options(options: dict[str, Any | None] | None = None) -> dict[str, Any | None]:
    paimon_options_dict: dict[str, Any | None] = {}
    extra_options = getattr(daft.context.get_context(), "extra_options", {})
    for k, v in extra_options.items():
        if k.startswith(PAIMON_OPTION_PREFIX):
            paimon_options_dict[k[len(PAIMON_OPTION_PREFIX) :]] = v
    if options is not None:
        paimon_options_dict.update(options)
    return paimon_options_dict


class PaimonCatalog(Catalog):
    _inner: InnerCatalog

    def __init__(self) -> None:
        raise RuntimeError("PaimonCatalog.__init__ is not supported, please use `Catalog.from_paimon` instead.")

    @staticmethod
    def _from_obj(obj: object) -> PaimonCatalog:
        """Returns an PaimonCatalog instance if the given object can be adapted so."""
        if isinstance(obj, InnerCatalog):
            c = PaimonCatalog.__new__(PaimonCatalog)
            c._inner = obj
            return c
        raise ValueError(f"Unsupported paimon catalog type: {type(obj)}")

    @property
    def name(self) -> str:
        return self._inner.name

    ###
    # create_*
    ###

    def _create_namespace(self, identifier: Identifier) -> None:
        ident = _to_pypaimon_ident(identifier)
        self._inner.create_namespace(ident)

    def _create_table(
        self,
        identifier: Identifier,
        schema: Schema,
        properties: Properties | None = None,
        partition_fields: list[PartitionField] | None = None,
    ) -> Table:
        raise NotImplementedError

    ###
    # drop_*
    ###

    def _drop_namespace(self, identifier: Identifier) -> None:
        pass

    def _drop_table(self, identifier: Identifier) -> None:
        pass

    ###
    # has_*
    ###

    def _has_namespace(self, identifier: Identifier) -> bool:
        ident = _to_pypaimon_ident(identifier)
        try:
            _ = self._inner.get_database(ident)
            return True
        except DatabaseNotExistException:
            return False

    def _has_table(self, identifier: Identifier) -> bool:
        ident = _to_pypaimon_ident(identifier)
        try:
            self._inner.get_table(ident)
            return True
        except TableNotExistException:
            return False

    ###
    # get_*
    ###

    def _get_table(self, identifier: Identifier) -> PaimonTable:
        ident = _to_pypaimon_ident(identifier)
        try:
            tbl = self._inner.get_table(ident)
            return PaimonTable._from_obj(tbl)
        except TableNotExistException as ex:
            # convert to not found because we want to (sometimes) ignore it internally
            raise NotFoundError() from ex
        except Exception as ex:
            # wrap original exceptions
            raise Exception("pypaimon raised an exception while calling get_table") from ex

    ###
    # list_*
    ###

    def _list_namespaces(self, pattern: str | None = None) -> list[Identifier]:
        raise NotImplementedError

    def _list_tables(self, pattern: str | None = None) -> list[Identifier]:
        raise NotImplementedError


class PaimonTable(Table):
    _inner: InnerTable
    _schema: Schema

    _read_options = set()
    for key, value in CoreOptions.__dict__.items():
        if isinstance(value, ConfigOption):
            _read_options.add(value.key())

    _write_options: set[str] = set()

    def __init__(self) -> None:
        raise RuntimeError("paimonTable.__init__ is not supported, please use `Table.from_paimon` instead.")

    @property
    def name(self) -> str:
        return self._inner.name()[-1]

    @staticmethod
    def to_pyarrow_schema(data_fields: list[DataField]) -> pa.Schema:
        import pyarrow

        pa_fields = []
        for field in data_fields:
            pa_fields.append(PyarrowFieldParser.from_paimon_field(field))
        return pyarrow.schema(pa_fields)

    def schema(self) -> Schema:
        return self._schema

    @staticmethod
    def _from_obj(obj: object) -> PaimonTable:
        """Returns an paimonTable if the given object can be adapted so."""
        if isinstance(obj, InnerTable):
            t = PaimonTable.__new__(PaimonTable)
            t._inner = obj
            t._schema = Schema.from_pyarrow_schema(PaimonTable.to_pyarrow_schema(obj.fields))
            return t
        raise ValueError(f"Unsupported paimon table type: {type(obj)}")

    def read(self, **options: Any | None) -> DataFrame:
        core_options = context_paimon_options(options)
        Table._validate_options("paimon read", options, PaimonTable._read_options)
        if len(core_options) > 0:
            self._inner = self._inner.copy(core_options)
        return read_paimon(self._inner)

    def append(self, df: DataFrame, **options: Any) -> None:
        pass

    def overwrite(self, df: DataFrame, **options: Any) -> None:
        pass


def _to_pypaimon_ident(ident: Identifier | str) -> tuple[str, ...] | str:
    return ".".join(tuple(ident)) if isinstance(ident, Identifier) else ident
