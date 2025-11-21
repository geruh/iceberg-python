# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
from __future__ import annotations

import binascii
from base64 import b64decode
from collections.abc import Callable
from enum import Enum
from typing import Any, Mapping, Sequence

from pydantic import Field, field_validator

from pyiceberg.expressions import BooleanExpression
from pyiceberg.manifest import DEFAULT_READ_VERSION, DataFile, DataFileContent, FileFormat, TableVersion
from pyiceberg.partitioning import PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.table import FileScanTask
from pyiceberg.typedef import IcebergBaseModel, Record


class PlanStatus(str, Enum):
    COMPLETED = "completed"
    SUBMITTED = "submitted"
    CANCELLED = "cancelled"
    FAILED = "failed"


def _parse_key_value_map(value: Any, converter: Callable[[Any], Any] | None = None) -> dict[int, Any] | None:
    """
    Parse Iceberg map format of seperated key-value pairs.
    format {"keys": [1,2], "values": [a,b]} to {1: a, 2: b}.
    """
    if value is None:
        return None
    if isinstance(value, dict):
        if "keys" in value and "values" in value:
            keys = value["keys"]
            values = value["values"]
            if len(keys) != len(values):
                raise ValueError(f"keys and values must have same length: {len(keys)} != {len(values)}")
            converter_fn = converter or (lambda x: x)
            return {int(k): converter_fn(v) for k, v in zip(keys, values, strict=True) if v is not None}
        # Already in dict format
        return {int(k): (converter(v) if converter else v) for k, v in value.items() if v is not None}
    return None


def _decode_hex(value: str) -> bytes:
    return bytes.fromhex(value)


def _decode_key_metadata(value: str | None) -> bytes | None:
    if value is None:
        return None
    try:
        return b64decode(value)
    except binascii.Error:
        return value.encode()


def _content_from_str(value: str) -> DataFileContent:
    snake_case = value.replace("-", "_").upper()
    try:
        return DataFileContent[snake_case]
    except KeyError as exc:
        raise ValueError(f"Unknown content value: {value}") from exc


class RestContentFile(IcebergBaseModel):
    spec_id: int = Field(alias="spec-id")
    content: str = Field(alias="content")
    file_path: str = Field(alias="file-path")
    file_format: FileFormat = Field(alias="file-format")
    partition: dict[str, Any] | None = Field(default=None)
    file_size_in_bytes: int = Field(alias="file-size-in-bytes")
    record_count: int = Field(alias="record-count")
    column_sizes: dict[int, int] | None = Field(alias="column-sizes", default=None)
    value_counts: dict[int, int] | None = Field(alias="value-counts", default=None)
    null_value_counts: dict[int, int] | None = Field(alias="null-value-counts", default=None)
    nan_value_counts: dict[int, int] | None = Field(alias="nan-value-counts", default=None)
    lower_bounds: dict[int, bytes] | None = Field(alias="lower-bounds", default=None)
    upper_bounds: dict[int, bytes] | None = Field(alias="upper-bounds", default=None)
    key_metadata: bytes | None = Field(alias="key-metadata", default=None)
    split_offsets: list[int] | None = Field(alias="split-offsets", default=None)
    equality_ids: list[int] | None = Field(alias="equality-ids", default=None)
    sort_order_id: int | None = Field(alias="sort-order-id", default=None)
    first_row_id: int | None = Field(alias="first-row-id", default=None)
    referenced_data_file: str | None = Field(alias="referenced-data-file", default=None)
    content_offset: int | None = Field(alias="content-offset", default=None)
    content_size_in_bytes: int | None = Field(alias="content-size-in-bytes", default=None)

    @field_validator("file_format", mode="before")
    @classmethod
    def _upper_file_format(cls, value: Any) -> Any:
        return value.upper() if isinstance(value, str) else value

    @field_validator("column_sizes", "value_counts", "null_value_counts", "nan_value_counts", mode="before")
    @classmethod
    def _parse_int_map(cls, value: Any) -> dict[int, int] | None:
        return _parse_key_value_map(value, int)

    @field_validator("lower_bounds", "upper_bounds", mode="before")
    @classmethod
    def _parse_binary_map(cls, value: Any) -> dict[int, bytes] | None:
        return _parse_key_value_map(value, _decode_hex)

    @field_validator("key_metadata", mode="before")
    @classmethod
    def _parse_key_metadata(cls, value: Any) -> bytes | None:
        return _decode_key_metadata(value) if isinstance(value, str) else value

    def to_data_file(
        self,
        *,
        schema: Schema,
        partition_specs: Mapping[int, PartitionSpec],
        table_format_version: TableVersion = DEFAULT_READ_VERSION,
    ) -> DataFile:
        partition_spec = partition_specs.get(self.spec_id)

        # match partitions find a better way to do this
        if partition_spec is None:
            raise ValueError(f"Unknown partition spec id: {self.spec_id}")

        partition_struct = partition_spec.partition_type(schema)
        partition_kwargs = {}
        if self.partition and partition_spec.fields:
            # Partition is sent as {field_id: value} dict
            values_by_field_id = {int(k): v for k, v in self.partition.items()}
            partition_kwargs = {
                field.name: values_by_field_id.get(field.field_id)
                for field in partition_struct.fields
            }
        partition_record = Record._bind(struct=partition_struct, **partition_kwargs)

        data_file = DataFile.from_args(
            _table_format_version=table_format_version,
            content=_content_from_str(self.content),
            file_path=self.file_path,
            file_format=self.file_format,
            partition=partition_record,
            record_count=self.record_count,
            file_size_in_bytes=self.file_size_in_bytes,
            column_sizes=self.column_sizes,
            value_counts=self.value_counts,
            null_value_counts=self.null_value_counts,
            nan_value_counts=self.nan_value_counts,
            lower_bounds=self.lower_bounds,
            upper_bounds=self.upper_bounds,
            key_metadata=self.key_metadata,
            split_offsets=self.split_offsets,
            equality_ids=self.equality_ids,
            sort_order_id=self.sort_order_id,
            first_row_id=self.first_row_id,
            referenced_data_file=self.referenced_data_file,
            content_offset=self.content_offset,
            content_size_in_bytes=self.content_size_in_bytes,
        )
        data_file.spec_id = partition_spec.spec_id
        return data_file


class RestFileScanTask(IcebergBaseModel):
    data_file: RestContentFile = Field(alias="data-file")
    delete_file_references: list[int] | None = Field(alias="delete-file-references", default=None)
    # TODO: fix boolean expression
    residual_filter: Any | None = Field(alias="residual-filter", default=None)

    def to_file_scan_task(
        self,
        *,
        schema: Schema,
        partition_specs: Mapping[int, PartitionSpec],
        delete_files: Sequence[DataFile],
        table_format_version: TableVersion = DEFAULT_READ_VERSION,
    ) -> FileScanTask:
        data_file = self.data_file.to_data_file(
            schema=schema,
            partition_specs=partition_specs,
            table_format_version=table_format_version,
        )
        # TODO: parse to evaluate that there are no equality deletes
        # Resolve delete file references by index
        delete_file_set = {delete_files[idx] for idx in (self.delete_file_references or [])}
        return FileScanTask(
            data_file=data_file,
            delete_files=delete_file_set,
            residual=self.residual_filter,
        )


class PlanTableScanRequest(IcebergBaseModel):
    snapshot_id: int | None = Field(alias="snapshot-id", default=None)
    # TODO: fix boolean expression
    filter: Any | None = Field(default=None)
    select: list[str] | None = Field(default=None)
    case_sensitive: bool = Field(alias="case-sensitive", default=True)
    stats_fields: list[str] | None = Field(alias="stats-fields", default=None)
    start_snapshot_id: int | None = Field(alias="start-snapshot-id", default=None)
    end_snapshot_id: int | None = Field(alias="end-snapshot-id", default=None)
    use_snapshot_schema: bool | None = Field(alias="use-snapshot-schema", default=None)


class PlanTableScanResponse(IcebergBaseModel):
    plan_id: str | None = Field(alias="plan-id", default=None)
    plan_status: PlanStatus = Field(alias="plan-status")
    plan_tasks: list[str] = Field(alias="plan-tasks", default_factory=list)
    delete_files: list[RestContentFile] = Field(alias="delete-files", default_factory=list)
    file_scan_tasks: list[RestFileScanTask] = Field(alias="file-scan-tasks", default_factory=list)

    def to_file_scan_tasks(
        self,
        *,
        schema: Schema,
        partition_specs: Mapping[int, PartitionSpec],
        table_format_version: TableVersion = DEFAULT_READ_VERSION,
    ) -> list[FileScanTask]:
        if self.plan_status != PlanStatus.COMPLETED:
            raise ValueError(
                f"Cannot build file scan tasks for status {self.plan_status}. "
                f"Only synchronous planning is supported."
            )

        delete_file_list = [
            delete_file.to_data_file(
                schema=schema,
                partition_specs=partition_specs,
                table_format_version=table_format_version,
            )
            for delete_file in self.delete_files
        ]

        return [
            task.to_file_scan_task(
                schema=schema,
                partition_specs=partition_specs,
                delete_files=delete_file_list,
                table_format_version=table_format_version,
            )
            for task in self.file_scan_tasks
        ]


class RestFileScanTasks(IcebergBaseModel):
    plan_status: PlanStatus = Field(alias="plan-status")
    plan_tasks: list[str] = Field(alias="plan-tasks", default_factory=list)
    delete_files: list[RestContentFile] = Field(alias="delete-files", default_factory=list)
    file_scan_tasks: list[RestFileScanTask] = Field(alias="file-scan-tasks", default_factory=list)

    def to_file_scan_tasks(
        self,
        *,
        schema: Schema,
        partition_specs: Mapping[int, PartitionSpec],
        table_format_version: TableVersion = DEFAULT_READ_VERSION,
    ) -> list[FileScanTask]:

        if self.plan_status != PlanStatus.COMPLETED:
            raise ValueError(f"Cannot build file scan tasks for status {self.plan_status}")

        delete_file_list = [
            delete_file.to_data_file(
                schema=schema,
                partition_specs=partition_specs,
                table_format_version=table_format_version,
            )
            for delete_file in self.delete_files
        ]

        return [
            task.to_file_scan_task(
                schema=schema,
                partition_specs=partition_specs,
                delete_files=delete_file_list,
                table_format_version=table_format_version,
            )
            for task in self.file_scan_tasks
        ]
