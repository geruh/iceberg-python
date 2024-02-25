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

from datetime import datetime
from typing import Any, Dict, List, Literal, Optional
from uuid import UUID, uuid4

from pydantic import Field, model_validator

from pyiceberg.schema import Schema
from pyiceberg.table import Namespace
from pyiceberg.typedef import IcebergBaseModel, IcebergRootModel
from pyiceberg.utils.datetime import datetime_to_millis


def cleanup_version_id(data: Dict[str, Any]) -> Dict[str, Any]:
    """Run before validation."""
    if "current-version-id" in data and data["current-version-id"] == -1:
        data["current-version-id"] = None
    return data


class SQLViewRepresentation(IcebergBaseModel):
    type: str = Field(default="sql")

    sql: str = Field()
    """The view query SQL text"""

    dialect: str = Field()
    """The view query SQL dialect"""


class ViewVersion(IcebergBaseModel):
    version_id: int = Field(alias="version-id")
    """ID of the view version"""

    timestamp_millis: int = Field(alias="timestamp-ms", default_factory=lambda: datetime_to_millis(datetime.now().astimezone()))
    """Timestamp in milliseconds from the unix epoch when the view version was created"""

    summary: Dict[str, str] = Field(default_factory=dict)
    """The version summary such as the name of the operation that created that version of the view"""

    representations: List[SQLViewRepresentation] = Field(default_factory=list)
    """List of the representations for this view version"""

    schema_id: int = Field(alias="schema-id")
    """ID of the table’s current schema."""

    default_namespace: Namespace = Field(alias="default-namespace")
    """The default namespace when the view version was created"""

    default_catalog: Optional[str] = Field(alias="default-catalog", default=None)
    """The default catalog when the view version was created"""


class ViewHistoryEntry(IcebergBaseModel):
    timestamp_millis: int = Field(alias="timestamp-ms")
    """Timestamp in milliseconds from the unix epoch when the view was last updated"""

    version_id: int = Field(alias="version-id")
    """id of the new current view version"""


class ViewMetadata(IcebergBaseModel):
    location: str = Field()
    """The view’s base location. This is used by writers to determine where
    to store data files, manifest files, and table metadata files."""

    view_uuid: UUID = Field(alias="view-uuid", default_factory=uuid4)
    """A UUID that identifies the view, generated when the view is created.
    Implementations must throw an exception if a table’s view does not match
    the expected UUID after refreshing metadata."""

    format_version: Literal[1] = Field(alias="format-version", default=1)
    """An integer version number for the format. Currently, we only have one version."""

    schemas: List[Schema] = Field(default_factory=list)
    """A list of schemas, stored as objects with schema-id."""

    current_version_id: int = Field(alias="current-version-id", default=0)
    """ID of the table’s current view version."""

    versions: List[ViewVersion] = Field(default_factory=list)
    """A list of view versions, stored as full ViewVersion objects."""

    history: List[ViewHistoryEntry] = Field(alias="version-log", default_factory=list)
    """ The version history of this table."""

    properties: Dict[str, str] = Field(default_factory=dict)
    """A string to string map of view properties."""

    @model_validator(mode="before")
    def cleanup_version_id(cls, data: Dict[str, Any]) -> Dict[str, Any]:
        return cleanup_version_id(data)

    @property
    def versions_by_id(self) -> Dict[int, ViewVersion]:
        return {version.version_id: version for version in self.versions}

    @property
    def schemas_by_id(self) -> Dict[int, Schema]:
        return {schema.schema_id: schema for schema in self.schemas}


class ViewMetadataWrapper(IcebergRootModel[ViewMetadata]):
    root: ViewMetadata
