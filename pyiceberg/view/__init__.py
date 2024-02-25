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

import uuid
from abc import abstractmethod
from enum import Enum
from functools import singledispatch
from typing import TYPE_CHECKING, Any, Dict, List, Literal, Optional, Tuple

from pydantic import Field, SerializeAsAny

if TYPE_CHECKING:
    from pyiceberg.catalog import Catalog

from pyiceberg.exceptions import CommitFailedException
from pyiceberg.io import FileIO
from pyiceberg.schema import Schema
from pyiceberg.table import TableIdentifier
from pyiceberg.typedef import IcebergBaseModel, Identifier
from pyiceberg.view.metadata import ViewHistoryEntry, ViewMetadata, ViewVersion


class ViewUpdateAction(Enum):
    upgrade_format_version = "upgrade-format-version"
    add_schema = "add-schema"
    set_default_spec = "set-default-spec"
    set_location = "set-location"
    set_properties = "set-properties"
    remove_properties = "remove-properties"


class ViewUpdate(IcebergBaseModel):
    action: ViewUpdateAction


class UpgradeFormatVersionUpdate(ViewUpdate):
    action: ViewUpdateAction = ViewUpdateAction.upgrade_format_version
    format_version: int = Field(alias="format-version")


class AddSchemaUpdate(ViewUpdate):
    action: ViewUpdateAction = ViewUpdateAction.add_schema
    schema_: Schema = Field(alias="schema")
    # This field is required: https://github.com/apache/iceberg/pull/7445
    last_column_id: int = Field(alias="last-column-id")


class SetPropertiesUpdate(ViewUpdate):
    action: ViewUpdateAction = ViewUpdateAction.set_properties
    updates: Dict[str, str]


class RemovePropertiesUpdate(ViewUpdate):
    action: ViewUpdateAction = ViewUpdateAction.remove_properties
    removals: List[str]


class SetLocationUpdate(ViewUpdate):
    action: ViewUpdateAction = ViewUpdateAction.set_location
    location: str


class _ViewMetadataUpdateContext:
    _updates: List[ViewUpdate]

    def __init__(self) -> None:
        self._updates = []

    def add_update(self, update: ViewUpdate) -> None:
        self._updates.append(update)


@singledispatch
def _apply_view_update(update: ViewUpdate, base_metadata: ViewMetadata, context: _ViewMetadataUpdateContext) -> ViewMetadata:
    """Apply a table update to the table metadata.

    Args:
        update: The update to be applied.
        base_metadata: The base metadata to be updated.
        context: Contains previous updates and other change tracking information in the current transaction.

    Returns:
        The updated metadata.

    """
    raise NotImplementedError(f"Unsupported view update: {update}")


@_apply_view_update.register(SetPropertiesUpdate)
def _(update: SetPropertiesUpdate, base_metadata: ViewMetadata, context: _ViewMetadataUpdateContext) -> ViewMetadata:
    if len(update.updates) == 0:
        return base_metadata

    properties = dict(base_metadata.properties)
    properties.update(update.updates)

    context.add_update(update)
    return base_metadata.model_copy(update={"properties": properties})


@_apply_view_update.register(RemovePropertiesUpdate)
def _(update: RemovePropertiesUpdate, base_metadata: ViewMetadata, context: _ViewMetadataUpdateContext) -> ViewMetadata:
    if len(update.removals) == 0:
        return base_metadata

    properties = dict(base_metadata.properties)
    for key in update.removals:
        properties.pop(key)

    context.add_update(update)
    return base_metadata.model_copy(update={"properties": properties})


class ViewRequirement(IcebergBaseModel):
    type: str

    @abstractmethod
    def validate(self, base_metadata: Optional[ViewMetadata]) -> None:
        """Validate the requirement against the base metadata.

        Args:
            base_metadata: The base metadata to be validated against.

        Raises:
            CommitFailedException: When the requirement is not met.
        """
        ...


class AssertTableUUID(ViewRequirement):
    """The table UUID must match the requirement's `uuid`."""

    type: Literal["assert-view-uuid"] = Field(default="assert-view-uuid")
    uuid: uuid.UUID

    def validate(self, base_metadata: Optional[ViewMetadata]) -> None:
        if base_metadata is None:
            raise CommitFailedException("Requirement failed: current table metadata is missing")
        elif self.uuid != base_metadata.table_uuid:
            raise CommitFailedException(f"View UUID does not match: {self.uuid} != {base_metadata.uuid}")


class CommitViewRequest(IcebergBaseModel):
    identifier: TableIdentifier = Field()
    requirements: Tuple[SerializeAsAny[ViewRequirement], ...] = Field(default_factory=tuple)
    updates: Tuple[SerializeAsAny[ViewUpdate], ...] = Field(default_factory=tuple)


class CommitViewResponse(IcebergBaseModel):
    metadata: ViewMetadata
    metadata_location: str = Field(alias="metadata-location")


class View:
    identifier: Identifier = Field()
    metadata: ViewMetadata = Field()
    metadata_location: str = Field()
    io: FileIO
    catalog: Catalog

    def __init__(
        self, identifier: Identifier, metadata: ViewMetadata, metadata_location: str, io: FileIO, catalog: Catalog
    ) -> None:
        self.identifier = identifier
        self.metadata = metadata
        self.metadata_location = metadata_location
        self.io = io
        self.catalog = catalog

    def refresh(self) -> Any:
        """Refresh the current view metadata."""
        fresh = self.catalog.load_view(self.identifier[1:])
        self.metadata = fresh.metadata
        self.io = fresh.io
        self.metadata_location = fresh.metadata_location
        return self

    def name(self) -> Identifier:
        """Return the identifier of this view."""
        return self.identifier

    def schemas(self) -> Dict[int, Schema]:
        """Return a dict of the schema of this view."""
        return {schema.schema_id: schema for schema in self.metadata.schemas}

    def schema(self) -> Schema:
        """Return the schema for this view."""
        return next(schema for schema in self.metadata.schemas if schema.schema_id == self.current_version().schema_id)

    @property
    def properties(self) -> Dict[str, str]:
        """Properties of the view."""
        return self.metadata.properties

    def current_version(self) -> ViewVersion:
        """Return the view's base location."""
        return next(schema for schema in self.metadata.versions if schema.version_id == self.metadata.current_version_id)

    def versions(self) -> Dict[int, ViewVersion]:
        """Return a dict of the view version of this view."""
        return {version.version_id: version for version in self.metadata.versions}

    def version(self, version_id: int) -> ViewVersion:
        return self.metadata.versions[version_id]

    def location(self) -> str:
        """Return the view's base location."""
        return self.metadata.location

    def history(self) -> List[ViewHistoryEntry]:
        """Return the view's base location."""
        return self.metadata.history

    def _do_commit(self, updates: Tuple[ViewUpdate, ...], requirements: Tuple[ViewRequirement, ...]) -> None:
        response = self.catalog._commit_view(  # pylint: disable=W0212
            CommitViewRequest(
                identifier=TableIdentifier(namespace=self.identifier[:-1], name=self.identifier[-1]),
                updates=updates,
                requirements=requirements,
            )
        )  # pylint: disable=W0212
        self.metadata = response.metadata
        self.metadata_location = response.metadata_location

    def __eq__(self, other: Any) -> bool:
        """Return the equality of two instances of the View class."""
        return (
            self.identifier == other.identifier
            and self.metadata == other.metadata
            and self.metadata_location == other.metadata_location
            if isinstance(other, View)
            else False
        )
