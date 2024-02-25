import io
import json
from datetime import datetime
from typing import Any, Dict
from uuid import UUID

from pyiceberg.schema import Schema
from pyiceberg.serializers import FromByteStream
from pyiceberg.table import Namespace
from pyiceberg.typedef import UTF8
from pyiceberg.types import IntegerType, NestedField
from pyiceberg.view import ViewMetadata, ViewVersion


def test_view_from_dict(example_view_metadata: Dict[str, Any]) -> None:
    ViewMetadata(**example_view_metadata)


def test_from_byte_stream(example_view_metadata: Dict[str, Any]) -> None:
    """Test generating a TableMetadata instance from a file-like byte stream"""
    data = bytes(json.dumps(example_view_metadata), encoding=UTF8)
    byte_stream = io.BytesIO(data)
    FromByteStream.view_metadata(byte_stream=byte_stream)


def test_view_metadata_parsing(example_view_metadata: Dict[str, Any]) -> None:
    """Test retrieving values from a ViewMetadata instance"""
    view_metadata = ViewMetadata(**example_view_metadata)

    assert view_metadata.format_version == 1
    assert view_metadata.view_uuid == UUID("fa6506c3-7681-40c8-86dc-e36561f83385")
    assert view_metadata.location == "s3://warehouse/default.db/event_agg"
    assert view_metadata.current_version_id == 1
    assert len(view_metadata.schemas) == 1
    assert view_metadata.schemas[0].schema_id == 1
    assert view_metadata.schemas[0].fields[0].field_id == 1
    assert view_metadata.schemas[0].fields[0].name == "event_count"
    assert not view_metadata.schemas[0].fields[0].required
    assert view_metadata.schemas[0].fields[0].field_type == IntegerType()
    assert view_metadata.schemas[0].fields[1].name == "event_date"
    assert len(view_metadata.versions) == 1
    assert view_metadata.versions[0].version_id == 1
    assert view_metadata.versions[0].timestamp_millis == 1573518431292
    assert view_metadata.versions[0].schema_id == 1
    assert view_metadata.versions[0].default_catalog == "prod"
    assert view_metadata.versions[0].default_namespace == Namespace(["default"])
    assert view_metadata.versions[0].summary["engine-name"] == "Spark"
    assert view_metadata.versions[0].summary["engineVersion"] == "3.3.2"
    assert len(view_metadata.versions[0].representations) == 1
    assert view_metadata.versions[0].representations[0].type == "sql"
    assert len(view_metadata.history) == 1
    assert view_metadata.history[0].timestamp_millis == 1573518431292
    assert view_metadata.history[0].version_id == 1
    assert view_metadata.properties["comment"] == "Daily event counts"


def test_parsing_correct_types(example_view_metadata: Dict[str, Any]) -> None:
    view_metadata = ViewMetadata(**example_view_metadata)
    assert isinstance(view_metadata.schemas[0], Schema)
    assert isinstance(view_metadata.schemas[0].fields[0], NestedField)
    assert isinstance(view_metadata.schemas[0].fields[0].field_type, IntegerType)


def test_updating_metadata(example_view_metadata: Dict[str, Any]) -> None:
    """Test creating a new ViewMetadata instance that's an updated version of
    an existing ViewMetadata instance"""
    view_metadata = ViewMetadata(**example_view_metadata)

    new_representation = {"dialect": "spark", "sql": "select event_count from events"}

    new_view_version = {
        "version_id": "2",
        "timestamp_millis": int(datetime.now().timestamp() * 1000),
        "default_catalog": "prod",
        "default_namespace": ["default"],
        "summary": {"user": "some-user"},
        "representations": [new_representation],
        "schema_id": 1,
    }

    mutable_view_metadata = view_metadata.model_dump()
    mutable_view_metadata["versions"].append(new_view_version)
    mutable_view_metadata["current-version-id"] = 2

    view_metadata = ViewMetadata(**mutable_view_metadata)

    assert view_metadata.current_version_id == 2
    assert view_metadata.versions[-1] == ViewVersion(**new_view_version)
