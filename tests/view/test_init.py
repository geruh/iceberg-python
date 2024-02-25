from datetime import datetime

from pyiceberg.schema import Schema
from pyiceberg.types import DateType, IntegerType, NestedField
from pyiceberg.view import View, ViewVersion
from pyiceberg.view.metadata import SQLViewRepresentation, ViewMetadata


def test_schema(view: View) -> None:
    assert view.schema() == Schema(
        NestedField(field_id=1, name="event_count", field_type=IntegerType(), required=False, doc="Count of events"),
        NestedField(field_id=2, name="event_date", field_type=DateType(), required=False),
        schema_id=1,
        identifier_field_ids=[],
    )


def test_schemas(view: View) -> None:
    assert view.schemas() == {
        1: Schema(
            NestedField(field_id=1, name="event_count", field_type=IntegerType(), required=False, doc="Count of events"),
            NestedField(field_id=2, name="event_date", field_type=DateType(), required=False),
            schema_id=1,
            identifier_field_ids=[],
        )
    }


def test_location(view: View) -> None:
    assert view.location() == "s3://warehouse/default.db/event_agg"


def test_metadata():
    print(ViewMetadata())


def _create_view_version(version_id: int, schema_id: int, sql: str) -> ViewVersion:
    representation = SQLViewRepresentation(dialect="spark", sql=sql)
    return ViewVersion(
        version_id=version_id,
        timestamp_millis=datetime.now(),
        default_catalog="prod",
        default_namespace="default",
        summary={"user": "some-user"},
        representations=[representation],
        schema_id=schema_id,
    )
