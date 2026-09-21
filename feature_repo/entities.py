"""Feast entity definitions for FeatureForge."""

from feast import Entity
from feast.value_type import ValueType

user = Entity(
    name="user",
    join_keys=["user_id"],
    value_type=ValueType.STRING,
    description="End user for personalization features.",
)


content = Entity(
    name="content",
    join_keys=["content_id"],
    value_type=ValueType.STRING,
    description="Content item for popularity features.",
)
