"""Schema evolution framework for versioned CDC payload parsing and migrations."""

from src.schema_evolution.migrations import migrate_subscriptions_to_v2
from src.schema_evolution.registry import (
    PAYLOAD_SCHEMA_REGISTRY,
    SUBSCRIPTIONS_V1_SCHEMA,
    SUBSCRIPTIONS_V2_SCHEMA,
    get_payload_schema,
    is_supported_schema_version,
)

__all__ = [
    "PAYLOAD_SCHEMA_REGISTRY",
    "SUBSCRIPTIONS_V1_SCHEMA",
    "SUBSCRIPTIONS_V2_SCHEMA",
    "get_payload_schema",
    "is_supported_schema_version",
    "migrate_subscriptions_to_v2",
]
