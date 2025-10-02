from pathlib import Path

DEFAULT_ENTITY = "refugee_displacement_conformed"
DEFAULT_GRAIN = "year"


def route_entities(file_path: Path, source_id: str):
    """
    Stub router: always returns the default entity + grain.
    Later you can make this config-driven (YAML/DB regex-based).

    Args:
        file_path: Path object pointing to the Bronze file.
        source_id: Source identifier, e.g., "unhcr".

    Returns:
        List of dicts with entity and grain.
    """
    return [
        {
            "entity": DEFAULT_ENTITY,
            "entity_grain": DEFAULT_GRAIN,
        }
    ]
