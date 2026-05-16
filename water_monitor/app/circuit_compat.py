"""
Circuit alias resolution — single source of truth for legacy circuit name mapping.

Apply resolve_circuit() in: all route handlers, backup restore, history filters,
event processing, cluster code. Never resolves from display names — only from
stable IDs and aliases.

Do NOT add input_number.* entities to any safety-critical firmware write path.
"""
from __future__ import annotations

import re

# Hard-coded legacy aliases.  These are the only values ever stored in the old DB.
# Extend this dict if additional aliases are needed in future; do NOT derive aliases
# from user-supplied display names.
_ALIASES: dict[str, str] = {
    "main":       "circuit_1",
    "irrigation": "circuit_2",
    "valve_1":    "circuit_1",
    "valve_2":    "circuit_2",
}

# Validation pattern for user-supplied display names
_LABEL_RE = re.compile(r"^[\w\s\-']+$")


def resolve_circuit(value: str) -> str:
    """
    Map a legacy or alias circuit name to its stable circuit ID.

    Returns value unchanged if it is already a stable ID or unrecognised.

    Examples:
        resolve_circuit("main")       -> "circuit_1"
        resolve_circuit("irrigation") -> "circuit_2"
        resolve_circuit("circuit_1")  -> "circuit_1"
        resolve_circuit("unknown")    -> "unknown"
    """
    return _ALIASES.get(value, value)


def validate_display_name(name: str) -> str:
    """
    Validate and return a stripped display name.

    Raises ValueError if the name is empty, too long, or contains
    characters outside letters, digits, spaces, hyphens, underscores,
    or apostrophes.
    """
    name = name.strip()
    if not name:
        raise ValueError("Display name cannot be empty")
    if len(name) > 40:
        raise ValueError("Display name cannot exceed 40 characters")
    if not _LABEL_RE.match(name):
        raise ValueError(
            "Display name may only contain letters, numbers, spaces, "
            "hyphens, underscores, and apostrophes"
        )
    return name
