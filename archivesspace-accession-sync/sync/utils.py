"""
Shared Utilities

Common utility functions used across multiple modules.
Consolidates shared logic to avoid duplication.
"""

from typing import Any, Optional


def col_letter(col_idx: int) -> str:
    """
    Convert a 1-indexed column number to an Excel column letter.

    Args:
        col_idx: 1-indexed column number (1=A, 26=Z, 27=AA, etc.)

    Returns:
        Column letter string (e.g., "A", "Z", "AA", "AK").

    Examples:
        >>> col_letter(1)
        'A'
        >>> col_letter(26)
        'Z'
        >>> col_letter(27)
        'AA'
        >>> col_letter(53)
        'BA'
    """
    result = ""
    while col_idx > 0:
        col_idx, remainder = divmod(col_idx - 1, 26)
        result = chr(65 + remainder) + result
    return result


def col_index(letter: str) -> int:
    """
    Convert an Excel column letter to a 1-indexed column number.

    Args:
        letter: Column letter string (e.g., "A", "Z", "AA").

    Returns:
        1-indexed column number.

    Examples:
        >>> col_index('A')
        1
        >>> col_index('Z')
        26
        >>> col_index('AA')
        27
    """
    result = 0
    for char in letter.upper():
        result = result * 26 + (ord(char) - 64)
    return result


def validate_config_value(
    value: Any,
    expected_type: type,
    min_val: Optional[float] = None,
    max_val: Optional[float] = None,
    allowed_values: Optional[list] = None,
    field_name: str = "value",
) -> tuple[bool, str]:
    """
    Validate a configuration value against expected constraints.

    Args:
        value: The value to validate.
        expected_type: The expected Python type (int, float, str, bool).
        min_val: Minimum numeric value (for int/float).
        max_val: Maximum numeric value (for int/float).
        allowed_values: List of allowed values (for enums).
        field_name: Name of the field for error messages.

    Returns:
        Tuple of (is_valid, error_message). error_message is empty if valid.
    """
    if value is None:
        return False, f"{field_name}: value is required but not set."

    if not isinstance(value, expected_type):
        # Allow int where float is expected
        if expected_type is float and isinstance(value, int):
            value = float(value)
        else:
            return False, (
                f"{field_name}: expected {expected_type.__name__}, "
                f"got {type(value).__name__}."
            )

    if isinstance(value, (int, float)):
        if min_val is not None and value < min_val:
            return False, f"{field_name}: value {value} is below minimum {min_val}."
        if max_val is not None and value > max_val:
            return False, f"{field_name}: value {value} is above maximum {max_val}."

    if allowed_values is not None and value not in allowed_values:
        return False, (
            f"{field_name}: value '{value}' is not in allowed values: "
            f"{', '.join(str(v) for v in allowed_values)}."
        )

    return True, ""


def parse_time_string(time_str: str) -> tuple[int, int]:
    """
    Parse a 24-hour time string into hours and minutes.

    Args:
        time_str: Time in HH:MM format (e.g., "20:00").

    Returns:
        Tuple of (hour, minute).

    Raises:
        ValueError: If the format is invalid.
    """
    try:
        parts = time_str.strip().split(":")
        if len(parts) != 2:
            raise ValueError(f"Invalid time format: '{time_str}'. Use HH:MM (24-hour).")
        hour = int(parts[0])
        minute = int(parts[1])
        if not (0 <= hour <= 23):
            raise ValueError(f"Hour must be 0-23, got {hour}.")
        if not (0 <= minute <= 59):
            raise ValueError(f"Minute must be 0-59, got {minute}.")
        return hour, minute
    except (ValueError, IndexError) as e:
        raise ValueError(f"Invalid time format: '{time_str}'. Use HH:MM (24-hour). {e}")
