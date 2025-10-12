"""
Generate human-readable batch IDs for import tracking.

A batch ID should be:
1. Human-readable and descriptive
2. Unique (no collisions)
3. Sortable by time
4. Easy to reference in commands

Format: {database}_{collection}_{timestamp}_{suffix}
Example: mydb_users_20250113_143025_a3f
"""

import getpass
import socket
from datetime import datetime, timezone


def generate_batch_id(database: str, collection: str) -> str:
    """
    Generate a human-readable batch ID for an import operation.

    Format: {database}_{collection}_{timestamp}_{suffix}

    Args:
        database: Database name
        collection: Collection name

    Returns:
        A batch ID string like "mydb_users_20250113_143025_a3f"
    """
    # Timestamp: YYYYMMDD_HHMMSS in UTC
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")

    # Short suffix for uniqueness (first 3 chars of username + pid last 3 digits)
    # This prevents collisions if multiple imports start in the same second
    import os
    username = getpass.getuser()[:3].lower()
    pid_suffix = str(os.getpid())[-3:]
    suffix = f"{username}{pid_suffix}"

    # Clean database and collection names (replace problematic chars)
    clean_db = database.replace("-", "_").replace(".", "_")
    clean_col = collection.replace("-", "_").replace(".", "_")

    # Combine: database_collection_timestamp_suffix
    batch_id = f"{clean_db}_{clean_col}_{timestamp}_{suffix}"

    return batch_id


def generate_short_batch_id(database: str = None, collection: str = None) -> str:
    """
    Generate a shorter batch ID when database/collection names are long.

    Format: {timestamp}_{user}_{host}

    Args:
        database: Database name (optional, not used in short format)
        collection: Collection name (optional, not used in short format)

    Returns:
        A batch ID string like "20250113_143025_jdr_laptop"
    """
    # Timestamp: YYYYMMDD_HHMMSS in UTC
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")

    # User (first 3 chars)
    username = getpass.getuser()[:3].lower()

    # Hostname (first part before dot, max 8 chars)
    hostname = socket.gethostname().split('.')[0][:8].lower()

    batch_id = f"{timestamp}_{username}_{hostname}"

    return batch_id


def generate_simple_batch_id() -> str:
    """
    Generate the simplest batch ID - just timestamp with counter.

    Format: {timestamp}_{counter}

    Returns:
        A batch ID string like "20250113_143025_001"
    """
    import threading

    # Use a class variable to track counter per second
    if not hasattr(generate_simple_batch_id, '_lock'):
        generate_simple_batch_id._lock = threading.Lock()
        generate_simple_batch_id._last_timestamp = ""
        generate_simple_batch_id._counter = 0

    with generate_simple_batch_id._lock:
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")

        if timestamp == generate_simple_batch_id._last_timestamp:
            generate_simple_batch_id._counter += 1
        else:
            generate_simple_batch_id._last_timestamp = timestamp
            generate_simple_batch_id._counter = 0

        counter = str(generate_simple_batch_id._counter).zfill(3)

    return f"{timestamp}_{counter}"


def generate_suffix_only_batch_id() -> str:
    """
    Generate batch ID using just the unique suffix (username + pid).

    Format: {user}{pid}
    Example: jdr927

    Returns:
        A short batch ID string like "jdr927"
    """
    import os
    username = getpass.getuser()[:3].lower()
    pid_suffix = str(os.getpid())[-3:]
    return f"{username}{pid_suffix}"


def generate_counter_only_batch_id() -> int:
    """
    Generate the simplest possible batch ID - just a counter.

    Format: sequential integer

    Returns:
        An integer like 1, 2, 3, etc.
    """
    import threading

    if not hasattr(generate_counter_only_batch_id, '_lock'):
        generate_counter_only_batch_id._lock = threading.Lock()
        generate_counter_only_batch_id._counter = 0

    with generate_counter_only_batch_id._lock:
        generate_counter_only_batch_id._counter += 1
        return generate_counter_only_batch_id._counter


# Aliases for convenience
create_batch_id = generate_batch_id
create_simple_batch_id = generate_simple_batch_id
create_counter_batch_id = generate_counter_only_batch_id
