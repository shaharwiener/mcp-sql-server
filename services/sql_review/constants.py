"""Constants for SQL Review services.

This module centralizes all hardcoded values used across SQL Review services
to improve maintainability and consistency.
"""

# Default timeouts (in seconds)
DEFAULT_QUERY_TIMEOUT = 30
DEFAULT_CONNECTION_TIMEOUT = 30
DEFAULT_COMMAND_TIMEOUT = 300

# Default limits
DEFAULT_MAX_ROWS = 100
DEFAULT_SAMPLE_SIZE = 50
DEFAULT_TIME_WINDOW_MINUTES = 1440  # 24 hours

# System databases
SYSTEM_DATABASES = ["master", "msdb", "model", "tempdb"]
DEFAULT_SYSTEM_DB = "master"

# Query validation
MAX_QUERY_LENGTH = 100000  # 100KB

# Event class IDs for trace events
SECURITY_EVENT_CLASSES = [
    102, 103, 104, 105, 106, 108, 109, 110, 111, 130, 135
]

SCHEMA_CHANGE_EVENT_CLASSES = [
    46,   # Object:Created
    47,   # Object:Deleted
    164,  # Object:Altered
    53, 54, 55, 56  # Additional DDL events
]

# Object type mappings
OBJECT_TYPE_MAP = {
    'U': 'table',
    'V': 'view',
    'P': 'procedure',
    'FN': 'function',
    'IF': 'function',
    'TF': 'function',
    'TR': 'trigger'
}

# Valid object types for search
VALID_OBJECT_TYPES = ['table', 'view', 'procedure', 'function', 'trigger']

# Job run status codes
JOB_STATUS_MAP = {
    0: 'Failed',
    1: 'Succeeded',
    2: 'Retry',
    3: 'Canceled'
}
