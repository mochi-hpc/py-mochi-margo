# (C) 2022 The University of Chicago
# See COPYRIGHT in top-level directory.

import pickle
from typing import Any
from .typing import hg_addr_t, hg_bulk_t, margo_instance_id
from .bulk import Bulk


def loads(mid: margo_instance_id, raw_data: bytes) -> Any:
    """
    This loads function wraps pickle.loads and sets
    the current mid in Bulk so that Bulk objects can
    be properly deserialized.
    """
    Bulk._current_mid = mid
    result = pickle.loads(raw_data)
    Bulk._current_mid = None
    return result


def dumps(mid: margo_instance_id, data: Any) -> bytes:
    """
    This dumps function wraps pickle.dumps and
    sets the current mid in Bulk so that Bulk objects
    can be properly serialized.
    """
    Bulk._current_mid = mid
    result = pickle.dumps(data)
    Bulk._current_mid = None
    return result
