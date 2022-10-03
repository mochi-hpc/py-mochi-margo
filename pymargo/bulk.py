import _pymargo
from typing import TYPE_CHECKING, Optional
from .typing import hg_bulk_t, margo_instance_id

if TYPE_CHECKING:
    from .core import Engine  # noqa: F401

"""
Tags to indicate what type of operations are expected from a Bulk handle.
"""
read_only = _pymargo.access.read_only
write_only = _pymargo.access.write_only
read_write = _pymargo.access.read_write

"""
Types of operations to perform on Bulk handles.
"""
push = _pymargo.xfer.push
pull = _pymargo.xfer.pull


class Bulk:
    """
    The Bulk class wraps hg_bulk_t objects at C++ level and
    enable serialization/deserialization to send them in RPCs.
    """

    _current_mid: Optional[margo_instance_id] = None

    def __init__(self, hg_bulk: hg_bulk_t):
        """
        Constructor. This method is not supposed to be called by users.
        Users must call Engine.create_bulk instead.
        hg_bulk : native hg_bulk_t handle.
        """
        self._hg_bulk = hg_bulk

    def __del__(self) -> None:
        """
        Destructor. Frees the underlying hg_bulk_t handle.
        """
        if hasattr(self, '_hg_bulk'):
            _pymargo.bulk_free(self._hg_bulk)

    def __getstate__(self):
        return _pymargo.bulk_to_str(self._hg_bulk, False)

    def __setstate__(self, state: bytes):
        if not hasattr(Bulk, '_current_mid'):
            raise RuntimeError(
                "Could not reconstruct Bulk object: no Bulk._current_mid set")
        if Bulk._current_mid is None:
            raise RuntimeError(
                "Could not reconstruct Bulk object: no Bulk._current_mid set")
        self._hg_bulk = _pymargo.str_to_bulk(Bulk._current_mid, state)
