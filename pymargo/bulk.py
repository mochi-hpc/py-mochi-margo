import _pymargo
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .core import Engine

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

    def __init__(self, engine: 'Engine', hg_bulk: _pymargo.hg_bulk_t):
        """
        Constructor. This method is not supposed to be called by users.
        Users must call Engine.create_bulk instead.
        engine : Engine from which the Bulk object is created.
        hg_bulk : native hg_bulk_t handle.
        """
        self._engine = engine
        self._hg_bulk = hg_bulk

    def __del__(self) -> None:
        """
        Destructor. Frees the underlying hg_bulk_t handle.
        """
        if hasattr(self, '_hg_bulk'):
            _pymargo.bulk_free(self._hg_bulk)

    def to_base64(self, eager: bool = False) -> str:
        """
        Converts the Bulk handle into a base64 string so it can be sent
        as argument to an RPC.
        eager : if set to True, the data exposed by the Bulk handle will
        be serialized as well.
        """
        return _pymargo.bulk_to_base64(self._hg_bulk, eager)

    def to_bytes(self, eager: bool = False):
        """
        Converts the Bulk handle into a bytes object.
        eager : if set to True, the data exposed by the Bulk handle will
        be serialized as well.
        """
        return _pymargo.bulk_to_str(self._hg_bulk, eager)

    @staticmethod
    def from_base64(engine, bulk_str: str) -> 'Bulk':
        """
        Static method that creates a Bulk from a base64 representation.
        engine : Engine to use for this Bulk handle.
        bulk_str : base64 representation of a Bulk handle.
        """
        blk = _pymargo.base64_to_bulk(engine._mid, bulk_str)
        return Bulk(engine, blk)

    @staticmethod
    def from_bytes(engine, bulk_bytes: bytes) -> 'Bulk':
        """
        Static method that creates a Bulk from a bytes representation.
        engine : Engine to use for this Bulk handle.
        bulk_str : bytes representation of a Bulk handle.
        """
        blk = _pymargo.str_to_bulk(engine._mid, bulk_bytes)
        return Bulk(engine, blk)
