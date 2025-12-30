# (C) 2018 The University of Chicago
# See COPYRIGHT in top-level directory.
#
# Compatibility shim for backward compatibility.
# This module re-exports everything from mochi.margo
# so that existing code using "import pymargo" continues to work.

from mochi.margo import *
from mochi.margo import (
    client,
    server,
    MargoAddress,
    MargoHandle,
    MargoInstance,
    remote
)
