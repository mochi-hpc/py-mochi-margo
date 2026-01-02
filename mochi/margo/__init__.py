# (C) 2018 The University of Chicago
# See COPYRIGHT in top-level directory.
from . import core

client = core.client
server = core.server

MargoAddress = core.Address
MargoHandle = core.Handle
MargoInstance = core.Engine
Engine = core.Engine
Address = core.Address
Handle = core.Handle

remote = core.remote
provider = core.provider
on_finalize = core.on_finalize
on_prefinalize = core.on_prefinalize
