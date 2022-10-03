# (C) 2022 The University of Chicago
# See COPYRIGHT in top-level directory.


def __get_capsule_type():
    import ctypes

    class PyTypeObject(ctypes.Structure):
        pass  # don't need to define the full structure
    capsuletype = PyTypeObject.in_dll(ctypes.pythonapi, "PyCapsule_Type")
    capsuletypepointer = ctypes.pointer(capsuletype)
    return ctypes.py_object.from_address(
        ctypes.addressof(capsuletypepointer)).value


PyCapsule = __get_capsule_type()


margo_instance_id = PyCapsule
hg_addr_t = PyCapsule
hg_bulk_t = PyCapsule
margo_request = PyCapsule
