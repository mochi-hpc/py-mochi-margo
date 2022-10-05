from setuptools import setup, Extension
from sysconfig import get_config_vars
import pybind11
import pkgconfig
import os
import os.path
import sys

(opt,) = get_config_vars('OPT')
os.environ['OPT'] = " ".join(
    flag for flag in opt.split() if flag != '-Wstrict-prototypes'
)

pk = pkgconfig.parse('margo')
libraries = pk['libraries']
library_dirs = pk['library_dirs']
include_dirs = pk['include_dirs']
include_dirs.append(".")
include_dirs.append(pybind11.get_include())

extra_compile_args=['-std=c++11', '-g']
if sys.platform == 'darwin':
    extra_compile_args.append('-mmacosx-version-min=10.9')

files = ["pymargo/src/pymargo.cpp"]

pymargo_module = Extension('_pymargo', files,
        libraries=libraries,
        library_dirs=library_dirs,
        include_dirs=include_dirs,
        extra_compile_args=extra_compile_args,
        depends=[])

setup(name='pymargo',
      version='0.3',
      author='Matthieu Dorier',
      description="""Python binding for Margo""",
      ext_modules=[ pymargo_module ],
      packages=['pymargo']
     )
