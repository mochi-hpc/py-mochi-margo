from distutils.core import setup
from distutils.extension import Extension
from distutils.sysconfig import get_config_vars
import pybind11
from pybind11 import get_include
import pkgconfig
import os
import os.path
import sys

def get_pybind11_include():
    path = os.path.dirname(pybind11.__file__)
    return '/'.join(path.split('/')[0:-4] + ['include'])

(opt,) = get_config_vars('OPT')
os.environ['OPT'] = " ".join(
		    flag for flag in opt.split() if flag != '-Wstrict-prototypes'
		)

pk = pkgconfig.parse('margo')
libraries = pk['libraries']
library_dirs = pk['library_dirs'] 
include_dirs = pk['include_dirs']
include_dirs.append(".")
include_dirs.append(get_pybind11_include())

# use upstream pybind11 get_include() for
# header file inclusion in Python venvs
# see: https://xgitlab.cels.anl.gov/sds/py-ssg/-/merge_requests/3
include_dirs.append(get_include())

extra_compile_args=['-std=c++11', '-g']
if sys.platform == 'darwin':
    extra_compile_args.append('-mmacosx-version-min=10.9')

files = ["pymargo/src/pymargo.cpp", "pymargo/src/base64.cpp"]

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
