from distutils.core import setup
from distutils.extension import Extension
from distutils.sysconfig import get_config_vars
import pkgconfig
import os
import os.path
import sys

(opt,) = get_config_vars('OPT')
os.environ['OPT'] = " ".join(
		    flag for flag in opt.split() if flag != '-Wstrict-prototypes'
		)

standard_lib_path = os.environ['LIBRARY_PATH'].split(':')

pk = pkgconfig.parse('margo')
libraries = pk['libraries']
if (sys.version_info[0] == 3):
    libraries.append('boost_python3')
elif ((sys.version_info[0] == 2 and sys.version_info[1] == 7)):
    libraries.append('boost_python27')
else:
    raise "PyMargo only works with Python 2.7 or Python 3"

library_dirs = pk['library_dirs'] 
include_dirs = pk['include_dirs']
include_dirs.append(".")

files = ["pymargo/src/pymargo.cpp"]

pymargo_module = Extension('_pymargo', files,
        libraries=libraries,
        library_dirs=library_dirs,
        include_dirs=include_dirs,
        depends=[])

setup(name='pymargo',
      version='0.1',
      author='Matthieu Dorier',
      description="""Python binding for Margo""",      
      ext_modules=[ pymargo_module ],
      packages=['pymargo']
     )
