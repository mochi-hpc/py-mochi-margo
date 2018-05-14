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

pk = pkgconfig.parse('margo')
libraries = pk['libraries']
python_version = str(sys.version_info[0])+str(sys.version_info[1])
libraries.append('boost_python'+python_version)
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
