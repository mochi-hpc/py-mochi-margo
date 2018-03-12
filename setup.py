from distutils.core import setup
from distutils.extension import Extension
from distutils.sysconfig import get_config_vars
import os
import os.path
import sys

(opt,) = get_config_vars('OPT')
os.environ['OPT'] = " ".join(
		    flag for flag in opt.split() if flag != '-Wstrict-prototypes'
		)

include_dirs = ["."]
files = ["pymargo/src/pymargo.cpp"]

pymargo_module = Extension('_pymargo', files,
		           libraries=['boost_python','margo'],
			   include_dirs=['.'],
			   depends=[])
setup(name='pymargo',
      version='0.1',
      author='Matthieu Dorier',
      description="""Python binding for Margo""",      
      ext_modules=[ pymargo_module ],
      packages=['pymargo']
     )
