from setuptools import setup
from Cython.Build import cythonize

setup(
    ext_modules=cythonize(
        "old_reader.pyx",
        compiler_directives={
            'language_level': "3",
            'boundscheck': False,
            'wraparound': False,
            'cdivision': True,
            'infer_types': True,
        }
    )
)