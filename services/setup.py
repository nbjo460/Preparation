# setup.py
from setuptools import setup
from Cython.Build import cythonize

setup(
    name="mavlink_cython",
    ext_modules=cythonize(
        "py_cy.pyx",
        compiler_directives={
            'language_level': 3,
            'boundscheck': False,
            'wraparound': False,
            'initializedcheck': False,
        }
    ),
)