from setuptools import setup
from Cython.Build import cythonize

setup(
    name="fast_get_value",
    ext_modules=cythonize("fast_get_value.pyx", language_level=3),
)