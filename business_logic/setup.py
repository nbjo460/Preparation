from setuptools import setup, Extension
from Cython.Build import cythonize

extensions = [
    Extension(
        "reader_fast",
        ["reader_fast.pyx"],
        extra_compile_args=["-O3", "-march=native", "-ffast-math"],
    )
]

setup(
    name="reader_fast",
    ext_modules=cythonize(extensions, compiler_directives={
        "language_level":3, "boundscheck":False, "wraparound":False, "cdivision":True
    })
)
