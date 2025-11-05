from setuptools import setup
from Cython.Build import cythonize
from setuptools.extension import Extension
import os

ext_modules = [
    Extension(
        name="coordinate_extractor_cy",
        sources=[os.path.join("coordinate_extractor_cy.pyx")],
        language="c",
    )
]

setup(
    name="coordinate_extractor_cy",
    ext_modules=cythonize(
        ext_modules,
        compiler_directives={"language_level": 3},
        build_dir="build",  # אפשר גם להשאיר ברירת מחדל
    ),
)
