from setuptools import setup
from Cython.Build import cythonize

setup(
    name="coordinate_extractor_cy",
    ext_modules=cythonize(
        "coordinate_extractor_cy.pyx",
        compiler_directives={'language_level': "3"}
    ),
)
