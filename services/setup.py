from setuptools import setup
from Cython.Build import cythonize

setup(
    name="coordinate_extractor_cy",
    ext_modules=cythonize(
        "coordinate_extractor_cy.pyx",
        annotate=True,
        compiler_directives={
            "language_level": 3,
            "boundscheck": False,
            "wraparound": False,
            "initializedcheck": False,
            "cdivision": True,
        },
    ),
)
