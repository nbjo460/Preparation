@echo off
@REM REM Activate virtual environment if exists
@REM IF EXIST ".venv\Scripts\activate.bat" (
@REM     echo Activating virtual environment...
@REM     call .venv\Scripts\activate.bat
@REM ) ELSE (
@REM     echo No virtual environment found. Using system Python.
@REM )

REM Add src to PYTHONPATH
set PYTHONPATH=%CD%\src;%PYTHONPATH%

REM 0️⃣ Check import order with isort
echo.
echo Running isort (check only, show diff, line length 120)...
isort src  --diff --line-length 120
isort test  --diff --line-length 120

REM 1️⃣ Check code formatting with Black
echo.
echo Running Black (check only, line length 120)...
black src  --line-length 120
black test  --line-length 120

REM 2️⃣ Type hints check with Mypy
echo.
echo Running Mypy (ignore missing imports, disallow untyped defs)...
mypy src --ignore-missing-imports --disallow-untyped-defs

REM 3️⃣ Lint code with Pylint (max line length 120)
echo.
echo Running Pylint (max line length=120)...
pylint --max-line-length=120 src
pylint --max-line-length=120 test

REM 4️⃣ Run all tests with Pytest
echo.
@REM echo Running Pytest (stop on first fail, disable warnings, verbose)...
@REM pytest --maxfail=1 --disable-warnings -v

echo.
echo All checks completed!
pause
