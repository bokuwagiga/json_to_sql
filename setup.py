from setuptools import setup, find_packages

setup(
    name="JsonToSQL",
    version="1.0.2",
    description="Decompose nested JSON data into relational SQL Server tables",
    author="bokuwagiga",
    url="https://github.com/bokuwagiga/json_to_sql",
    packages=find_packages(),
    install_requires=[
        "pandas>=1.0.0",
        "pyodbc>=4.0.30",
    ],
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
        "Programming Language :: Python :: 3.13",
    ],
    python_requires=">=3.7",
)