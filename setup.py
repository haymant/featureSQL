from setuptools import setup, find_packages

setup(
    name="featureSQL",
    version="0.1.0",
    description="SQL Query Engine for Feature Store",
    author="",
    packages=find_packages(exclude=["tests*"]),
    install_requires=[
        "numpy",
        "pandas",
        "requests",
        "yahooquery",
        "loguru",
        "fire",
        "tqdm",
        "duckdb",
    ],
    entry_points={
        "console_scripts": [
            "featureSQL=featureSQL.cli:main",
        ],
    },
    python_requires=">=3.8",
)
