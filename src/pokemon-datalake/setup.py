from setuptools import find_packages, setup

setup(
    name="pokemon_datalake",
    packages=find_packages(exclude=["pokemon_datalake_tests"]),
    install_requires=[
        "dagster",
        "dagster-cloud",
        "requests",
        "boto3",
        "python-dotenv",
        "duckdb",
        "tdqm",
        "deltalake",
        "pandas",
    ],
    extras_require={"dev": ["dagster-webserver", "pytest"]},
)
