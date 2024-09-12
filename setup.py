from setuptools import find_packages, setup

setup(
    name="ads",
    packages=find_packages(exclude=["ads_tests"]),
    install_requires=[
        "dagster",
        "dagster-cloud"
    ],
    extras_require={"dev": ["dagster-webserver", "pytest"]},
)

