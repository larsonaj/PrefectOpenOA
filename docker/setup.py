from setuptools import setup, find_packages

with open('requirements.txt') as r:
    requirements = r.read().splitlines()

setup(
    name="prefect-flycast",
    version='0.1',
    install_requires=requirements
)