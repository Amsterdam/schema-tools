#!/usr/bin/env python
import io
import os
import re

from setuptools import find_packages
from setuptools import setup


def read(filename):
    filename = os.path.join(os.path.dirname(__file__), filename)
    text_type = type(u"")
    with io.open(filename, mode="r", encoding="utf-8") as fd:
        return re.sub(text_type(r":[a-z]+:`~?(.*?)`"), text_type(r"``\1``"), fd.read())


setup(
    name="amsterdam-schema-tools",
    version="0.9.8",
    url="https://github.com/amsterdam/schema-tools",
    license="Mozilla Public 2.0",
    author="Jan Murre",
    author_email="jan.murre@catalyz.nl",
    description="Tools to work with Amsterdam schema.",
    long_description=read("README.md"),
    long_description_content_type="text/markdown",
    packages=find_packages(exclude=("tests",)),
    install_requires=[
        "geoalchemy2",
        "psycopg2",
        "click",
        "jsonschema",
        "ndjson>=0.3.0",
        "shapely",
        "python-string-utils",
        "python-dateutil",
        "requests",
        "jinja2",
        "mappyfile",
        "cachetools",
    ],
    extras_require={
        "tests": [
            "pytest",
            "pytest-cov",
            "pytest-django",
            "pytest-sqlalchemy",
            "requests-mock",
        ],
        "django": [
            "django >= 3.0.4",
            "django-postgres-unlimited-varchar >= 1.1.0",
            "django-gisserver >= 0.5",
            "django-environ",
        ],
    },
    tests_require=["pytest", "pytest-cov", "pytest-sqlalchemy", "requests-mock"],
    entry_points="""
        [console_scripts]
        schema=schematools.cli:main
        django=schematools.contrib.django.cli:main
    """,
    classifiers=[
        "Development Status :: 2 - Pre-Alpha",
        "License :: OSI Approved :: Mozilla Public License 2.0 (MPL 2.0)",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
    ],
    python_requires=">=3.7",
)
