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
    version="0.0.3",
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
        "amsterdam-schema",
        "ndjson",
        "shapely",
    ],
    entry_points="""
        [console_scripts]
        schema=schematools.cli:schema
    """,
    classifiers=[
        "Development Status :: 2 - Pre-Alpha",
        "License :: OSI Approved :: Mozilla Public License 2.0 (MPL 2.0)",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
    ],
)
