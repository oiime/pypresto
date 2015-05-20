#!/usr/bin/env python

from setuptools import setup, find_packages

setup(
    name="pypresto",
    version="0.0.1",
    description="Python client library for presto",
    url="https://github.com/qdatum/pypresto",
    download_url='https://github.com/qdatum/pypresto/tarball/0.0.1',
    author="Itamar Maltz",
    author_email="ism@qdatum.io",
    packages=find_packages(),
    platforms=["any"],
    license='MIT',
    keywords="cassandra hive presto",
    classifiers=[
        "Intended Audience :: Developers",
        "Operating System :: OS Independent",
        "Programming Language :: Python",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "License :: OSI Approved :: MIT License",
    ],
)
