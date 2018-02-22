#!/usr/bin/env python
"""The setup script."""
import os.path as op
import warnings

from setuptools import find_packages, setup


def _read_md_as_rst(file):
    """Read Markdown file and convert it to ReStructuredText."""
    from pypandoc import convert_file
    return convert_file(file, 'rst', format='md')


def _read_md_as_md(file):
    """Read Markdown file."""
    with open(op.join(op.dirname(__file__), file)) as ifh:
        return ifh.read()


def read_md(file):
    """Read MarkDown file and try to convert it to ReStructuredText if you can."""
    try:
        return _read_md_as_rst(file)
    except ImportError:
        warnings.warn("pypandoc module not found, could not convert Markdown to RST!")
        return _read_md_as_md(file)


requirements = [
    'Click>=6.0',
    # TODO: put package requirements here
]

test_requirements = [
    'pytest',
    # TODO: put package test requirements here
]

setup(
    name='jobsubmitter',
    version='0.1.1.dev0',
    description="Package for running jobs on Sun Grid Engine (SGE) / PBS / Slurm clusters.",
    long_description=read_md('README.md') + '\n\n' + read_md('HISTORY.md'),
    author="Alexey Strokach",
    author_email='alex.strokach@utoronto.ca',
    url='https://gitlab.com/kimlab/jobsubmitter',
    packages=find_packages(),
    entry_points={'console_scripts': ['jobsubmitter=jobsubmitter.cli:main']},
    scripts=['jobsubmitter/scripts/qsub.sh'],
    package_data={
        'jobsubmitter': 'scripts/*.sh',
    },
    include_package_data=True,
    install_requires=requirements,
    license='MIT',
    zip_safe=False,
    keywords='jobsubmitter',
    classifiers=[
        "License :: OSI Approved :: MIT License",
        'Development Status :: 2 - Pre-Alpha',
        'Intended Audience :: Developers',
        'Natural Language :: English',
        'Programming Language :: Python :: 3.6',
    ],
    test_suite='tests',
    tests_require=test_requirements,
)
