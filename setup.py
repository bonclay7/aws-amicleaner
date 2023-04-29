#!/usr/bin/env python

from setuptools import setup, find_packages
from amicleaner import __author__, __author_email__
from amicleaner import __license__, __version__


with open('README.rst') as readme_file:
    readme = readme_file.read()

with open('HISTORY.rst') as history_file:
    history = history_file.read()

install_requirements = ['awscli', 'argparse', 'boto',
                        'boto3', 'prettytable', 'blessings', 'future']

test_requirements = ['moto', 'pytest', 'pytest-pep8', 'pytest-cov']


setup(
    name='aws-amicleaner',
    version=__version__,
    description='Cleanup tool for AWS AMIs and snapshots',
    long_description=readme + "\n\n" + history,
    author=__author__,
    author_email=__author_email__,
    url='https://github.com/bonclay7/aws-amicleaner/',
    license=__license__,
    packages=find_packages(exclude=['tests']),
    classifiers=[
        'Development Status :: 5 - Production/Stable',
        'Environment :: Console',
        'Intended Audience :: Developers',
        'Intended Audience :: System Administrators',
        'Intended Audience :: Information Technology',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Topic :: Software Development',
        'Topic :: Software Development :: Libraries :: Python Modules',
    ],
    entry_points={
        'console_scripts': [
            'amicleaner = amicleaner.cli:main',
        ],
    },
    tests_require=test_requirements,
    install_requires=install_requirements,
)
