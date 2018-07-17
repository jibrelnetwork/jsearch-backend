from setuptools import setup
import os.path
import pathlib


version = pathlib.Path('version.txt').read_text().strip()

setup(
    name='jsearch',
    version=version,
    description='JSearch backend services',
    packages=['jsearch', 'jsearch.common', 'jsearch.syncer'],
    zip_safe=False,
    platforms='any',
    install_requires=[],
    include_package_data=True,
    entry_points={
        'console_scripts': [
            'jsearch-syncer = jsearch.syncer.main:run',
        ]
    }
)
