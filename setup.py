import pathlib

from setuptools import setup, find_packages

version = pathlib.Path('version.txt').read_text().strip()
setup(
    name='jsearch',
    version=version,
    description='JSearch backend services',
    packages=find_packages(
        exclude=('*.tests.*',),
        include=('jsearch', 'jsearch.*',)
    ),
    zip_safe=False,
    platforms='any',
    install_requires=[],
    include_package_data=True,
    entry_points={
        'console_scripts': [
            'jsearch-syncer = jsearch.syncer.main:run',
            'jsearch-post-processing = jsearch.post_processing.main:run',
            'jsearch-check = jsearch.validation.__main__:check',
        ]
    }
)
