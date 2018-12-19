import pathlib

from setuptools import setup

version = pathlib.Path('version.txt').read_text().strip()

setup(
    name='jsearch',
    version=version,
    description='JSearch backend services',
    packages=[
        'jsearch',
        'jsearch.common',
        'jsearch.common.integrations',
        'jsearch.common.processing',
        'jsearch.syncer',
        'jsearch.post_processing',
    ],
    zip_safe=False,
    platforms='any',
    install_requires=[],
    include_package_data=True,
    entry_points={
        'console_scripts': [
            'jsearch-syncer = jsearch.syncer.main:run',
            'jsearch-post-processing = jsearch.post_processing.main:run',
        ]
    }
)
