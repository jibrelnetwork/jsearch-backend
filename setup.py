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
            'jsearch-syncer-pending = jsearch.pending_syncer.main:run',
            'jsearch-monitor-balance = jsearch.monitor_balance.__main__:main',
        ]
    }
)
