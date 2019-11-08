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
    include_package_data=True,
    zip_safe=False,
    platforms='any',
    install_requires=[],
    entry_points={
        'console_scripts': [
            'jsearch = jsearch.cli:cli',
            'jsearch-syncer = jsearch.syncer.main:run',
            'jsearch-syncer-pending = jsearch.pending_syncer.main:run',
            'jsearch-token-holders-cleaner = jsearch.token_holders_cleaner.main:run',
            'jsearch-data-checker = jsearch.data_checker.main:run',
            'jsearch-monitor-balance = jsearch.monitor_balance.__main__:main',
            'jsearch-index-manager = jsearch.common.index_manager.main:run',
        ]
    }
)
