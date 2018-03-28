from setuptools import setup
import os.path


version = '0.1.0'

requirements = open(os.path.join(os.path.dirname(__file__), 'requirements.txt'), 'r').readlines()

setup(
    name='jsearch',
    version=version,
    description='JSearch backend services',
    packages=['jsearch'],
    zip_safe=False,
    platforms='any',
    install_requires=requirements,
    include_package_data=True,
    entry_points={
        'console_scripts': [
            'jsearch-syncer = jsearch.syncer.main:run',
        ]
    }
)