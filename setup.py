from setuptools import setup, find_packages

from metrik import __version__

setup(
    name='Metrik',
    description='Data aggregation framework for Python',
    version=__version__,
    author='Bradlee Speice',
    author_email='bradlee.speice@gmail.com',
    packages=find_packages(),
    package_data={
        'metrik': ['default.conf']
    },
    install_requires=[
        'pyquery >= 1.2.13',
        'luigi >= 2.2.0',
        'requests >= 2.11.0',
        'six >= 1.10.0',
        'pymongo >= 3.2',
        'pytz >= 2016.6.1',
        'python-dateutil >= 2.4.2',
        'pandas >= 0.17.1',
        'argparse >= 1.1.0',
        'requests-oauthlib >= 0.4.0'
    ],
    setup_requires=[
        'pytest_runner'
    ],
    tests_require=[
        'pytest',
        'pytest-catchlog',
        'pandas-datareader'
    ],
    entry_points={
        'console_scripts': [
            'metrik = metrik.batch:handle_commandline'
        ]
    }
)