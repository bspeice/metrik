from setuptools import setup, find_packages

setup(
    name='Metrik',
    description='Data aggregation framework for Python',
    version='0.1.0',
    author='Bradlee Speice',
    author_email='bradlee.speice@gmail.com',
    packages=find_packages(),
    install_requires=[
        'pyquery >= 1.2.13',
        'luigi >= 2.2.0',
    ],
    setup_requires=[
        'pytest_runner'
    ],
    tests_require=[
        'pytest',
        'pytest-catchlog'
    ]
)