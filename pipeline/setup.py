import sys
from setuptools import setup, find_packages
from setuptools.command.test import test as test_command


install_requires = [
    i.strip() for i in open('requirements.txt').readlines() if i.strip()]


class PyTest(test_command):
    user_options = [('pytest-args=', 'a', 'Arguments to pass to pytest')]

    def initialize_options(self):
        test_command.initialize_options(self)
        self.pytest_args = []

    def finalize_options(self):
        test_command.finalize_options(self)
        self.test_args = []
        self.test_suite = True

    def run_tests(self):
        import pytest
        errno = pytest.main(self.pytest_args)
        sys.exit(errno)


setup(
    name='sample-etl-pipeline',
    version='1.0',
    description='Sample ETL pipeline with Luigi and PySpark',
    packages=find_packages(exclude=['tests']),
    include_package_data=True,
    zip_safe=False,
    install_requires=install_requires,
    tests_require=['pytest'],
    cmdclass={'test': PyTest},
    entry_points={'console_scripts': []},
)
