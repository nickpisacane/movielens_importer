from setuptools import setup
import movielens_importer

setup(
    name='movielens_importer',
    version=movielens_importer.__version__,
    decription='MovieLens data set importer.',
    long_description='MovieLens data set importer.',
    url='https://github.com/nickpisacane/movielens_importer',
    author='Nick Pisacane',
    author_email='pisacanen@gmail.com',
    liscense='MIT',
    scripts=['bin/movielens-download']
)
