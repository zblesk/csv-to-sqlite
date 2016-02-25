from setuptools import setup
from codecs import open
from os import path

here = path.abspath(path.dirname(__file__))
with open(path.join(here, 'ReadMe.rst'), encoding='utf-8') as f:
    long_description = f.read()

setup(
    name='csv-to-sqlite',
    version='1.0.1',
    description='A utility that copies csv files into a SQLite database',
    py_modules=['csv_to_sqlite'],
    long_description=long_description,
    url='https://github.com/zblesk/csv-to-sqlite',
    author='Ladislav Benc',
    author_email='laci@zblesk.net',
    license='MIT',
    keywords='csv sqlite conversion copy',
    classifiers=[
        'Development Status :: 5 - Production/Stable',
        'Environment :: Console',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Natural Language :: English',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3.5',
        'Topic :: Utilities',
    ],
    install_requires=[
        'Click',
    ],
    entry_points='''
        [console_scripts]
        csv-to-sqlite=csv_to_sqlite:start
    ''',
)
