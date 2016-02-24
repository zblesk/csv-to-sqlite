from setuptools import setup

setup(
    name='csv-to-sqlite',
    version='1.0.0',
    py_modules=['csv_to_sqlite'],
    install_requires=[
        'Click',
    ],
    entry_points='''
        [console_scripts]
        csv-to-sqlite=csv_to_sqlite:start
    ''',
)