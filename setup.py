from setuptools import setup, find_packages
from os import path


here = path.abspath(path.dirname(__file__))

with open(path.join(here, 'README.md'), encoding='utf-8') as f:
    long_description = f.read()


setup(
    name='hsdbi',
    packages=find_packages(exclude=['testing']),
    version='0.1a',
    description='A simple interface for accessing databases.',
    long_description=long_description,
    author='Tim Niven',
    author_email='tim.niven.public@gmail.com',
    url='https://github.com/timniven/hsdbi',
    download_url='https://github.com/timniven/hsdbi/archive/0.1a.tar.gz',
    license='MIT',
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'Topic :: Software Development :: Libraries :: Python Modules'
        'Programming Language :: Python :: 3',
        'License :: OSI Approved :: MIT License'
    ],
    keywords='database interface facade',
    install_requires=[
        'pymongo',
        'sqlalchemy'
    ]
)
