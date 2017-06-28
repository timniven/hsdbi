from setuptools import setup


setup(
    name='hsdbi',
    description='A simple interface for accessing databases.',
    long_description='Exposes a common interface for all kinds of database '
                     'connections. In version 0.1, SQL databases (via '
                     'sqlalchemy) and MongoDB are implemented.',
    version='0.1a',
    url='https://github.com/timniven/hsdbi',
    download_url='https://github.com/timniven/hsdbi/archive/0.1a.tar.gz',
    author='Tim Niven',
    author_email='tim.niven.public@gmail.com',
    license='MIT',
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'Programming Language :: Python :: 3',
        'License :: OSI Approved :: MIT License'
    ],
    packages=['hsdbi'],
    install_requires=[
        'pymongo>=3.4.0',
        'sqlalchemy>=1.1.11'
    ]
)
