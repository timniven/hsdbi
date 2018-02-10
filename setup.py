from setuptools import setup, find_packages


# python setup.py sdist upload -r pypi


setup(
    name='hsdbi',
    packages=find_packages(exclude=['testing']),
    version='0.1a18',
    description='A simple interface for accessing databases.',
    author='Tim Niven',
    author_email='tim.niven.public@gmail.com',
    url='https://github.com/timniven/hsdbi',
    download_url='https://github.com/timniven/hsdbi/archive/0.1a18.tar.gz',
    license='MIT',
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'Topic :: Software Development :: Libraries :: Python Modules',
        'Programming Language :: Python :: 3',
        'License :: OSI Approved :: MIT License',
    ],
    keywords='database interface facade',
    install_requires=[
        'pymongo',
        'sqlalchemy'
    ]
)
