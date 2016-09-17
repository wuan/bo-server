#!/usr/bin/env python

from setuptools import setup, find_packages

import glob

setup(
    name='blitzortung-server',
    packages=find_packages(exclude=('tests',)),
    scripts=glob.glob('scripts/*'),
    install_requires=['blitzortung'],
    tests_require=['nose', 'mock', 'coverage', 'assertpy'],
    version='1.6',
    description='blitzortung.org server scripts',
    download_url='http://www.tryb.de/andi/blitzortung/',
    author='Andreas Wuerl',
    author_email='blitzortung@tryb.de',
    url='http://www.blitzortung.org/',
    license='GPL-3 License',
    long_description="""a library providing python classes for blitzortung operation""",
    platforms='OS Independent',
    classifiers=[
        'Development Status :: 4 - Beta',
        'Environment :: Console',
        'Environment :: Plugins',
        'Intended Audience :: Developers',
        'Intended Audience :: Education',
        'Intended Audience :: Science/Research',
        'License :: OSI Approved :: Apache Software License',
        'Natural Language :: English',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Topic :: Scientific/Engineering :: Atmospheric Science',
        'Topic :: Scientific/Engineering :: GIS',
        'Topic :: Software Development :: Libraries :: Python Modules',
        'Topic :: Utilities',
    ],
)
