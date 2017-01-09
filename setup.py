import os

from setuptools import setup

readme = open('README.rst').read()

CLASSIFIERS = [
    'Development Status :: 3 - Alpha',
    'Environment :: Other Environment',
    'Intended Audience :: Developers',
    'License :: OSI Approved :: MIT License',
    'Operating System :: MacOS :: MacOS X',
    'Operating System :: POSIX',
    'Programming Language :: Python',
    'Programming Language :: Python :: 3.6',
    'Topic :: Utilities',
    'Topic :: Software Development :: Libraries :: Python Modules',
]

fname = os.path.join(os.path.dirname(__file__), 'requirements.txt')

with open(fname) as f:
    REQUIREMENTS = list(map(lambda l: l.strip(), f.readlines()))

py_modules = []

for root, folders, files in os.walk('aiothrift'):
    for f in files:
        if f.endswith('.py'):
            full = os.path.join(root, f[:-3])
            parts = full.split(os.path.sep)
            modname = '.'.join(parts)
            py_modules.append(modname)

setup(
    name='aiothrift',
    version='0.0.1',

    url='http://github.com/moonshadow/aiothrift/',
    description='Thrift async',
    long_description=readme,
    author='Wang Haowei',
    author_email='hwwangwang@gmail.com',
    license='MIT',

    classifiers=CLASSIFIERS,
    zip_safe=False,
    py_modules=py_modules,
    include_package_data=True,
    install_requires=[
    ],
)
