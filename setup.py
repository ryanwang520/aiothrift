import pathlib
import re
from setuptools import setup

here = pathlib.Path(__file__).parent
fname = here / 'aiothrift' / '__init__.py'

with fname.open() as fp:
    try:
        version = re.findall(r"__version__ = '([^']+)'$", fp.read(), re.M)[0]
    except IndexError:
        raise RuntimeError('Unable to determin version.')


def read(name):
    fname = here / name
    with fname.open() as f:
        return f.read()


with open('requirements.txt') as f:
    REQUIREMENTS = list(map(lambda l: l.strip(), f.readlines()))

setup(name='aiothrift',
      version=version,
      description='async thrift server and client',
      long_description='\n\n'.join([read('README.rst'), read('CHANGES.rst')]),
      classifiers=[

          'License :: OSI Approved :: MIT License',
          'Programming Language :: Python',
          'Programming Language :: Python :: 3',
          'Programming Language :: Python :: 3.4',
          'Programming Language :: Python :: 3.5',
          'Programming Language :: Python :: 3.6',
          'Environment :: Web Environment',
          'Intended Audience :: Developers',
          'Topic :: Software Development',
          'Topic :: Software Development :: Libraries',
      ],
      platforms=['POSIX', 'WINDOWS'],
      url='http://github.com/moonshadow/aiothrift/',
      author='Wang Haowei',
      author_email='hwwangwang@gmail.com',
      license='MIT',
      packages=['aiothrift'],
      install_requires=REQUIREMENTS,
      include_package_data=True,
      )
