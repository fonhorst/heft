from os.path import exists
from setuptools import setup

setup(name='SchedulerStand',
      version='0.1',
      description='scheduling heuristics for hpc environment',
      url='https://github.com/fonhorst/heft',
      author='fonhorst',
      author_email='alipoov.nb@gmail.com',
      license='BSD',
      packages=['heft'],
      long_description=open('README.md').read() if exists("README.md") else "",
      zip_safe=False,
      install_requires=['deap>=1.0.1',
                        'numpy>=1.8.0',
                        'scoop>=0.7',
                        'networkx>=1.8.1',
                        'distance>=0.1.3',
                        'matplotlib>=1.3.1'])
