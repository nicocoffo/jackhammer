from setuptools import setup

setup(name='jackhammer',
      version='0.1',
      description='Run scripts until it works',
      url='http://github.com/nicocoffo/jackhammer',
      author='Nicholas Coughlin',
      author_email='nicocoffo@gmail.com',
      license='MIT',
      packages=['jackhammer', 'jackhammer.cloud_providers'],
      zip_safe=False)
