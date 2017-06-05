#import ez_setup
#ez_setup.use_setuptools()
from setuptools import setup, find_packages
setup(name='digilabnasq',
      version='0.1',
      packages= find_packages(),
      install_requires=[
          'celery==3.1.22',
          'pymongo==3.2.1',
      ],
      include_package_data=True,
)
