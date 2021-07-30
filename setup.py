from setuptools import setup, find_packages

from version import build_version

setup(name=' ',
      version=build_version,
      description='A ',
      url='ht ',
      author=' ',
      author_email=' ',
      license='',
      packages=find_packages(),
      package_data={'': ['requirements.txt', '__main__.py']},
      data_files=[('.', ['requirements.txt', '__main__.py'])],
      include_package_data=True,
      zip_safe=False)
