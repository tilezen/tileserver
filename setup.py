import os.path
from setuptools import setup, find_packages

version_path = os.path.join(os.path.dirname(__file__), 'VERSION')
with open(version_path) as fh:
    version = fh.read().strip()

setup(name='tileserver',
      version=version,
      description="",
      long_description="""\
""",
      # Get strings from http://pypi.python.org/pypi?%3Aaction=list_classifiers
      classifiers=[],
      keywords='',
      author='',
      author_email='',
      url='',
      license='',
      packages=find_packages(exclude=['ez_setup', 'examples', 'tests']),
      include_package_data=True,
      zip_safe=False,
      install_requires=[
          'PyYAML',
          'tilequeue >= 0.7.1',
          'werkzeug',
      ],
      test_suite='tests',
      entry_points="""
      # -*- Entry points: -*-
      """,
      )
