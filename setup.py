from setuptools import setup, find_packages

version = '0.5.0.dev0'

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
          'tilequeue',
          'TileStache',
          'werkzeug',
      ],
      entry_points="""
      # -*- Entry points: -*-
      """,
      )
