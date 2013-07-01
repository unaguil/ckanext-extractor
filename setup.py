from setuptools import setup, find_packages
import sys, os

version = '0.1'

setup(
    name='ckanext-extractor',
    version=version,
    description="SPARQL endpoint analyzer and extractor generator for CKAN",
    long_description="""\
    """,
    classifiers=[], # Get strings from http://pypi.python.org/pypi?%3Aaction=list_classifiers
    keywords='',
    author='Unai Aguilera',
    author_email='unai.aguilera@deusto.es',
    url='http://www.morelab.deusto.es',
    license='AGPL',
    packages=find_packages(exclude=['ez_setup', 'examples', 'tests']),
    namespace_packages=['ckanext', 'ckanext.extractor'],
    include_package_data=True,
    zip_safe=False,
    install_requires=[
        'celery',],
    entry_points=\
    """
    [ckan.plugins]
        extractor=ckanext.extractor.plugin:ExtractorExtension

    [ckan.celery_task]
        tasks = ckanext.extractor.celery_import:task_imports
    """,
)
