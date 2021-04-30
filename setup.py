from setuptools import setup, find_packages

setup(name='etlgcp',
      version='0.1.2',
      description='This package is full of GCP functions to easily do basic actions.',
      author='Affini-Tech',
      author_email='lco@affini-tech.com',
      packages=['etlgcp'],
      install_requires=[
        'google-cloud-bigquery>=1.24.0',
        'google-cloud-logging>=1.15.0',
        'google-cloud-pubsub>=1.4.3',
        'google-cloud-storage>=1.28.1'
      ]
    )
