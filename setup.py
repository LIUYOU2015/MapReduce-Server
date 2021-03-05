"""
Mapreduce python modules.
"""

from setuptools import setup

setup(
    name='mapreduce',
    version='0.1.0',
    packages=['mapreduce'],
    include_package_data=True,
    install_requires=[
        'click==7.1.1',
        'pycodestyle==2.5.0',
        'pydocstyle==5.0.2',
        'pylint==2.4.4',
        'pytest==5.3.5',
    ],
    entry_points={
        'console_scripts': [
            'mapreduce-master = mapreduce.master.__main__:main',
            'mapreduce-worker = mapreduce.worker.__main__:main',
            'mapreduce-submit = mapreduce.submit:main',
        ]
    },
)
