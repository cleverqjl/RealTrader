from setuptools import setup, find_packages

setup(
    name="realtrader",
    version="0.1",
    packages=find_packages(),
    package_dir={'': '.'},
    install_requires=[
        'numpy',
        'pandas'
    ]
)
