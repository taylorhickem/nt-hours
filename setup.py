from setuptools import setup, find_packages

setup(
    name='nt-hours',
    version='1.0',
    packages=find_packages(),
    url='https://github.com/taylorhickem/nt-hours',
    description='add-on utility for time tracking report using NowThen time tracker app by AngryAztec',
    author='@taylorhickem',
    long_description=open('README.md').read(),
    install_requires=open("requirements.txt", "r").read().splitlines(),
    include_package_data=True
)