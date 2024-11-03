from setuptools import setup, find_packages

setup(
    name='firstpackage',
    version='0.1',
    packages=find_packages(exclude=['tests*']),
    license='MIT',
    description='EDSA example python package',
    long_description=open('README.md').read(),
    install_requires=['numpy'],
    url='https:github.com/petermartian/firstpackage',
    author='Martian',
    author_email='petmartian@gmail.com'
)