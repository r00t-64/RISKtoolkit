from setuptools import setup, find_packages

setup(
    name='riesgosutils',
    version='0.1.0',
    packages=find_packages(),
    install_requires=[
        'sqlalchemy',
        'pandas',
        'numpy',
        'exchangelib',
        'openpyxl',
        'cx_Oracle'

    ],
    author='Isaac Rivera',
    author_email='ariveras@cajaarequipa.pe',
    description='Coleccion de clases y funciones recurrentes para el Departamento de Riesgos.',
    long_description=open('README.md').read(),
    long_description_content_type='text/markdown',
    url='https://github.com/',
    classifiers=[
        'Programming Language :: Python :: 3',
        'License :: 2024, Propiedad de Caja Arequipa.',
        'Operating System :: OS Independent',
    ],
    python_requires='>=3.10',
)