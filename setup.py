from setuptools import setup, find_packages

with open("README.md", encoding="utf-8") as f:
    long_description = f.read()
setup(
    name='riesgosutils',
    version='0.1.2',
    packages=find_packages(),
    install_requires=[
        'sqlalchemy==2.0.40',
        'pandas',
        'numpy',
        'exchangelib',
        'openpyxl',
        'oracledb',
        'boto3',
        'pyspark',
        'delta-spark',
        'pyarrow',
        'fastparquet',
        'pymssql',
        'oracledb',
        'psycopg2-binary'


    ],
    author='Isaac Rivera',
    author_email='ariveras@uni.pe',
    description='Coleccion de clases y funciones recurrentes para el Departamento de Riesgos.',
    long_description=long_description,
    long_description_content_type='text/markdown',
    url='https://github.com/',
    classifiers=[
        'Programming Language :: Python :: 3',
        'License :: 2024, Propiedad de Caja Arequipa.',
        'Operating System :: OS Independent',
    ],
    python_requires='>=3.10',
)
