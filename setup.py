from setuptools import setup, find_packages

setup(
    name='pat2vec',
    version='1.0.0',
    description='Extract, transform, feature engineer, and produce machine learning ready datasets from a CogStack datalake.',
    long_description=open('README.md').read(),
    long_description_content_type='text/markdown',
    author='Samora Hunter',
    author_email='samorahunter@gmail.com',
    url='https://github.com/SamoraHunter/pat2vec_time.git',
    packages=find_packages(),
    classifiers=[
        'Programming Language :: Python :: 3',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
    ],
)
