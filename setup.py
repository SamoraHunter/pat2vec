from setuptools import setup, find_packages

setup(
    name="pat2vec",
    version="1.0.0",
    description=(
        "A comprehensive Python package for healthcare data engineering, designed to "
        "extract, transform, and feature engineer patient data from CogStack-based "
        "electronic health record (EHR) datalakes. It provides tools for cohort "
        "building, batch data processing, clinical note analysis, and creating "
        "machine learning-ready datasets."
    ),
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    author="Samora Hunter",
    author_email="samorahunter@gmail.com",
    url="https://github.com/SamoraHunter/pat2vec.git",
    packages=find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
)
