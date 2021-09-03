import setuptools

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setuptools.setup(
    name="pyspark_parser3",
    version="0.0.3",
    author="Example Author",
    author_email="mukesh7773sharma@successive.tech",
    description="A small example package",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/Mukesh7773/repo-9.git",
    project_urls={
        "Bug Tracker": "https://github.com/Mukesh7773/repo-9/issues",
    },
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    package_dir={"": "src"},
    packages=setuptools.find_packages(where="src"),
    python_requires=">=3.6",
)