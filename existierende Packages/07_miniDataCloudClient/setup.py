import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="MDCclient",
    version="0.0.2",
    author="Adrian Lauber",
    author_email="adrian.lauber@hslu.ch",
    description="python-mini-data-cloud-client",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://gitlab.com/alauber/mdcclient",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.6',
)
