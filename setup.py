import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name='mptools',
    version='0.1.1dev',
    author="Pamela McA'Nulty",
    author_email='pamela@mcanulty.com',
    packages=setuptools.find_packages(),
    license='MIT License',
    url="https://github.com/pypa/sampleproject",
    description='Wrapper for multiprocessing that provides a bunch of boilerplate.',
    long_description=long_description,
    long_description_content_type="text/markdown",
    classifiers=[
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Development Status :: 2 - Pre-Alpha",
        "Topic :: Utilities ",
    ],
)