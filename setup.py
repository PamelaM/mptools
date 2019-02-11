from distutils.core import setup

setup(
    name='mptools',
    version='0.1dev',
    author="Pamela McA'Nulty",
    author_email='mptools@mcanulty.com',
    packages=['mptools','mptools.tests'],
    license='MIT License',
    url='http://pypi.python.org/pypi/mptools/',
    description='Wrapper for multiprocessing that provides a bunch of boilerplate.',
    long_description=open('README.md').read(),
)