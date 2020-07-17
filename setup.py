import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name='aspen_pyconnect',
    version='0.1.5',
    description='A wrapper for accessing data from Aspen IP.21.',
    url='https://github.com/RobertLeePrice',
    author='Robert Price, Jose Roberts, Arpan Seth',
    author_email='rlprice410@gmail.com',
    long_description=long_description,
    long_description_content_type="text/markdown",
    license='MIT',
    packages=setuptools.find_packages(),
    zip_safe=False,
    install_requires=[
        'requests',
        'zeep',
        'pandas'
    ],
)