from setuptools import find_packages, setup
from subprocess import check_call


with open("requirements.txt") as f:
    packages = [l.strip() for l in f.readlines() if not l.strip().startswith("#")]


setup(
    name="homedir-archiver",
    version="0.1",
    url="https://github.com/yuvipanda/homedir-archiver",
    license="3-clause BSD",
    author="Yuvi Panda",
    author_email="yuvipanda@gmail.com",
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    packages=find_packages(),
    install_requires=packages,
    platforms="any",
    zip_safe=False,
)
