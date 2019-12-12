from setuptools import find_packages, setup

import verifier

setup(
    name='verifier',
    version=verifier.version,
    description='Asset verification and deposit preparation tool',
    author='Joshua A. Westgard',
    author_email="westgard@umd.edu",
    platforms=["any"],
    license="MIT",
    url="http://github.com/jwestgard/aws-verifier",
    packages=find_packages(),
    entry_points = {
        'console_scripts': ['verifier=verifier.__main__:main']
        },
    install_requires=[i.strip() for i in open("requirements.txt").readlines()]
)
