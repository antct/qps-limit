
from setuptools import find_packages, setup

from qps_limit import __version__

with open("readme.md", "r") as fh:
    long_description = fh.read()

setup(
    name='qps-limit',
    version=__version__,
    author='tt',
    author_email='527892245@qq.com',
    packages=find_packages(),
    url='https://github.com/antct/rate-limit',
    license="GPLv2",
    classifiers=[
        'Intended Audience :: Developers',
        'License :: OSI Approved :: GNU General Public License v2 (GPLv2)',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
        'Programming Language :: Python :: 3.10',
        'Programming Language :: Python :: 3.11',
        'Programming Language :: Python :: 3 :: Only',
    ],
    zip_safe=True,
    install_requires=[
        "aiolimiter"
    ],
    description='Run functions under any limited rate',
    long_description=long_description,
    long_description_content_type='text/markdown'
)
