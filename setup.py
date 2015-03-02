from setuptools import setup

setup(
    name = "django-aerospike-cache",
    url = "https://github.com/aerospike/aerospike-django-plugin",
    author = "Aerospike",
    author_email = "dhaval@aerospike.com",
    version = "0.2.0",
    packages = ["aerospike_cache"],
    description = "Aerospike Cache Backend for Django",
    install_requires=['aerospike>=1.0.37',],
    classifiers = [
        "Programming Language :: Python",
        "Programming Language :: Python :: 2.6",
        "Programming Language :: Python :: 2.7",
        "Programming Language :: Python :: 3.3",
        "Operating System :: OS Independent",
        "Topic :: Software Development :: Libraries",
        "Topic :: Utilities",
        "Environment :: Web Environment",
        "Framework :: Django",
    ],
)
