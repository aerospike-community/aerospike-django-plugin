==========================
Aerospike Django Cache Backend
==========================

A cache backend for Django using the Aerospike key-value store server.


Changelog
=========

0.1.0
------

* first release.

Notes
-----

This cache backend requires the `python-client`_ Python client library for
communicating with the Aerospike server.

You can install aerospike python client library it by following instruction `install-python-client`_.


Usage
-----

1. Run ``python setup.py install`` to install,
   or place ``aerospike_cache`` on your Python path.

2. Modify your Django settings to use ``aerospike_cache`` :

On Django < 1.3::

    CACHE_BACKEND = 'aerospike_cache.cache://<host>:<port>'

On Django >= 1.3::


    # The OPTIONS are optional and needed if you need to override the default, username/password are only required for enterprise edition.
    CACHES = {
        'default': {
            'BACKEND': 'aerospike_cache.AerospikeCache',
            'LOCATION': '<host>:<port>',
            'OPTIONS': {# optional
                'NAMESPACE': "test",
                'SET': "cache",   
                'BIN': "entry",
                'USERNAME': "username",
                'PASSWORD': "password",
            },
        },
    }

.. _aerospike: http://www.aerospike.com
.. _python-client: http://www.aerospike.com/docs/client/python/
.. _install-python-client: http://www.aerospike.com/docs/client/python/install/
