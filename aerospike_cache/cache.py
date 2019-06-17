"Aerospike cache module"
from __future__ import print_function
import time, sys

import types # to check for function type for picking

#pickling is taken care by the client library for all except function/class/tuple
try:
    import cPickle as pickle
except ImportError:
    import pickle

try:
    import aerospike
except ImportError:
    raise InvalidCacheBackendError(
        "Aerospike cache backend requires the 'aerospike' library")

#from array import array #for unsupported data types
import inspect

from django.core.cache.backends.base import BaseCache, DEFAULT_TIMEOUT
try:
    # Django 1.5+
    from django.utils.encoding import smart_text, smart_bytes
except ImportError:
    # older Django, thus definitely Python 2
    from django.utils.encoding import smart_unicode, smart_str
    smart_text = smart_unicode
    smart_bytes = smart_str


class AerospikeCache(BaseCache):
    def __init__(self, server, params):
        """
        set up cache backend.
        """
        self._init(server, params)

    def _init(self, server, params):
        """
        OPTIONS have
        HOST
        PORT
        NAMESPACE
        SET
        BIN
        TIMEOUT
        """
        BaseCache.__init__(self, params)
        self._server = server
        self._params = params

        if ':' in self.server:
            host, port = self.server.rsplit(':', 1)
            try:
                port = int(port)
            except (ValueError, TypeError):
                raise ImproperlyConfigured("port value must be an integer")
        else:
            host, port = None, None

        config = {
            "hosts": [
                  ( host, port )
              ],
              "policies": {
                  #aerospike timeout has no equivalent in django cache
                  #"timeout": self.timeout # milliseconds
              }
          }

        self._client = aerospike.client(config)

        #community edition does not need username/password
        if self.username is None and self.password is None:
            self._client.connect()
        #check for username/password for enterprise versions
        else:
            self._client.connect(self.username, self.password)


    #for pickling, not needed as pickling is handled by the client library
    def __getstate__(self):
        return {'params': self._params, 'server': self._server}

    def __setstate__(self, state):
        self._init(**state)

    @property
    def server(self):
        """
        the server:port combination for aerospike server
        """
        return self._server or "127.0.0.1:3000"

    @property
    def password(self):
        """
        user's password
        """
        return self.params.get('password', self.options.get('PASSWORD', None))

    @property
    def username(self):
        """
        user's username
        """
        return self.params.get('username', self.options.get('USERNAME', None))

    @property
    def params(self):
        """
        configuration params
        """
        return self._params or {}

    @property
    def timeout(self):
        """
        sets the ttl for the record. The timeout in django cache is equivalent
        to ttl in aerospike meta data for records. so TTL (Aerospike) == TIMEOUT (django).
        The default value changed to 10 s == 10000 ms
        """
        return self.params.get('TIMEOUT', 10000)

    @property
    def options(self):
        """
        The configuration options property.
        HOST - the host at which aerospike server is running
        PORT - the port at which aerospike server is running
        NAMESPACE - aerospike namespace name
        SET - aerospike set name
        TIMEOUT - the time at which the value gets expired, for aerospike its
        the ttl value
        """
        return self.params.get('OPTIONS', {
                'HOST': "127.0.0.1",
                'PORT': 3000,
                'NAMESPACE': "test",
                'SET': "cache",
                'BIN': "entry",
                'TIMEOUT': 10000,
            })

    @property
    def meta(self):
        """
        The meta data for the record. For now only setting the ttl value.
        """
        meta = {
            'ttl': 10000
        }
        return meta

    @meta.setter
    def meta(self, value):
        """
        The setter for the meta property
        """
        self.meta = value

    @property
    def policy(self):
        """
        The policy for the record. For now default is to to send the digest.
        """
        policy = {
            'key': aerospike.POLICY_KEY_DIGEST
        } # store the key along with the record
        return policy

    @property
    def aero_namespace(self):
        """
        The configured aerospike namespace to hold the cache.
        """
        return self.params.get('NAMESPACE', self.options.get('NAMESPACE', "test"))

    @property
    def aero_set(self):
        """
        The configured aerospike set to hold the cache.
        """
        return self.params.get('SET', self.options.get('SET', "cache"))

    @property
    def aero_bin(self):
        """
        The configured aerospike bin to hold the cache.
        """
        return self.params.get('BIN', self.options.get('BIN', "entry"))

    def make_key(self, key, version=None):
        """
        Constructs the aerospike key from given user key
        """
        ret_key = (self.aero_namespace, self.aero_set, key)
        return ret_key

    def add(self, key, value, timeout=DEFAULT_TIMEOUT, version=None):
        """
        Set a value in the cache if the key does not already exist. If
        timeout is given, that timeout will be used for the key; otherwise
        the default cache timeout will be used.

        Returns True if the value was stored, False otherwise.
        """
        aero_key = self.make_key(key, version=version)

        # the pickling is taken care by the client library, it detects data types
        # as integer or string or list or map or objects as blobs
        # but for function/class/tuple it has to manually convert to blob
        #http://stackoverflow.com/a/624948/119031 to check for function type
        #TODO - use bytearray(function/class/tuple) to serialize unsupported data types
        value_type = type(value)
        if not isinstance(value, (int, str, list, dict)):
            pickle_value = pickle.dumps(value)
            #now store it as an array
            #value = array('B', pickle_value).tostring()
            #aerospike python library does not recognize array so use bytearray
            value = bytearray( pickle_value)


        meta = {}
        #check if its int or long else use default
        if isinstance(timeout, int):
            meta['ttl'] = timeout
        else:
            meta['ttl'] = self.timeout

        #compose the value for the cache key
        record = {self.aero_bin: value}
        ret = self._client.put(aero_key, record, meta, self.policy)

        if ret == 0:
            return True
        return False

    def get(self, key, default=None, version=None):
        """
        Fetch a given key from the cache. If the key does not exist, return
        default, which itself defaults to None.
        """
        aero_key = self.make_key(key, version=version)

        try:
            (key, metadata, record) = self._client.get(aero_key,self.policy)
            if record is None:
                return default
            value = record[self.aero_bin]
            unpickled_value = self.unpickle(value)

            return unpickled_value
        except Exception as e:
            print("error: {0}".format(e), file=sys.stderr)
        return default

    def set(self, key, value, timeout=DEFAULT_TIMEOUT, version=None):
        """
        Set a value in the cache. It is similar to add
        """
        return self.add(key, value, timeout, version)

    def delete(self, key, version=None):
        """
        Delete a key from the cache, failing silently.
        """
        self._client.remove(self.make_key(key, version=version))

    def get_many(self, keys, version=None):
        """
        Fetch a bunch of keys from the cache. For certain backends (memcached,
        pgsql) this can be *much* faster when fetching multiple values.

        Returns a dict mapping each key in keys to its value. If the given
        key is missing, it will be missing from the response dict.
        """
        if not keys:
            return {}
        ret_data = {}
        # get list of keys using python map
        new_keys = list(map(lambda key: self.make_key(key,version), keys))

        #get dict of key,meta,rec
        records = self._client.get_many(new_keys)

        for key, value in records.iteritems():
            #extract aerospike record from returned tuple value,
            (aero_key, metadata, record) = value
            if record is None:
                continue
            #the python client library will unpickle the appropriate value
            unpickled_value = record[self.aero_bin]
            ret_data[key] = unpickled_value
        return ret_data

    def has_key(self, key, version=None):
        """
        Returns True if the key is in the cache and has not expired.
        """
        meta = None
        try:
            key, meta = self._client.exists(self.make_key(key, version=version))
        except Exception as eargs:
            print("error: {0}".format(eargs), file=sys.stderr)

        if meta == None:
            return False

        return True

    def incr(self, key, delta=1, version=None):
        """
        Add delta to value in the cache. If the key does not exist, raise a
        ValueError exception.
        """
        exists = self.has_key(key)
        if not exists:
            raise ValueError("Key '%s' not found" % key)
        try:
            aero_key = self.make_key(key, version=version)
            value = self._client.increment(aero_key, self.aero_bin, delta)
        except Exception as eargs:
            value = self.get(key) + delta
            self.set(key, value)
        return value

    def clear(self):
        """
        Remove *all* values from the cache at once.
        """

        #remove each record in the bin
        def callback(key_meta_bins_tuple):
            key = key_meta_bins_tuple[0]
            self._client.remove(key)

        scan_obj = self._client.scan(self.aero_namespace, self.aero_set)

        scan_obj.foreach(callback)

    def close(self, **kwargs):
        """
        closes the database connection
        """
        self._client.close(**kwargs)
        
    def unpickle(self, value):
        """
        Unpickles the given value, it is unpickled by client lib and therefore
        not unpickled here.
        """
        #if its byte array then unpickle else ignore
        if isinstance(value, bytearray):
            value = smart_bytes(value)
            return pickle.loads(value)
        return value
