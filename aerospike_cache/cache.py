"Aerospike cache module"
from __future__ import print_function
import time, sys

try:
    import cPickle as pickle
except ImportError:
    import pickle

try:
    import aerospike
except ImportError:
    raise InvalidCacheBackendError(
        "Aerospike cache backend requires the 'aerospike' library")

from django.core.cache.backends.base import BaseCache, DEFAULT_TIMEOUT
try:
    # Django 1.5+
    from django.utils.encoding import smart_text, smart_bytes
except ImportError:
    # older Django, thus definitely Python 2
    from django.utils.encoding import smart_unicode, smart_str
    smart_text = smart_unicode
    smart_bytes = smart_str

from pprint import pprint

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
                  "timeout": self.timeout # milliseconds
              }
          }
        
        self._client = aerospike.client(config)
        
        if self.username is None and self.password is None:
            self._client.connect()    
        else:
            self._client.connect(self.username, self.password)
        
    
    #for pickling
    def __getstate__(self):
        return {'params': self._params, 'server': self._server}

    def __setstate__(self, state):
        self._init(**state)
        
    @property
    def server(self):
        return self._server or "127.0.0.1:3000"

    @property
    def password(self):
        return self.params.get('password', self.options.get('PASSWORD', None))

    @property
    def username(self):
        return self.params.get('username', self.options.get('USERNAME', None))
    
    @property
    def params(self):
        return self._params or {}

    @property
    def timeout(self):
        return self.params.get('TIMEOUT', 1000)
    
    @property
    def options(self):
        return self.params.get('OPTIONS', {
                'HOST': "127.0.0.1",
                'PORT': 3000,
                'NAMESPACE': "test",
                'SET': "cache",   
                'BIN': "entry",   
            })
    
    @property
    def meta(self):
        meta = {
            'ttl': 0
        }
        return meta
    
    @meta.setter
    def meta(self, value):
        self.meta = value
    
    @property
    def policy(self):
        policy = {
            'key': aerospike.POLICY_KEY_DIGEST
        } # store the key along with the record
        return policy
    
    @property
    def aero_namespace(self):
        return self.params.get('NAMESPACE', self.options.get('NAMESPACE', "test"))    
    
    @property
    def aero_set(self):
        return self.params.get('SET', self.options.get('SET', "cache"))
    
    @property
    def aero_bin(self):
        return self.params.get('BIN', self.options.get('BIN', "entry"))
    
    def make_key(self, key, version=None):
        """Constructs the key
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
        
        pickle_value = pickle.dumps(value)
        
        meta = {}
        if isinstance(timeout, (int , long)):
            meta['ttl'] = timeout
        else:
            meta['ttl'] = self.timeout
        
        record = {self.aero_bin: pickle_value}
        ret = self._client.put(aero_key, record, meta, self.policy)

        (key, metadata, record) = self._client.get(aero_key,self.policy)
        
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
        new_keys = list(map(lambda key: self.make_key(key,version), keys))
        aero_keys = dict(zip(new_keys, keys))
        #get dict of key,meta,rec
        records = self._client.get_many(new_keys, {'timeout': self.timeout})

        for key, value in records.iteritems():
            (aero_key, metadata, record) = value
            if record is None:
                continue
            value = record[self.aero_bin]
            unpickled_value = self.unpickle(value)            
            ret_data[key] = unpickled_value
        return ret_data
        
    def has_key(self, key, version=None):
        """
        Returns True if the key is in the cache and has not expired.
        """
        meta = None
        try:
            key, meta = self._client.exists(self.make_key(key, version=version))
        except Exception, eargs:
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
        except Exception, eargs:
            value = self.get(key) + delta
            self.set(key, value)
        return value
    
    def clear(self):
        """Remove *all* values from the cache at once."""
        records = []

        #remove each record in the bin
        def callback( (key, meta, bins) ):
            self._client.remove(key)
            
        scan_obj = self._client.scan(self.aero_namespace, self.aero_set)

        scan_obj.foreach(callback)
        
    def unpickle(self, value):
        """
        Unpickles the given value.
        """
        value = smart_bytes(value)
        return pickle.loads(value)
    
    