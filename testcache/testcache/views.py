from django.core.cache import get_cache
from django.http import HttpResponse


def home(request):
    #cache = get_cache('aerospike_cache.cache://127.0.0.1:3000')
    cache = get_cache('default')
    cache.set("foo", "bar")
    return HttpResponse("Pants")