Cachy
======

Generic caching decorator

Usage
------
```
plain_download = requests.get

def save_json(fn, data):
    pass
def read_json(fn):
    return open(fn, "r").read()

download = Cachedf(plain_downoad, save_json, read_json, expire_time_secs=60)
result = download(filename, url, params={})
```