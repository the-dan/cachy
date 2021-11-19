import requests

from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry

DEFAULT_TIMEOUT = 5 # seconds

class TimeoutHTTPAdapter(HTTPAdapter):
    def __init__(self, *args, **kwargs):
        self.timeout = DEFAULT_TIMEOUT
        if "timeout" in kwargs:
            self.timeout = kwargs["timeout"]
            del kwargs["timeout"]
        super().__init__(*args, **kwargs)

    def send(self, request, **kwargs):
        timeout = kwargs.get("timeout")
        if timeout is None:
            kwargs["timeout"] = self.timeout
        return super().send(request, **kwargs)

def flappy_network(timeout = 2.5, retries = 3, backoff_factor = 1):
    retry_strategy = Retry(
        total=retries,
        status_forcelist=[429, 500, 502, 503, 504],
        method_whitelist=["HEAD", "GET", "OPTIONS"],
        backoff_factor = backoff_factor
    )
    adapter = TimeoutHTTPAdapter(timeout=timeout, max_retries=retry_strategy)

    http = requests.Session()
    http.mount("https://", adapter)
    http.mount("http://", adapter)

    return http
