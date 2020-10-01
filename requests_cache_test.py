import time

import requests
import requests_cache

requests_cache.install_cache('demo_cache')
for i in range(10):
    print(time.time(), requests.get('http://httpbin.org/delay/1'))
