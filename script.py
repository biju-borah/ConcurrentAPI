import requests
import json
url = "http://localhost:3000"
for _ in range(100):
    res = requests.get(url)
    print(res.status_code)
