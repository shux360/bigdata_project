import json, urllib.request
for url in ["http://localhost:8000/health","http://localhost:8000/api/v1/grid/live","http://localhost:8000/metrics"]:
    with urllib.request.urlopen(url,timeout=5) as response:
        body=response.read().decode(); print(url,response.status,body[:400])

