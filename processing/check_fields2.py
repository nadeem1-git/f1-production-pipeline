import requests

for ep, params in [("meetings", {"year": 2024}), ("drivers", {"session_key": 9662})]:
    r = requests.get(f"https://api.openf1.org/v1/{ep}", params=params)
    data = r.json()
    print(ep, "->", data[0] if data else "EMPTY")
    print()