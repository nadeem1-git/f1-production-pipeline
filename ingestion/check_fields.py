import requests

for ep in ['pit', 'stints', 'weather', 'race_control', 'team_radio']:
    r = requests.get(f'https://api.openf1.org/v1/{ep}', params={'session_key': 9662})
    data = r.json()
    print(ep, '->', data[0] if data else 'EMPTY')
    print()