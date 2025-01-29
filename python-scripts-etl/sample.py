import requests

url = "https://cricbuzz-cricket.p.rapidapi.com/stats/v1/rankings/batsmen"

headers = {
    "x-rapidapi-key": "78cfa35503msh8b182bf143082e8p133d40jsn6758f1621277",
    "x-rapidapi-host": "cricbuzz-cricket.p.rapidapi.com"
}
params = {
    'formatType': 'odi'
}

response = requests.get(url, headers=headers, params=params)

print("Status Code:", response.status_code)
print("Response JSON:", response.json())  # Print full response
