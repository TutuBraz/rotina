import requests

url = "https://www.searchapi.io/api/v1/search"
params = {
  "engine": "google_news",
  "q": "Jeff Bezos news",
  "location": "New York,United States",
  "api_key": "V9Dgg9eLnrLqHvBvSgHdCCXj"
}

response = requests.get(url, params=params)
print(response.text)