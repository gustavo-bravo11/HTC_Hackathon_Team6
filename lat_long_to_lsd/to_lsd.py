import requests
from bs4 import BeautifulSoup
import random

# List of free proxies (replace with fresh proxies from a proxy provider)
PROXIES = [
    "http://203.30.190.10:8080",
    "http://185.93.3.123:8080",
    "http://78.46.60.50:3128",
    "http://45.77.67.1:8080"
]

def get_lsd_from_website(latitude, longitude):
    url = f"https://legallandconverter.com/cgi-bin/shopats201703.cgi?cmd=reverse1&latitude={latitude}&longitude={longitude}&prg=Calc"

    # Pick a random proxy
    proxy = random.choice(PROXIES)
    proxies = {"http": proxy, "https": proxy}

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36"
    }

    try:
        response = requests.get(url, headers=headers, proxies=proxies, timeout=5)

        if response.status_code != 200:
            return f"Error fetching data: {response.status_code}"

        soup = BeautifulSoup(response.text, "html.parser")
        
        # Find LSD information inside <b> tags
        for tag in soup.find_all("b"):
            if "LSD" in tag.text:
                return tag.text.strip()

        return "LSD not found."
    
    except requests.exceptions.RequestException as e:
        return f"Request failed: {e}"

# Example usage
latitude = 52.14108
longitude = -112.15671

lsd_info = get_lsd_from_website(latitude, longitude)
print("Extracted LSD Info:", lsd_info)
