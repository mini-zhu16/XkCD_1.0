import requests
import logging
import pandas as pd

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def get_current_comic_number():
    url = "https://xkcd.com/info.0.json"
    response = requests.get(url)
    if response.status_code == 200:
        comic_data = response.json()
        current_comic_number = comic_data["num"]
        return current_comic_number
    else:
        logging.error("Failed to retrieve data for current comic. Status code: %d", response.status_code)
        return None

def get_comic_data():
    all_comic_data = []
    current_comic_number = get_current_comic_number()
    if current_comic_number is not None:
        for num in range(1, current_comic_number+1):
            url = f"https://xkcd.com/{num}/info.0.json"
            response = requests.get(url)
            try:
                response.raise_for_status()
                comic_data = response.json()
                all_comic_data.append(comic_data)
                logging.info(f"Successfully retrieved data for comic #{num}")
            except requests.exceptions.RequestException as e:
                logging.warning(f"Failed to retrieve data for comic #{num}: {e}")
        return pd.DataFrame(all_comic_data)
    else:
        logging.error("Unable to fetch comic data. Exiting function.")
        return pd.DataFrame()