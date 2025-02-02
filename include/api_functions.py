import requests
import logging
import pandas as pd

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Get the current/latest comic number from the XKCD API
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

# get comic data from a comic number
def get_comic_data(start_num = 0):
    all_comic_data = []
    current_comic_number = get_current_comic_number()
    if current_comic_number is not None:
        for num in range(start_num + 1, current_comic_number + 1):
            url = f"https://xkcd.com/{num}/info.0.json"
            response = requests.get(url)
            try:
                response.raise_for_status()
                comic_data = response.json()

                # define the schema
                filtered_data = {
                    "num": comic_data.get("num", None),
                    "title": comic_data.get("title", None),
                    "safe_title": comic_data.get("safe_title", None),
                    "alt": comic_data.get("alt", None),
                    "img": comic_data.get("img", None),
                    "year": comic_data.get("year", None),
                    "month": comic_data.get("month", None),
                    "day": comic_data.get("day", None),
                    "news": comic_data.get("news", None),
                    "link": comic_data.get("link", None),
                    "transcript": comic_data.get("transcript", None),
                    "extra_parts": comic_data.get("extra_parts", None)

                }
                
                all_comic_data.append(filtered_data)
                logging.info(f"Successfully retrieved data for comic #{num}")
            except requests.exceptions.RequestException as e:
                logging.warning(f"Failed to retrieve data for comic #{num}: {e}")
        return pd.DataFrame(all_comic_data)
    else:
        logging.error("Unable to fetch comic data. Exiting function.")
        return pd.DataFrame()