from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from bs4 import BeautifulSoup
import re
import time
import logging

# Configure logging
log_file = r"D:\DU Training\Case Study-Zomato\Pipeline Scripts\Log\LOG-Pipeline.txt"
logging.basicConfig(filename=log_file, level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')



def coordinates_extractor(website_url):
    chrome_options = Options()
    chrome_options.add_argument('--ignore-ssl-errors=yes')
    chrome_options.add_argument('--ignore-certificate-errors')
    driver = webdriver.Chrome(options=chrome_options)
    
    try:
        # Open the webpage
        driver.get(website_url)

        # Wait for 5 seconds to allow the page to fully render
        time.sleep(5)

        # Get the page source after rendering
        page_source = driver.page_source

        # Parse the HTML content
        soup = BeautifulSoup(page_source, 'html.parser')

        # Find all <a> tags
        a_tags = soup.find_all("a")

        # Regular expression to extract coordinates from URL
        coord_pattern = re.compile(r'([-+]?\d*\.\d+,\s*[-+]?\d*\.\d+)')

        # Search for coordinates in <a> tags
        found_coordinates = set()
        for a_tag in a_tags:
            href = a_tag.get("href")
            if href and "https://www.google.com/maps/dir/" in href:
                # Extract coordinates from the URL using regular expression
                matches = re.findall(coord_pattern, href)
                if matches:
                    # Print the coordinates found
                    lat, lon = matches[0].split(',')
                    if (lat, lon) not in found_coordinates:
                        found_coordinates.add((lat, lon))
                        # print(lat,',',lon)
                        return lat, lon
                        # Stop after finding the first unique set of coordinates
    except Exception as e:
        logging.error(f"An error occurred: {e}")
    finally:
        # Close the WebDriver
        driver.quit()

    return None
