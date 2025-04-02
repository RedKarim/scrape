import csv
import time
import random
import os
from urllib.parse import quote_plus
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
import chromedriver_binary

class CompanyWebsiteScraper:
    def __init__(self):
        self.input_file = 'data/needHP.csv'
        self.output_file = 'data/output_needHP.csv'
        self.debug_dir = 'debug'
        if not os.path.exists(self.debug_dir):
            os.makedirs(self.debug_dir)
        
        self.chrome_options = webdriver.ChromeOptions()
        self.chrome_options.add_argument('--headless')
        self.chrome_options.add_argument('--no-sandbox')
        self.chrome_options.add_argument('--disable-dev-shm-usage')
        if not os.path.exists(self.output_file):
            with open(self.output_file, 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerow(['Company Name', 'Official Website'])

    def start_scraping(self):
        with webdriver.Chrome(options=self.chrome_options) as driver:
            with open(self.input_file, 'r', encoding='utf-8') as f:
                reader = csv.reader(f)
                next(reader, None)
                for row in reader:
                    if row:
                        company_name = row[0]
                        self.scrape_company(driver, company_name)
                        time.sleep(1)
    def scrape_company(self, driver, company_name):
        search_query = f"{company_name} 公式サイト"
        search_url = f"https://www.google.com/search?q={quote_plus(search_query)}"
        
        print(f"Searching URL: {search_url}")
        
        driver.get(search_url)
        time.sleep(random.randint(4, 8))
        
        # debug_file = os.path.join(self.debug_dir, f"{company_name.replace(' ', '_')}.html")
        # with open(debug_file, 'w', encoding='utf-8') as f:
        #     f.write(driver.page_source)
        
        try:
            # XPath
            full_xpath = "/html/body/div[3]/div/div[13]/div/div[2]/div[2]/div/div/div[1]/div/div/div/div[1]/div/div/span/a"
            
            general_xpath = "//div[@class='yuRUbf']//a"
            
            official_website = None
            for xpath in [full_xpath, general_xpath]:
                try:
                    official_website = WebDriverWait(driver, 5).until(
                        EC.presence_of_element_located((By.XPATH, xpath))
                    ).get_attribute('href')
                    if official_website:
                        break
                except:
                    continue
            
            if not official_website:
                raise NoSuchElementException("No matching elements found")

        except (TimeoutException, NoSuchElementException) as e:
            print(f"Error extracting URL for {company_name}: {str(e)}")
            official_website = "Not found"
        
        print(f"Company: {company_name}")
        print(f"URL: {official_website}")
        # print(f"Debug file: {debug_file}")
        
        with open(self.output_file, 'a', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow([company_name, official_website])

if __name__ == "__main__":
    scraper = CompanyWebsiteScraper()
    scraper.start_scraping()
    print(f"Scraping completed. Results saved to {scraper.output_file}")