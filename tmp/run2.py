import csv
import time
import random
import os
import glob
from urllib.parse import quote_plus, urlparse, urlunparse
import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from selenium.webdriver.common.action_chains import ActionChains
import logging
import re
import spacy
from bs4 import BeautifulSoup
import google.generativeai as genai
from typing import List
import json
import base64

class CompanySalesScraper:
    def __init__(self, api_key: str):
        self.input_file = './data/input.csv'
        self.output_file = './data/output_executive.csv'

        # ログ設定
        logging.basicConfig(
            filename='sales_scraper.log',
            filemode='a',
            format='%(asctime)s - %(levelname)s - %(message)s',
            level=logging.DEBUG
        )
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.DEBUG)

        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.DEBUG)
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        console_handler.setFormatter(formatter)
        self.logger.addHandler(console_handler)

        self.logger.info("Initializing CompanySalesScraper...")
        self.clear_output_file()

        # Configure undetected-chromedriver options
        self.chrome_options = uc.ChromeOptions()
        
        # Basic settings
        self.chrome_options.add_argument('--headless')  # Note: using regular headless mode for undetected-chromedriver
        self.chrome_options.add_argument('--no-sandbox')
        self.chrome_options.add_argument('--window-size=1920,1080')
        self.chrome_options.add_argument('--start-maximized')
        
        # Language settings
        self.chrome_options.add_argument('--lang=ja-JP')
        self.chrome_options.add_argument('--accept-lang=ja-JP,ja;q=0.9,en;q=0.8')
        
        # Random user agent
        user_agents = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36 Edg/122.0.0.0",
        ]
        self.chrome_options.add_argument(f'user-agent={random.choice(user_agents)}')

        # Initialize remaining attributes
        if not os.path.exists(self.output_file):
            with open(self.output_file, 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerow(['会社名', '業種', '公式サイトURL', '年商'])
            print(f"アウトプットファイルを作成しました: {self.output_file}")
        
        if not os.path.exists(self.input_file):
            raise FileNotFoundError(f"入力ファイルが見つかりません: {self.input_file}")
        else:
            print(f"インプットファイルを確認しました: {self.input_file}")
        
        try:
            self.nlp = spacy.load("ja_core_news_sm")
        except OSError:
            print("spaCyの日本語モデルが見つかりません。以下のコマンドでインストールしてください:")
            print("python3 -m spacy download ja_core_news_sm")
            raise
        
        genai.configure(api_key=api_key)
        self.model = genai.GenerativeModel(
            "gemini-1.5-flash",
            generation_config={"response_mime_type": "application/json"},
        )

    def clear_output_file(self):
        """出力ファイルを初期化する"""
        try:
            with open(self.output_file, 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerow(['会社名', '業種', '公式サイトURL', '年商'])
            self.logger.info(f"Output file initialized: {self.output_file}")
            print(f"アウトプットファイルを初期化しました: {self.output_file}")
        except Exception as e:
            self.logger.error(f"Failed to initialize output file: {str(e)}")
            print(f"アウトプットファイル初期化エラー: {str(e)}")
    
    def inject_stealth_js(self, driver):
        """
        Inject stealth.min.js to make the browser more realistic
        """
        try:
            # Basic stealth script
            stealth_js = """
            Object.defineProperty(navigator, 'webdriver', {
                get: () => undefined
            });
            Object.defineProperty(navigator, 'plugins', {
                get: () => [1, 2, 3, 4, 5]
            });
            Object.defineProperty(navigator, 'languages', {
                get: () => ['ja-JP', 'ja', 'en-US', 'en']
            });
            window.chrome = {
                runtime: {}
            };
            """
            driver.execute_script(stealth_js)
            
            # Additional canvas fingerprint evasion
            canvas_js = """
            HTMLCanvasElement.prototype.toDataURL = function() {
                return 'data:image/png;base64,RANDOMDATA';
            };
            """
            driver.execute_script(canvas_js.replace('RANDOMDATA', base64.b64encode(os.urandom(32)).decode()))
            
            self.logger.debug("Stealth JS injected successfully")
        except Exception as e:
            self.logger.error(f"Failed to inject stealth JS: {str(e)}")

    def simulate_human_behavior(self, driver):
        """
        Simulate human-like mouse movements and interactions
        """
        try:
            actions = ActionChains(driver)
            
            # Random mouse movements
            for _ in range(random.randint(2, 4)):
                x = random.randint(100, 800)
                y = random.randint(100, 600)
                actions.move_by_offset(x, y)
                actions.pause(random.uniform(0.1, 0.3))
            
            # Random scrolls
            for _ in range(random.randint(1, 3)):
                scroll_amount = random.randint(100, 500)
                driver.execute_script(f"window.scrollTo(0, {scroll_amount})")
                time.sleep(random.uniform(0.5, 1))
            
            actions.perform()
            self.logger.debug("Human behavior simulation completed")
        except Exception as e:
            self.logger.error(f"Failed to simulate human behavior: {str(e)}")

    def scrape_company_data(self, driver, company_name):
        self.logger.debug(f"Starting scrape_company_data for {company_name}")
        try:
            # First, find the official website URL
            official_url = self.find_official_website(driver, company_name)
            if not official_url:
                self.logger.warning(f"Could not find official website for {company_name}")
                official_url = ""

            # Extract annual sales information
            annual_sales = self.extract_annual_sales(driver, company_name)
            self.logger.debug(f"Annual sales for {company_name}: {annual_sales}")

            # Extract industry information
            industry = "不明"  # Default value
            if official_url:
                try:
                    self.logger.debug(f"Attempting to access company website: {official_url}")
                    driver.get(official_url)
                    self.logger.debug("Successfully accessed company website")
                    time.sleep(5)  # Wait for page load
                    
                    # Extract page content
                    page_content = self.extract_cleaned_content(driver, official_url)
                    self.logger.debug(f"Page content length: {len(page_content)}")
                    
                    # Extract industry information
                    industry = self.extract_industry_info(page_content)
                    self.logger.debug(f"抽出された業種: {industry}")
                except Exception as e:
                    self.logger.error(f"Failed to extract industry information: {str(e)}")

            # Return the collected data
            return [company_name, industry, official_url, annual_sales]

        except Exception as e:
            self.logger.error(f"Failed to process company data: {str(e)}")
            return [company_name, "不明", "", ""]
    
    def extract_cleaned_content(self, driver, url):
        """
        Seleniumを使用してWebページの内容を取得し、不要な要素を除去して整形したテキストを返す関数
        """
        try:
            # URLにアクセス
            driver.get(url)
            
            # ページの読み込みを待機
            time.sleep(random.uniform(4, 8))
            
            # ページのHTML全体を取得
            html_content = driver.page_source
            
            # BeautifulSoupを使用して構造化されたコンテンツを抽出
            soup = BeautifulSoup(html_content, 'html.parser')
            
            # 標準的な処理：不要な要素を削除
            for tag in soup(['script', 'style', 'meta', 'link', 'noscript', 'svg']):
                tag.decompose()
                
            # 役員情報が含まれる可能性の高いセクションに優先度を置く
            priority_content = []
            
            # 役員情報を含む可能性の高いセクションを探す
            officer_keywords = ['役員', '取締役', '代表', '社長', '会長', 'CEO', '執行役員', '監査役', 'オフィサー', 'ディレクター']
            
            # テーブルから役員情報を抽出 (テーブルを最優先)
            for table in soup.find_all('table'):
                # テーブルのHTMLを保存して後で処理
                table_html = str(table)
                table_text = []
                for row in table.find_all('tr'):
                    cells = [cell.get_text().strip() for cell in row.find_all(['td', 'th'])]
                    if cells and any(keyword in ' '.join(cells).lower() for keyword in officer_keywords):
                        table_text.append(' | '.join(cells))
                
                if table_text:
                    # テーブルデータを文字列化
                    table_data = "\n".join(table_text)
                    # テーブルHTMLと文字列化データの両方を追加
                    priority_content.append(f"テーブルデータ:\n{table_data}\n\nテーブルHTML:\n{table_html}")
            
            # 見出し要素を探す
            for heading in soup.find_all(['h1', 'h2', 'h3', 'h4', 'h5', 'h6']):
                heading_text = heading.get_text().strip()
                if any(keyword in heading_text for keyword in officer_keywords):
                    # 見出しとその直後のコンテンツを抽出
                    next_elements = []
                    for sibling in heading.find_next_siblings():
                        if sibling.name in ['h1', 'h2', 'h3', 'h4', 'h5', 'h6']:
                            break
                        # HTMLを保存
                        next_elements.append(str(sibling))
                    
                    priority_content.append(f"見出し: {heading_text}\n\n" + "\n".join(next_elements))
            
            # セクションを探す
            for section in soup.find_all(['section', 'div', 'article']):
                section_class = section.get('class', [])
                section_id = section.get('id', '')
                section_text = section.get_text().strip().lower()
                
                # クラス名、ID、またはテキスト内容に役員関連のキーワードが含まれているセクションを探す
                if any(any(keyword in str(attr).lower() for keyword in officer_keywords) 
                       for attr in [section_class, section_id, section_text]):
                    # セクションのHTMLと平文の両方を保存
                    section_html = str(section)
                    section_plain = section.get_text().strip()
                    priority_content.append(f"セクションHTML:\n{section_html}\n\nセクション平文:\n{section_plain}")
            
            # DLリストから役員情報を抽出
            for dl in soup.find_all('dl'):
                dl_html = str(dl)
                dl_text = []
                dts = dl.find_all('dt')
                dds = dl.find_all('dd')
                
                if len(dts) == len(dds):  # dtとddの数が一致する場合のみ
                    for dt, dd in zip(dts, dds):
                        dt_text = dt.get_text().strip()
                        dd_text = dd.get_text().strip()
                        if any(keyword in dt_text.lower() for keyword in officer_keywords) or any(keyword in dd_text.lower() for keyword in officer_keywords):
                            dl_text.append(f"{dt_text}: {dd_text}")
                    
                    if dl_text:
                        priority_content.append(f"DLデータ:\n" + "\n".join(dl_text) + f"\n\nDL HTML:\n{dl_html}")
            
            # ULリストから役員情報を抽出
            for ul in soup.find_all('ul'):
                ul_html = str(ul)
                ul_items = []
                list_items = ul.find_all('li')
                
                # リスト内のテキストをチェック
                list_text = ul.get_text().lower()
                if any(keyword in list_text for keyword in officer_keywords):
                    for li in list_items:
                        ul_items.append(li.get_text().strip())
                    
                    if ul_items:
                        priority_content.append(f"リストアイテム:\n" + "\n".join(ul_items) + f"\n\nリストHTML:\n{ul_html}")
            
            # 優先コンテンツが見つかった場合はそれを使用
            if priority_content:
                extracted_text = "\n\n".join(priority_content)
            else:
                # 全体のテキストを取得
                extracted_text = soup.get_text()
            
            # 抽出テキストの先頭に、より効果的なプロンプトのためのヒントを追加
            prefixed_text = """
以下は企業の役員情報を含むウェブページからの抽出テキストです。役職と氏名のペアを特定してください。
役職には「代表取締役社長」「代表取締役会長」「取締役」「社外取締役」「監査役」などがあります。
特に注意: ウェブページにある氏名の漢字は正確にそのまま抽出してください。例えば「竹林 基哉」を「竹林 元也」に変更しないでください。

本文:
""" + extracted_text
            
            return prefixed_text
            
        except Exception as e:
            return f"Error occurred: {str(e)}"
    

    def write_company_data(self, company_data):
        """
        Write company data to the output file
        Args:
            company_data (list): List containing [company_name, industry, official_url, annual_sales]
        """
        try:
            with open(self.output_file, 'a', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerow(company_data)
                self.logger.debug(f"Wrote data for {company_data[0]}")
        except Exception as e:
            self.logger.error(f"Error writing company data: {str(e)}")

    def cleanup_screenshots(self):
        """スクリーンショットファイルを削除する"""
        try:
            # スクリーンショットファイルのパターンにマッチするファイルを検索
            screenshot_files = glob.glob("screenshot_*.png")
            if screenshot_files:
                for file in screenshot_files:
                    try:
                        os.remove(file)
                        self.logger.debug(f"Screenshot removed: {file}")
                    except Exception as e:
                        self.logger.warning(f"Failed to remove screenshot {file}: {str(e)}")
                
                self.logger.info(f"Removed {len(screenshot_files)} screenshot files")
                print(f"{len(screenshot_files)}件のスクリーンショットファイルを削除しました。")
        except Exception as e:
            self.logger.error(f"Error during screenshot cleanup: {str(e)}")
            print(f"スクリーンショット削除中にエラーが発生しました: {str(e)}")

    def extract_industry_info(self, page_content: str) -> str:
        """
        Extract industry information from the page content using LLM
        Args:
            page_content (str): The page content to analyze
        Returns:
            str: The industry type
        """
        prompt = f"""
以下の文脈から企業の業種を特定してください。業種は以下のような形式で返してください：

主な業種の例:
飲食店の場合:
- カフェ・コーヒーショップ
- 喫茶店
- レストラン（和食）
- レストラン（寿司）
- レストラン（天ぷら）
- レストラン（そば・うどん）
- レストラン（洋食）
- レストラン（イタリアン）
- レストラン（フレンチ）
- レストラン（中華）
- レストラン（韓国料理）
- レストラン（多国籍料理）
- ファストフード（ハンバーガー）
- ファストフード（その他）
- 居酒屋・バー
- 焼肉店
- 回転寿司
- ラーメン専門店
- うどん・そば専門店
- とんかつ専門店
- 牛丼・丼物専門店
- パン・ベーカリーカフェ
- スイーツ・デザート専門店
- デリバリー専門店

食品製造業の場合:
- 食品製造（調理済食品）
- 食品製造（冷凍食品）
- 食品製造（菓子・スイーツ）
- 食品製造（パン・製菓）
- 食品製造（調味料・食品添加物）
- 食品製造（乳製品・乳業）
- 食品製造（食肉加工）
- 食品製造（水産加工）
- 食品製造（農産加工）
- 飲料製造（清涼飲料）
- 飲料製造（アルコール）
- 飲料製造（コーヒー・茶）
- 食品商社

食品小売業の場合:
- スーパーマーケット
- 食品スーパー
- コンビニエンスストア
- 食品専門店
- 酒類専門店
- 青果専門店
- 精肉専門店
- 鮮魚専門店
- パン専門店
- 菓子専門店

その他主要業種:
- 専門店（衣料品）
- 専門店（家電）
- 専門店（医薬品）
- ホテル・旅館
- 不動産
- 金融・保険
- IT・通信
- 物流・運送

文脈:
{page_content}

出力形式:
{{
    "industry": "業種名"
}}

注意事項:
1. 文脈に明示的に記載されている情報のみを使用すること
2. 業種は上記の分類に基づいて、できるだけ具体的な業種を選択すること
3. 不明な場合は「不明」と返すこと
4. 複数の業種に該当する場合は、最も主要な業種を選択すること
5. 上記のカテゴリに完全に一致しない場合でも、最も近い具体的な業種を選択すること
"""
        try:
            response = self.model.generate_content(prompt)
            import json
            result = json.loads(response.text)
            return result.get("industry", "不明")
        except Exception as e:
            self.logger.error(f"業種抽出に失敗: {str(e)}")
            return "不明"

    def save_screenshot(self, driver, company_name: str, stage: str):
        """
        Save a screenshot of the current page
        Args:
            driver: Selenium WebDriver instance
            company_name (str): Company name for the filename
            stage (str): Stage of the process where screenshot was taken
        """
        try:
            # Create screenshots directory if it doesn't exist
            if not os.path.exists('screenshots'):
                os.makedirs('screenshots')
            
            # Clean the company name for filename
            safe_company_name = re.sub(r'[^\w\-_\.]', '_', company_name)
            timestamp = time.strftime("%Y%m%d-%H%M%S")
            filename = f"screenshots/screenshot_{safe_company_name}_{stage}_{timestamp}.png"
            
            # Save screenshot
            driver.save_screenshot(filename)
            self.logger.info(f"Screenshot saved: {filename}")
        except Exception as e:
            self.logger.error(f"Failed to save screenshot: {str(e)}")

    def find_official_website(self, driver, company_name: str) -> str:
        """
        Search for the company's official website URL using Yahoo Japan search
        """
        try:
            time.sleep(random.uniform(2, 4))
            
            search_query = f"{company_name} 公式サイト"
            # Use Yahoo Japan search instead of Google
            search_url = f"https://search.yahoo.co.jp/search?p={quote_plus(search_query)}"
            self.logger.debug(f"Searching for official website on Yahoo: {search_url}")
            
            max_retries = 3
            for attempt in range(max_retries):
                try:
                    driver.get(search_url)
                    self.save_screenshot(driver, company_name, f"search_attempt_{attempt + 1}")
                    
                    # Simulate human behavior
                    self.simulate_human_behavior(driver)
                    self.save_screenshot(driver, company_name, f"search_scrolled_{attempt + 1}")
                    
                    # Check for bot detection
                    if "automated access" in driver.page_source.lower() or "captcha" in driver.page_source.lower():
                        self.logger.warning(f"Bot detection triggered on attempt {attempt + 1}")
                        time.sleep(random.uniform(10, 20))
                        continue
                    
                    break
                except Exception as e:
                    if attempt == max_retries - 1:
                        raise
                    time.sleep(random.uniform(5, 10))

            # Yahoo Japan specific selectors
            selectors = [
                "div.sw-Card__title a",  # Main title links
                "div.Contents__inner a",  # Content links
                "h3.Title a",            # Title links
                ".js-LinkArea",          # Link areas
                "li.SearchResult a",     # Search result links
                "div.cf a",              # General links
                ".sw-Card__titleInner a" # Card title links
            ]
            random.shuffle(selectors)

            # Find search results
            search_results = None
            for selector in selectors:
                try:
                    time.sleep(random.uniform(0.5, 1))
                    WebDriverWait(driver, 5).until(
                        EC.presence_of_element_located((By.CSS_SELECTOR, selector))
                    )
                    search_results = driver.find_elements(By.CSS_SELECTOR, selector)
                    if search_results and len(search_results) > 0:
                        break
                except Exception as e:
                    self.logger.debug(f"Selector {selector} failed: {str(e)}")
                    continue

            if not search_results or len(search_results) == 0:
                self.logger.warning("No search results found for official website")
                return ""

            # Get the first result's URL that looks like an official website
            for result in search_results[:5]:  # Check first 5 results
                try:
                    url = result.get_attribute("href")
                    if not url:
                        continue
                        
                    # Clean the URL
                    clean_url = self.clean_url(url)
                    
                    # Skip obvious non-official websites
                    skip_domains = ['yahoo.co.jp', 'google.com', 'facebook.com', 'twitter.com', 
                                  'linkedin.com', 'youtube.com', 'instagram.com']
                    if any(domain in clean_url.lower() for domain in skip_domains):
                        continue
                        
                    # If URL contains company name or looks official, use it
                    company_keywords = company_name.lower().replace('株式会社', '').replace('有限会社', '').split()
                    if any(keyword in clean_url.lower() for keyword in company_keywords):
                        self.logger.debug(f"Found official website URL: {clean_url}")
                        time.sleep(random.uniform(1, 2))
                        return clean_url

                except Exception as e:
                    self.logger.debug(f"Failed to process result: {str(e)}")
                    continue

            # If no good match found, return first result
            try:
                first_url = search_results[0].get_attribute("href")
                if first_url:
                    clean_url = self.clean_url(first_url)
                    self.logger.debug(f"Using first result as website URL: {clean_url}")
                    return clean_url
            except Exception as e:
                self.logger.error(f"Failed to get first result URL: {str(e)}")

            return ""

        except Exception as e:
            self.logger.error(f"Error finding official website: {str(e)}")
            return ""

    def extract_annual_sales(self, driver, company_name: str) -> str:
        """
        Extract annual sales information from Google search results
        Args:
            driver: Selenium WebDriver instance
            company_name (str): Company name to search for
        Returns:
            str: Annual sales amount or empty string if not found
        """
        try:
            # Construct search query for annual sales
            search_query = f"{company_name} 年商"
            search_url = f"https://www.google.co.jp/search?q={quote_plus(search_query)}&hl=ja"
            self.logger.debug(f"Searching for annual sales: {search_url}")

            # Access Google search page
            driver.get(search_url)
            time.sleep(5)  # Wait for page load

            # Try to find AI Overview section
            ai_overview_selectors = [
                "div[data-attrid='kc:/business/business_operation:revenue']",
                "div[data-attrid='kc:/organization/organization:revenue']",
                "div.kp-wholepage",
                "div.osrp-blk",
                "div[data-hveid]",
                "div.Z0LcW",
                "div.IZ6rdc",
                "div.zloOqf"
            ]

            # Find AI Overview content
            overview_text = ""
            for selector in ai_overview_selectors:
                try:
                    elements = driver.find_elements(By.CSS_SELECTOR, selector)
                    for element in elements:
                        text = element.text
                        if "億" in text or "万円" in text:
                            overview_text += text + "\n"
                except Exception as e:
                    self.logger.debug(f"Selector {selector} failed: {str(e)}")
                    continue

            if not overview_text:
                self.logger.warning("No annual sales information found in AI Overview")
                return ""

            # Use LLM to extract the sales amount
            prompt = f"""
以下のテキストから最新の年商（売上高）を抽出してください。
金額は「億円」「万円」などの単位を含めて正確に抽出してください。

テキスト:
{overview_text}

出力形式:
{{
    "annual_sales": "金額（単位含む）"
}}

注意事項:
1. 最新の数値を優先すること
2. 数値と単位は正確に抽出すること（例: 772億9600万円）
3. 見つからない場合は空文字列を返すこと
"""
            try:
                response = self.model.generate_content(prompt)
                import json
                result = json.loads(response.text)
                sales_amount = result.get("annual_sales", "")
                self.logger.debug(f"Extracted annual sales: {sales_amount}")
                return sales_amount
            except Exception as e:
                self.logger.error(f"Failed to extract annual sales with LLM: {str(e)}")
                return ""

        except Exception as e:
            self.logger.error(f"Error extracting annual sales: {str(e)}")
            return ""

    def start_scraping(self):
        """
        Start the scraping process by reading from input file and processing each company
        """
        try:
            # Initialize undetected-chromedriver
            driver = uc.Chrome(options=self.chrome_options)
            self.logger.info("Chrome driver initialized successfully")

            # Read input file
            with open(self.input_file, 'r', encoding='utf-8') as f:
                reader = csv.reader(f)
                next(reader)  # Skip header row
                companies = list(reader)
                
                for index, row in enumerate(companies):
                    if not row:  # Skip empty rows
                        continue
                    company_name = row[0].strip()
                    if not company_name:  # Skip empty company names
                        continue

                    self.logger.info(f"Processing company {index + 1}/{len(companies)}: {company_name}")
                    
                    try:
                        # Scrape company data
                        company_data = self.scrape_company_data(driver, company_name)
                        
                        # Write the data to output file
                        self.write_company_data(company_data)
                        
                        # Add a random delay between companies
                        delay = random.uniform(3, 7)
                        self.logger.debug(f"Waiting {delay:.2f} seconds before next company")
                        time.sleep(delay)
                        
                    except Exception as e:
                        self.logger.error(f"Error processing company {company_name}: {str(e)}")
                        time.sleep(random.uniform(10, 15))
                        continue

            # Clean up
            driver.quit()
            self.cleanup_screenshots()
            self.logger.info("Scraping completed successfully")

        except Exception as e:
            self.logger.error(f"Error in start_scraping: {str(e)}")
            raise

    def clean_url(self, url: str) -> str:
        """
        Clean and normalize a URL
        Args:
            url (str): The URL to clean
        Returns:
            str: The cleaned URL
        """
        try:
            # Parse the URL
            parsed = urlparse(url)
            
            # Remove common tracking parameters
            query_params = {}
            for param in parsed.query.split('&'):
                if '=' in param:
                    key, value = param.split('=', 1)
                    if key not in ['utm_source', 'utm_medium', 'utm_campaign', 'utm_term', 'utm_content']:
                        query_params[key] = value
            
            # Reconstruct the URL
            cleaned_query = '&'.join(f"{k}={v}" for k, v in query_params.items())
            cleaned_url = urlunparse((
                parsed.scheme,
                parsed.netloc,
                parsed.path,
                parsed.params,
                cleaned_query,
                parsed.fragment
            ))
            
            return cleaned_url
        except Exception as e:
            self.logger.error(f"Error cleaning URL {url}: {str(e)}")
            return url

if __name__ == "__main__":
    GOOGLE_API_KEY = "AIzaSyAAZratHSyw71DkAyk_WHcUkwkXW-yksGk"
    scraper = CompanySalesScraper(api_key=GOOGLE_API_KEY)
    scraper.start_scraping()