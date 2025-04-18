import csv
import time
import random
import os
import glob
from urllib.parse import quote_plus, urlparse, urlunparse
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
import chromedriver_binary  # 必要に応じてインストール
import logging
import re
import spacy
from bs4 import BeautifulSoup
import google.generativeai as genai
from typing import List
from shutil import which

class CompanySalesScraper:
    def __init__(self, api_key: str):
        self.input_file = './data/input.csv'
        self.output_file = './data/output_executive.csv'

        # Create data directory if it doesn't exist
        os.makedirs('./data', exist_ok=True)

        # ログ設定
        logging.basicConfig(
            filename='sales_scraper.log',
            filemode='a',
            format='%(asctime)s - %(levelname)s - %(message)s',
            level=logging.DEBUG
        )
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.DEBUG)

        # コンソールにもログを出力
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.DEBUG)
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        console_handler.setFormatter(formatter)
        self.logger.addHandler(console_handler)

        self.logger.info("Initializing CompanySalesScraper...")

        # 出力ファイルを初期化
        self.clear_output_file()

        # Cleanup any previous Chrome user data directories
        try:
            import subprocess
            subprocess.run("rm -rf /tmp/chrome-data-*", shell=True)
            self.logger.debug("Cleaned up previous Chrome user data directories")
        except Exception as cleanup_e:
            self.logger.debug(f"Failed to clean up Chrome data dirs: {cleanup_e}")

        self.chrome_options = webdriver.ChromeOptions()
        # ヘッドレスモードを有効にする - Amazon Linuxではヘッドレスモードが必要
        self.chrome_options.add_argument('--headless=new')
        self.chrome_options.add_argument('--no-sandbox')
        self.chrome_options.add_argument('--disable-dev-shm-usage')
        self.chrome_options.add_argument('--disable-gpu')
        self.chrome_options.add_argument('--window-size=1920,1080')
        self.chrome_options.add_argument("--disable-blink-features=AutomationControlled")
        self.chrome_options.add_argument("--disable-translate")
        # Amazon Linux用の追加オプション
        self.chrome_options.add_argument("--disable-extensions")
        self.chrome_options.add_argument("--disable-setuid-sandbox")
        # Chromeのロケールを日本語に設定
        self.chrome_options.add_argument("--lang=ja-JP")
        self.chrome_options.add_argument("--accept-lang=ja-JP,ja;q=0.9,en;q=0.8")
        # 一意のユーザーデータディレクトリ
        unique_dir = f"/tmp/chrome-data-{int(time.time())}-{random.randint(1, 100000)}"
        self.chrome_options.add_argument(f"--user-data-dir={unique_dir}")
        self.chrome_options.add_experimental_option('prefs', {
            'translate_whitelists': {},
            'translate': {'enabled': False},
            'intl.accept_languages': 'ja-JP,ja,en-US,en',
        })
        # User-Agentをより一般的なものに変更
        self.chrome_options.add_argument(
            "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/135.0.0.0 Safari/537.36"
        )

        # Check Chrome and ChromeDriver versions
        try:
            chrome_version = os.popen('google-chrome --version').read().strip()
            chromedriver_version = os.popen('chromedriver --version').read().strip()
            self.logger.info(f"Chrome version: {chrome_version}")
            self.logger.info(f"ChromeDriver version: {chromedriver_version}")
        except Exception as e:
            self.logger.error(f"Failed to get Chrome versions: {str(e)}")

        # Check if chromedriver is in PATH
        chromedriver_path = which('chromedriver')
        if chromedriver_path:
            self.logger.info(f"ChromeDriver found at: {chromedriver_path}")
        else:
            self.logger.warning("ChromeDriver not found in PATH")

        if not os.path.exists(self.output_file):
            with open(self.output_file, 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerow(['会社名', '業種', '公式サイトURL', '年商'])
            print(f"アウトプットファイルを作成しました: {self.output_file}")
        
        if not os.path.exists(self.input_file):
            raise FileNotFoundError(f"入力ファイルが見つかりません: {self.input_file}")
        else:
            print(f"インプットファイルを確認しました: {self.input_file}")
        
        # spaCyの日本語モデルをロード
        try:
            self.nlp = spacy.load("ja_core_news_sm")
        except OSError:
            print("spaCyの日本語モデルが見つかりません。以下のコマンドでインストールしてください:")
            print("python3 -m spacy download ja_core_news_sm")
            raise
        
        # Geminiの設定
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
    
        
    def scrape_company_data(self, driver, company_name):
        self.logger.debug(f"Starting scrape_company_data for {company_name}")
        try:
            # 検索クエリの構築
            search_query = f"{company_name} 役員"
            search_url = f"https://www.google.co.jp/search?q={quote_plus(search_query)}&hl=ja"
            self.logger.debug(f"Search URL constructed: {search_url}")

            # Google検索ページにアクセス
            try:
                self.logger.debug("Attempting to access Google search page...")
                driver.get(search_url)
                self.logger.debug("Successfully accessed Google search page")
                time.sleep(5)  # Amazon Linuxでは待機時間を長めに設定
                
                # Taking screenshot for debugging
                try:
                    screenshot_path = f"screenshot_{company_name}.png"
                    driver.save_screenshot(screenshot_path)
                    self.logger.debug(f"Saved screenshot to {screenshot_path}")
                except:
                    self.logger.debug("Failed to save screenshot")
                    
            except Exception as e:
                self.logger.error(f"Failed to access Google search page: {str(e)}")
                return [company_name, "取得失敗", f"エラー: 検索ページへのアクセスに失敗: {str(e)}", "エラー"]

            # 初期化
            company_url = None
            executives_data = []
            
            # 検索結果からURLを取得
            try:
                self.logger.debug("Attempting to find search results...")
                # 複数のセレクタを試す
                selectors = [
                    "div.g",  # 従来のセレクタ
                    "div[data-sokoban-container]",  # 新しいセレクタ
                    "div.tF2Cxc",  # 別の新しいセレクタ
                    "div.yuRUbf",  # 別の新しいセレクタ
                    "#search .g",  # 別の一般的なセレクタ
                    ".rc",  # 以前使用されていたセレクタ
                    "div.hlcw0c",  # モバイルビュー用セレクタ
                    "div.MjjYud",  # 新しい構造用
                    "h3.LC20lb"  # 見出しからの検索
                ]
                
                search_results = None
                for selector in selectors:
                    try:
                        self.logger.debug(f"Trying selector: {selector}")
                        # Amazon Linuxでは明示的待機を追加
                        WebDriverWait(driver, 3).until(
                            EC.presence_of_element_located((By.CSS_SELECTOR, selector))
                        )
                        search_results = driver.find_elements(By.CSS_SELECTOR, selector)
                        if search_results and len(search_results) > 0:
                            self.logger.debug(f"Found search results using selector: {selector}")
                            self.logger.debug(f"Number of results found: {len(search_results)}")
                            break
                    except Exception as e:
                        self.logger.debug(f"Selector {selector} failed: {str(e)}")
                        continue

                if not search_results or len(search_results) == 0:
                    self.logger.warning("No search results found with any selector")
                    # Amazon Linuxでは会社名から直接URLを推測することで代替
                    company_simple_name = company_name.split()[0].lower()
                    for ja, en in {"スターバックス": "starbucks", "ファーストリテイリング": "fastretailing", "コメダ": "komeda", "はま寿司": "hamazushi"}.items():
                        if ja in company_name:
                            company_simple_name = en
                            break
                    company_url = f"https://www.{company_simple_name}.co.jp/"
                    self.logger.debug(f"Falling back to guessed URL: {company_url}")
                else:
                    # 最初の検索結果のURLを取得
                    try:
                        self.logger.debug("Attempting to get first search result...")
                        first_result = search_results[0]
                        self.logger.debug("Got first search result")
                        
                        # 複数のリンクセレクタを試す
                        link_selectors = ["a", "a[href]", "a[jsname='UWckNb']"]
                        link = None
                        for link_selector in link_selectors:
                            try:
                                self.logger.debug(f"Trying link selector: {link_selector}")
                                link = first_result.find_element(By.CSS_SELECTOR, link_selector)
                                if link:
                                    self.logger.debug(f"Found link using selector: {link_selector}")
                                    break
                            except Exception as e:
                                self.logger.debug(f"Link selector {link_selector} failed: {str(e)}")
                                continue

                        if not link:
                            self.logger.error("No link element found in search result")
                            # Amazon Linux用の代替手段
                            a_elements = first_result.find_elements(By.TAG_NAME, "a")
                            if a_elements:
                                link = a_elements[0]
                                self.logger.debug("Found link using alternative method (direct a tag)")
                            else:
                                raise Exception("リンク要素が見つかりません")

                        company_url = link.get_attribute("href")
                        if not company_url:
                            self.logger.error("No URL found in link element")
                            raise Exception("URLが取得できません")

                        self.logger.debug(f"Successfully extracted company URL: {company_url}")
                    except Exception as e:
                        self.logger.error(f"Failed to extract company URL: {str(e)}")
                        return [company_name, "取得失敗", f"エラー: URLの取得に失敗: {str(e)}", "エラー"]
            except Exception as e:
                self.logger.error(f"Failed to process search results: {str(e)}")
                return [company_name, "取得失敗", f"エラー: 検索結果の処理に失敗: {str(e)}", "エラー"]

            # Extract annual sales information
            annual_sales = self.extract_annual_sales(driver, company_name)
            self.logger.debug(f"Annual sales for {company_name}: {annual_sales}")

            # Extract industry information
            industry = "不明"  # Default value
            if company_url:
                try:
                    self.logger.debug(f"Attempting to access company website: {company_url}")
                    driver.get(company_url)
                    self.logger.debug("Successfully accessed company website")
                    time.sleep(5)  # Wait for page load
                    
                    # Extract page content
                    page_content = self.extract_cleaned_content(driver, company_url)
                    self.logger.debug(f"Page content length: {len(page_content)}")
                    
                    # Extract industry information
                    industry = self.extract_industry_info(page_content)
                    self.logger.debug(f"抽出された業種: {industry}")
                except Exception as e:
                    self.logger.error(f"Failed to extract industry information: {str(e)}")

            # Return the collected data
            return [company_name, industry, company_url, annual_sales]

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

    def take_screenshot(self, driver, name):
        """Take a screenshot with timestamp"""
        try:
            timestamp = time.strftime("%Y%m%d-%H%M%S")
            filename = f"screenshot_{name}_{timestamp}.png"
            driver.save_screenshot(filename)
            self.logger.info(f"Screenshot saved: {filename}")
        except Exception as e:
            self.logger.error(f"Failed to take screenshot: {str(e)}")

    def search_with_retry(self, driver, url, max_retries=3):
        """
        Perform a search with advanced anti-detection measures
        """
        for attempt in range(max_retries):
            try:
                # Initial random delay
                time.sleep(random.uniform(5, 10))
                
                # Simulate human-like pre-navigation behavior
                self.simulate_human_behavior(driver)
                
                # Navigate to the URL with random timing
                parsed_url = urlparse(url)
                base_url = f"{parsed_url.scheme}://{parsed_url.netloc}"
                
                # First navigate to the base URL
                driver.get(base_url)
                time.sleep(random.uniform(2, 4))
                
                # Simulate some random interactions
                self.simulate_random_interactions(driver)
                
                # Then navigate to the full URL
                driver.get(url)
                
                # Random wait after page load
                time.sleep(random.uniform(3, 7))
                
                # Take screenshot for debugging
                self.take_screenshot(driver, f"search_attempt_{attempt}")
                
                # Check for bot detection
                page_source = driver.page_source.lower()
                if any(term in page_source for term in [
                    'sorry/index', 'captcha', 'unusual traffic', 
                    'automated queries', 'automated access', 
                    'security check', 'verify you are a human'
                ]):
                    self.logger.warning(f"Bot detection triggered on attempt {attempt + 1}")
                    if attempt < max_retries - 1:
                        # Exponential backoff with randomization
                        wait_time = (2 ** attempt) * 30 + random.uniform(15, 30)
                        self.logger.info(f"Waiting {wait_time:.2f} seconds before retry...")
                        time.sleep(wait_time)
                        continue
                    return False
                
                # Simulate post-load human behavior
                self.simulate_post_load_behavior(driver)
                return True
                
            except Exception as e:
                self.logger.warning(f"Search attempt {attempt + 1} failed: {str(e)}")
                if attempt < max_retries - 1:
                    wait_time = (2 ** attempt) * 20 + random.uniform(10, 20)
                    self.logger.info(f"Waiting {wait_time:.2f} seconds before retry...")
                    time.sleep(wait_time)
                else:
                    self.logger.error("All search attempts failed")
                    return False

    def simulate_human_behavior(self, driver):
        """Simulate realistic human-like behavior"""
        try:
            # Random scroll behavior
            scroll_positions = [100, 300, 500, 700]
            for pos in random.sample(scroll_positions, random.randint(2, 4)):
                driver.execute_script(f"window.scrollTo(0, {pos})")
                time.sleep(random.uniform(0.5, 1.5))
                
            # Random mouse movements
            for _ in range(random.randint(3, 6)):
                x = random.randint(100, 800)
                y = random.randint(100, 600)
                driver.execute_script("""
                    var event = new MouseEvent('mousemove', {
                        'view': window,
                        'bubbles': true,
                        'cancelable': true,
                        'clientX': arguments[0],
                        'clientY': arguments[1]
                    });
                    document.dispatchEvent(event);
                """, x, y)
                time.sleep(random.uniform(0.1, 0.3))
                
        except Exception as e:
            self.logger.debug(f"Error in simulate_human_behavior: {str(e)}")

    def simulate_random_interactions(self, driver):
        """Simulate random page interactions"""
        try:
            # Random viewport changes
            viewports = [
                (1920, 1080),
                (1366, 768),
                (1440, 900),
                (1536, 864)
            ]
            viewport = random.choice(viewports)
            driver.execute_script(f"window.resizeTo({viewport[0]}, {viewport[1]});")
            
            # Random focus/blur events
            driver.execute_script("""
                window.dispatchEvent(new Event('focus'));
                setTimeout(() => window.dispatchEvent(new Event('blur')), 500);
            """)
            
            # Simulate random key events
            key_events = ['keyup', 'keydown']
            for _ in range(random.randint(2, 4)):
                event = random.choice(key_events)
                key_code = random.randint(65, 90)  # A-Z
                driver.execute_script(f"""
                    document.dispatchEvent(new KeyboardEvent('{event}', {{
                        'keyCode': {key_code},
                        'which': {key_code},
                        'bubbles': true,
                        'cancelable': true
                    }}));
                """)
                time.sleep(random.uniform(0.1, 0.3))
                
        except Exception as e:
            self.logger.debug(f"Error in simulate_random_interactions: {str(e)}")

    def simulate_post_load_behavior(self, driver):
        """Simulate post-page-load behavior"""
        try:
            # Natural scrolling pattern
            total_height = driver.execute_script("return document.body.scrollHeight")
            view_height = driver.execute_script("return window.innerHeight")
            scroll_steps = range(0, total_height, int(view_height * 0.7))
            
            for pos in scroll_steps:
                driver.execute_script(f"window.scrollTo(0, {pos})")
                time.sleep(random.uniform(0.5, 1.0))
                
                # Random pauses during scrolling
                if random.random() < 0.3:
                    time.sleep(random.uniform(1, 2))
            
            # Scroll back to top with variable speed
            current_pos = total_height
            while current_pos > 0:
                step = random.randint(100, 300)
                current_pos = max(0, current_pos - step)
                driver.execute_script(f"window.scrollTo(0, {current_pos})")
                time.sleep(random.uniform(0.1, 0.3))
            
            # Random mouse movements near search results
            for _ in range(random.randint(2, 4)):
                x = random.randint(300, 700)
                y = random.randint(200, 500)
                driver.execute_script("""
                    var event = new MouseEvent('mousemove', {
                        'view': window,
                        'bubbles': true,
                        'cancelable': true,
                        'clientX': arguments[0],
                        'clientY': arguments[1]
                    });
                    document.dispatchEvent(event);
                """, x, y)
                time.sleep(random.uniform(0.2, 0.5))
                
        except Exception as e:
            self.logger.debug(f"Error in simulate_post_load_behavior: {str(e)}")

    def find_official_website(self, driver, company_name: str) -> str:
        """
        Search for the company's official website URL using multiple search engines
        Args:
            driver: Selenium WebDriver instance
            company_name (str): Company name to search for
        Returns:
            str: Official website URL or empty string if not found
        """
        try:
            # List of search engines to try
            search_engines = [
                {
                    'name': 'Yahoo Japan',
                    'url': lambda q: f"https://search.yahoo.co.jp/search?p={quote_plus(q)}&ei=UTF-8",
                    'selectors': [
                        "div.sw-Card__title a",
                        "div.Contents__inner a",
                        "h3.Title a"
                    ]
                },
                {
                    'name': 'Google',
                    'url': lambda q: f"https://www.google.co.jp/search?q={quote_plus(q)}&hl=ja",
                    'selectors': [
                        "div.g div.yuRUbf a",
                        "div[jscontroller] a",
                        "div.g a"
                    ]
                }
            ]

            # Construct search query for official website
            search_query = f"{company_name} 公式サイト"
            self.logger.debug(f"Searching for official website with query: {search_query}")

            for engine in search_engines:
                try:
                    search_url = engine['url'](search_query)
                    self.logger.info(f"Trying {engine['name']}: {search_url}")

                    # Take screenshot before search
                    self.take_screenshot(driver, f"before_{engine['name'].lower()}_{company_name}")

                    # Access search page with retry mechanism
                    if not self.search_with_retry(driver, search_url):
                        continue

                    # Try multiple selectors
                    for selector in engine['selectors']:
                        try:
                            WebDriverWait(driver, 5).until(
                                EC.presence_of_element_located((By.CSS_SELECTOR, selector))
                            )
                            links = driver.find_elements(By.CSS_SELECTOR, selector)
                            
                            if links:
                                for link in links:
                                    try:
                                        href = link.get_attribute("href")
                                        if not href:
                                            continue
                                            
                                        # Skip search engine cached pages and other unwanted URLs
                                        if any(skip in href.lower() for skip in [
                                            'webcache.googleusercontent.com',
                                            'translate.google',
                                            'search?q=',
                                            '/search?',
                                            'facebook.com',
                                            'twitter.com',
                                            'linkedin.com',
                                            'youtube.com'
                                        ]):
                                            continue

                                        # Check if URL contains company name (with some flexibility)
                                        company_parts = company_name.lower().replace('株式会社', '').split()
                                        url_lower = href.lower()
                                        
                                        # If URL contains significant part of company name, it's likely official
                                        if any(len(part) > 2 and part in url_lower for part in company_parts):
                                            clean_url = self.clean_url(href)
                                            self.logger.info(f"Found potential official website: {clean_url}")
                                            return clean_url

                                    except Exception as e:
                                        self.logger.debug(f"Error processing link: {str(e)}")
                                        continue

                        except Exception as e:
                            self.logger.debug(f"Selector {selector} failed: {str(e)}")
                            continue

                    # Add delay between search engines
                    time.sleep(random.uniform(10, 15))

                except Exception as e:
                    self.logger.error(f"Error with {engine['name']}: {str(e)}")
                    continue

            self.logger.warning(f"No official website found for {company_name}")
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
        try:
            print(f"出力ファイル: {os.path.abspath(self.output_file)}")
            print("スクレイピングを開始します...")
            
            # Cleanup any previous Chrome user data directories
            try:
                import subprocess
                subprocess.run("rm -rf /tmp/chrome-data-*", shell=True)
                self.logger.debug("Cleaned up previous Chrome user data directories")
            except Exception as cleanup_e:
                self.logger.debug(f"Failed to clean up Chrome data dirs: {cleanup_e}")
            
            with webdriver.Chrome(options=self.chrome_options) as driver, \
                 open(self.input_file, 'r', encoding='utf-8') as f_in:
                # Set page load timeout
                driver.set_page_load_timeout(30)
                driver.implicitly_wait(10)

                reader = csv.reader(f_in)
                header = next(reader, None)  # ヘッダーをスキップ
                print("インプットファイルのヘッダーをスキップしました。")

                for row in reader:
                    if row:
                        company_name = row[0].strip()
                        print(f"\n会社名: {company_name} のスクレイピングを開始します。")
                        
                        try:
                            # Take screenshot before processing
                            try:
                                screenshot_path = f"screenshot_before_{company_name}_{time.strftime('%Y%m%d-%H%M%S')}.png"
                                driver.save_screenshot(screenshot_path)
                                self.logger.debug(f"Saved before screenshot to {screenshot_path}")
                            except Exception as ss_e:
                                self.logger.debug(f"Failed to save before screenshot: {ss_e}")
                            
                            # Scrape company data
                            company_data = self.scrape_company_data(driver, company_name)
                            
                            # Take screenshot after processing
                            try:
                                screenshot_path = f"screenshot_after_{company_name}_{time.strftime('%Y%m%d-%H%M%S')}.png"
                                driver.save_screenshot(screenshot_path)
                                self.logger.debug(f"Saved after screenshot to {screenshot_path}")
                            except Exception as ss_e:
                                self.logger.debug(f"Failed to save after screenshot: {ss_e}")
                            
                            # Write the data
                            if company_data:
                                self.write_company_data(company_data)
                            else:
                                self.write_company_data([company_name, "取得失敗", "情報取得失敗", "情報なし"])
                            
                            # Add a longer delay between companies (4-8 seconds)
                            time.sleep(random.uniform(4, 8))
                            
                        except Exception as e:
                            self.logger.error(f"Error processing company {company_name}: {str(e)}")
                            # Take error screenshot
                            try:
                                screenshot_path = f"screenshot_error_{company_name}_{time.strftime('%Y%m%d-%H%M%S')}.png"
                                driver.save_screenshot(screenshot_path)
                                self.logger.debug(f"Saved error screenshot to {screenshot_path}")
                            except Exception as ss_e:
                                self.logger.debug(f"Failed to save error screenshot: {ss_e}")
                            
                            # Write error data
                            self.write_company_data([company_name, "取得失敗", f"エラー: {str(e)}", "エラー"])
                            continue
                            
        except Exception as e:
            logging.critical(f"スクレイピングが予期せず終了しました: {str(e)}")
            print(f"エラーが発生しました: {str(e)}")
        finally:
            # スクリーンショットファイルを削除
            self.cleanup_screenshots()
            
            logging.info("スクレイピングプロセスが完了しました。")
            print(f"\nスクレイピングが完了しました。結果は {os.path.abspath(self.output_file)} に保存されています。")

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