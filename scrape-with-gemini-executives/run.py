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
import chromedriver_binary  # 必要に応じてインストール
import logging
import re
import spacy
from bs4 import BeautifulSoup
import google.generativeai as genai
from typing import List

class CompanySalesScraper:
    def __init__(self, api_key: str):
        self.input_file = './data/input.csv'
        self.output_file = './data/output_トリドール.csv'

        # ログ設定
        logging.basicConfig(
            filename='sales_scraper.log',
            filemode='a',
            format='%(asctime)s - %(levelname)s - %(message)s',
            level=logging.DEBUG  # Changed to DEBUG level for more detailed logging
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
        
        self.chrome_options = webdriver.ChromeOptions()
        # ヘッドレスモードを有効にする場合はコメントを外してください
        # self.chrome_options.add_argument('--headless=new')  # Commented out headless mode
        self.chrome_options.add_argument('--no-sandbox')
        self.chrome_options.add_argument('--disable-dev-shm-usage')
        self.chrome_options.add_argument('--disable-gpu')
        self.chrome_options.add_argument('--window-size=1920,1080')
        self.chrome_options.add_argument("--disable-blink-features=AutomationControlled")  # Added to avoid detection
        self.chrome_options.add_argument(
            "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/135.0.0.0 Safari/537.36"
        )

        if not os.path.exists(self.output_file):
            with open(self.output_file, 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerow(['会社名', 'URL', '役職', '氏名'])
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
                writer.writerow(['会社名', 'URL', '役職', '氏名'])
            self.logger.info(f"Output file initialized: {self.output_file}")
            print(f"アウトプットファイルを初期化しました: {self.output_file}")
        except Exception as e:
            self.logger.error(f"Failed to initialize output file: {str(e)}")
            print(f"アウトプットファイル初期化エラー: {str(e)}")
    
    def start_scraping(self):
        try:
            print(f"出力ファイル: {os.path.abspath(self.output_file)}")
            print("スクレイピングを開始します...")
            
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
                        company_data = self.scrape_company_data(driver, company_name)
                        if company_data:
                            self.write_company_data([company_data])
                        # ランダムな遅延（5〜10秒）
                        time.sleep(random.uniform(4, 8))
        except Exception as e:
            logging.critical(f"スクレイピングが予期せず終了しました: {str(e)}")
            print(f"エラーが発生しました: {str(e)}")
        finally:
            logging.info("スクレイピングプロセスが完了しました。")
            print(f"\nスクレイピングが完了しました。結果は {os.path.abspath(self.output_file)} に保存されています。")
        
    def scrape_company_data(self, driver, company_name):
        self.logger.debug(f"Starting scrape_company_data for {company_name}")
        try:
            # 検索クエリの構築
            search_query = f"{company_name} 役員"
            search_url = f"https://www.google.com/search?q={quote_plus(search_query)}"
            self.logger.debug(f"Search URL constructed: {search_url}")

            # Google検索ページにアクセス
            try:
                self.logger.debug("Attempting to access Google search page...")
                driver.get(search_url)
                self.logger.debug("Successfully accessed Google search page")
                time.sleep(3)  # Increased wait time
                
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

            # 検索結果からURLを取得
            try:
                self.logger.debug("Attempting to find search results...")
                # 複数のセレクタを試す
                selectors = [
                    "div.g",  # 従来のセレクタ
                    "div[data-sokoban-container]",  # 新しいセレクタ
                    "div.tF2Cxc",  # 別の新しいセレクタ
                    "div.yuRUbf"  # 別の新しいセレクタ
                ]
                
                search_results = None
                for selector in selectors:
                    try:
                        self.logger.debug(f"Trying selector: {selector}")
                        search_results = driver.find_elements(By.CSS_SELECTOR, selector)
                        if search_results:
                            self.logger.debug(f"Found search results using selector: {selector}")
                            self.logger.debug(f"Number of results found: {len(search_results)}")
                            break
                    except Exception as e:
                        self.logger.debug(f"Selector {selector} failed: {str(e)}")
                        continue

                if not search_results:
                    self.logger.warning("No search results found with any selector")
                    return [company_name, "取得失敗", "エラー: 検索結果が見つかりませんでした", "エラー"]

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
                        raise Exception("リンク要素が見つかりません")

                    company_url = link.get_attribute("href")
                    if not company_url:
                        self.logger.error("No URL found in link element")
                        raise Exception("URLが取得できません")

                    self.logger.debug(f"Successfully extracted company URL: {company_url}")
                except Exception as e:
                    self.logger.error(f"Failed to extract company URL: {str(e)}")
                    return [company_name, "取得失敗", f"エラー: URLの取得に失敗: {str(e)}", "エラー"]

                # 会社のWebサイトにアクセス
                try:
                    self.logger.debug(f"Attempting to access company website: {company_url}")
                    driver.get(company_url)
                    self.logger.debug("Successfully accessed company website")
                    time.sleep(2)
                except Exception as e:
                    self.logger.error(f"Failed to access company website: {str(e)}")
                    return [company_name, "取得失敗", f"エラー: 会社のWebサイトへのアクセスに失敗: {str(e)}", "エラー"]

                # ページのHTMLを取得
                try:
                    self.logger.debug("Attempting to get page source...")
                    page_source = driver.page_source
                    self.logger.debug("Successfully got page source")
                except Exception as e:
                    self.logger.error(f"Failed to get page source: {str(e)}")
                    return [company_name, "取得失敗", f"エラー: ページのHTML取得に失敗: {str(e)}", "エラー"]

                # 役員情報の抽出
                try:
                    self.logger.debug("Attempting to extract executive information...")
                    # ページのテキストを取得
                    page_text = driver.find_element(By.TAG_NAME, "body").text
                    self.logger.debug("Successfully got page text")

                    # 役員情報を含む可能性のあるセクションを特定
                    doc = self.nlp(page_text)
                    executive_sections = []
                    for sent in doc.sents:
                        if any(keyword in sent.text for keyword in ["役員", "取締役", "代表", "社長", "会長", "CEO", "取締役会長", "執行役員"]):
                            executive_sections.append(sent.text)
                            self.logger.debug(f"Found executive section: {sent.text}")

                    if not executive_sections:
                        self.logger.warning("No executive sections found")
                        return [company_name, company_url, "役員情報なし", "情報なし"]

                    # 最も関連性の高いセクションを選択
                    best_section = max(executive_sections, key=lambda x: len(x))
                    self.logger.debug(f"Selected best section: {best_section}")

                    # 役員名の抽出 - spaCy NEの代わりにルールベースのアプローチを追加
                    executive_names = []
                    
                    # まずspaCyのNERを試す
                    for ent in self.nlp(best_section).ents:
                        if ent.label_ == "PERSON":
                            executive_names.append(ent.text)
                            self.logger.debug(f"Found executive name with spaCy: {ent.text}")
                    
                    # spaCyが失敗した場合、ルールベースのアプローチを試す
                    if not executive_names:
                        self.logger.debug("Attempting rule-based name extraction")
                        
                        # 日本人の名前の正規表現パターン (例: 森井 久恵、山田太郎など)
                        japanese_name_pattern = r'([一-龯ぁ-んァ-ヶー]{1,10}\s*[一-龯ぁ-んァ-ヶー]{1,10})'
                        
                        # 特定の役職名の後に続く名前を探す
                        # 役職パターン (長い順に並べる)
                        positions = [
                            "代表取締役最高経営責任者", "代表取締役社長兼CEO", "代表取締役社長CEO", 
                            "代表取締役社長", "代表取締役CEO", "代表取締役", "取締役社長", 
                            "社長", "取締役", "会長", "CEO", "取締役会長", "執行役員"
                        ]
                        
                        # 固有の表現をチェック - スターバックスのような特殊なケース
                        if "CEO)" in best_section:
                            # CEOの後の行を名前として使用
                            lines = best_section.split('\n')
                            for i, line in enumerate(lines):
                                if "CEO)" in line and i+1 < len(lines):
                                    name_candidate = lines[i+1].strip()
                                    if name_candidate and len(name_candidate) > 1:
                                        executive_names.append(name_candidate)
                                        self.logger.debug(f"Found executive name after CEO: {name_candidate}")
                                        break
                        
                        # 一般的な役職名の後に続く名前を探す
                        if not executive_names:
                            for position in positions:
                                if position in best_section:
                                    text_after_position = best_section[best_section.find(position) + len(position):]
                                    japanese_names = re.findall(japanese_name_pattern, text_after_position[:50])
                                    if japanese_names:
                                        executive_names.append(japanese_names[0])
                                        self.logger.debug(f"Found Japanese name after {position}: {japanese_names[0]}")
                                        break
                        
                        # 行ごとの解析を試みる
                        if not executive_names:
                            lines = best_section.split('\n')
                            for i, line in enumerate(lines):
                                if any(position in line for position in positions) and i+1 < len(lines):
                                    next_line = lines[i+1].strip()
                                    if re.match(japanese_name_pattern, next_line):
                                        executive_names.append(next_line)
                                        self.logger.debug(f"Found executive name in next line: {next_line}")
                                        break
                        
                        # 最終手段: ページ内の全ての日本人名を探して最初のものを使用
                        if not executive_names:
                            all_japanese_names = re.findall(japanese_name_pattern, best_section)
                            if all_japanese_names:
                                # 名前として使えそうな長さか確認
                                for name in all_japanese_names:
                                    if 2 <= len(name) <= 20:  # 名前の長さのチェック
                                        executive_names.append(name)
                                        self.logger.debug(f"Found Japanese name pattern: {name}")
                                        break

                    if not executive_names:
                        self.logger.warning("No executive names found")
                        return [company_name, company_url, "役員名抽出失敗", "情報なし"]

                    self.logger.debug(f"Successfully extracted executive information: {executive_names[0]}")
                    return [company_name, company_url, "役員", executive_names[0]]

                except Exception as e:
                    self.logger.error(f"Failed to extract executive information: {str(e)}")
                    return [company_name, "取得失敗", f"エラー: 役員情報の抽出に失敗: {str(e)}", "エラー"]

            except Exception as e:
                self.logger.error(f"Failed to process search results: {str(e)}")
                return [company_name, "取得失敗", f"エラー: 検索結果の処理に失敗: {str(e)}", "エラー"]

        except Exception as e:
            self.logger.error(f"Unexpected error occurred: {str(e)}")
            return [company_name, "取得失敗", f"エラー: {str(e)}", "エラー"]
    
    def extract_cleaned_content(self, driver, url):
        """
        Seleniumを使用してWebページの内容を取得し、不要な要素を除去して整形したテキストを返す関数
        """
        try:
            # URLにアクセス
            driver.get(url)
            
            # ページの読み込みを待機
            time.sleep(random.uniform(4, 8))
            
            # JavaScriptを実行して不要な要素を削除
            remove_elements_script = """
                const elementsToRemove = document.querySelectorAll('script, style, nav, footer, header, aside');
                elementsToRemove.forEach(element => element.remove());
                return document.body.innerText;
            """
            
            # JavaScriptを実行してテキストを取得
            text = driver.execute_script(remove_elements_script)
            
            if not text:
                return "Body content not found"
                
            # テキストの整形
            # 余分な空白を削除
            text = re.sub(r'\s+', ' ', text)
            text = text.strip()
            
            return text
            
        except Exception as e:
            return f"Error occurred: {str(e)}"
    
    def query(self, text: str) -> str:
       prompt = f"""
与えられた文脈から役員情報を抽出してJSON形式で返してください。

文脈:
{text}

出力形式:
{{
    "executives": [
        {{
            "役職": "役職名",
            "氏名": "氏名"
        }}
    ]
}}

抽出ルール:
1. 文脈に明示的に記載されている情報のみを使用すること
2. 以下の場合は該当フィールドを空文字列("")として返す：
   - 情報が不明確な場合
   - 情報が欠落している場合
   - 情報の信頼性が低い場合

エラー処理:
- 文脈が空の場合: {{"executives": []}}
- 役員情報が見つからない場合: {{"executives": []}}
- 不正なフォーマットの場合: {{"error": "Invalid format in source text"}}

注意事項:
- 推測や外部知識は使用しないこと
- 文脈に明示的に記載されている情報のみを使用すること
- 情報の重複がある場合は、最新の情報のみを使用すること
"""
       response = self.model.generate_content(prompt)
       return response.text
    
    def write_company_data(self, company_data):
        """
        会社データをファイルに直接書き込む
        Args:
            company_data (list): 書き込む会社データのリスト
        """
        try:
            with open(self.output_file, 'a', newline='', encoding='utf-8') as f_out:
                writer = csv.writer(f_out)
                for row in company_data:
                    if len(row) == 4:  # 正しい列数かチェック
                        writer.writerow(row)
                        print(f"書き込み完了: {row[0]} - {row[2]} - {row[3]}")
                print(f"{len(company_data)}件のデータを書き込みました")
                print(f"ファイルの場所: {os.path.abspath(self.output_file)}")
        except Exception as e:
            logging.error(f"ファイル書き込み中にエラーが発生: {str(e)}")
            print(f"ファイル書き込みエラー: {str(e)}")


if __name__ == "__main__":
    GOOGLE_API_KEY = "AIzaSyAAZratHSyw71DkAyk_WHcUkwkXW-yksGk"
    scraper = CompanySalesScraper(api_key=GOOGLE_API_KEY)
    scraper.start_scraping()