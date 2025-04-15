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

class CompanySalesScraper:
    def __init__(self, api_key: str):
        self.input_file = './data/input.csv'
        self.output_file = './data/output_executive.csv'

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
                        
                        # scrape_company_dataがリスト（単一役員）または役員のリストのリスト（複数役員）を返す可能性がある
                        company_data = self.scrape_company_data(driver, company_name)
                        
                        # company_dataがリストのリストである場合（複数役員）
                        if company_data and isinstance(company_data, list):
                            if len(company_data) > 0:
                                # 最初の要素が役員情報のリストか確認
                                if isinstance(company_data[0], list) and len(company_data) > 1:
                                    # 重複した名前のチェックと削除を行う
                                    company_data = self.remove_duplicate_executives(company_data)
                                    # 複数役員のリスト
                                    self.write_company_data(company_data)
                                else:
                                    # 単一役員
                                    self.write_company_data([company_data])
                        else:
                            # データが取得できなかった場合も出力に含める
                            self.write_company_data([[company_name, "取得失敗", "情報取得失敗", "情報なし"]])
                            self.logger.warning(f"{company_name}の情報が取得できませんでした")
                            
                        # ランダムな遅延（5〜10秒）
                        time.sleep(random.uniform(4, 8))
        except Exception as e:
            logging.critical(f"スクレイピングが予期せず終了しました: {str(e)}")
            print(f"エラーが発生しました: {str(e)}")
        finally:
            # スクリーンショットファイルを削除
            self.cleanup_screenshots()
            
            logging.info("スクレイピングプロセスが完了しました。")
            print(f"\nスクレイピングが完了しました。結果は {os.path.abspath(self.output_file)} に保存されています。")
    
    def remove_duplicate_executives(self, executives_data):
        """
        重複した役員名を持つデータを除去し、最初に出現したもののみを保持する
        
        Args:
            executives_data (list): 役員データのリスト [会社名, URL, 役職, 氏名]
        
        Returns:
            list: 重複を除去した役員データのリスト
        """
        self.logger.debug(f"重複チェック前の役員数: {len(executives_data)}")
        
        # 役員名をキーとした辞書を作成して重複を管理
        unique_executives = {}
        unique_data = []
        
        for exec_data in executives_data:
            if len(exec_data) < 4:
                # データが不完全な場合はスキップ
                continue
                
            company_name = exec_data[0]
            url = exec_data[1]
            position = exec_data[2]
            name = exec_data[3]
            
            # 会社内での名前の重複をチェック
            if name not in unique_executives:
                unique_executives[name] = True
                unique_data.append(exec_data)
                self.logger.debug(f"追加: {company_name} - {position} - {name}")
            else:
                self.logger.debug(f"重複として除外: {company_name} - {position} - {name}")
        
        removed_count = len(executives_data) - len(unique_data)
        if removed_count > 0:
            self.logger.info(f"{removed_count}件の重複役員データを除外しました")
        
        self.logger.debug(f"重複チェック後の役員数: {len(unique_data)}")
        return unique_data
        
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

            # 会社のWebサイトにアクセス
            try:
                self.logger.debug(f"Attempting to access company website: {company_url}")
                driver.get(company_url)
                self.logger.debug("Successfully accessed company website")
                # Amazon Linux上でより長く待機
                time.sleep(10)  # Increased wait time for better page loading
                
                # スクリーンショットを撮って確認
                try:
                    screenshot_path = f"screenshot_company_{company_name.replace(' ', '_')}.png"
                    driver.save_screenshot(screenshot_path)
                    self.logger.debug(f"Saved company page screenshot to {screenshot_path}")
                except Exception as s_ex:
                    self.logger.debug(f"Failed to save company screenshot: {s_ex}")
                    
            except Exception as e:
                self.logger.error(f"Failed to access company website: {str(e)}")
                
                # 直接ウェブサイトにアクセスを試みる
                try:
                    # 会社名からドメインを推測してアクセスを試みる
                    self.logger.debug("Attempting alternative access methods...")
                    
                    # まず .co.jp を試す
                    company_simple_name = company_name.split()[0].lower()
                    alt_url = f"https://www.{company_simple_name}.co.jp/"
                    self.logger.debug(f"Trying alternative URL: {alt_url}")
                    
                    driver.get(alt_url)
                    self.logger.debug(f"Successfully accessed alternative URL: {alt_url}")
                    company_url = alt_url
                    time.sleep(10)
                except Exception as alt_ex:
                    self.logger.error(f"Alternative access also failed: {str(alt_ex)}")
                    return [company_name, "取得失敗", f"エラー: 会社のWebサイトへのアクセスに失敗: {str(e)}", "エラー"]

            # ページのHTMLを取得して役員情報を抽出（LLMを使用）
            try:
                # 全ページコンテンツを取得
                page_content = self.extract_cleaned_content(driver, company_url)
                self.logger.debug(f"Page content length: {len(page_content)}")
                
                # LLMを使用して役員情報を抽出
                executives_json = self.query(page_content)
                self.logger.debug(f"LLM Response: {executives_json}")
                
                # JSON解析
                try:
                    import json
                    executives_data_json = json.loads(executives_json)
                    if "executives" in executives_data_json and executives_data_json["executives"]:
                        for executive in executives_data_json["executives"]:
                            position = executive.get("役職", "")
                            name = executive.get("氏名", "")
                            if name and name != "":
                                # 結果を追加
                                executives_data.append([company_name, company_url, position, name.strip()])
                                self.logger.debug(f"抽出された役員情報: {position} - {name}")
                        
                        if executives_data:
                            return executives_data  # 複数役員を返す
                except json.JSONDecodeError as e:
                    self.logger.error(f"JSON解析エラー: {str(e)}")
                
                # LLMが失敗した場合、従来のメソッドを試す
                if not executives_data:
                    self.logger.debug("LLM extraction failed, trying traditional methods")
                    try:
                        # ページのテキストを取得
                        page_text = driver.find_element(By.TAG_NAME, "body").text
                        # 従来の抽出ロジック
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

                        # 以下の単語は役職であり、名前ではないので除外リストに追加
                        position_terms = [
                            "取締役", "監査等委員", "代表取締役", "社長", "会長", "執行役員", "CEO", 
                            "取締役会長", "副社長", "常務", "専務", "執行", "監査役", "支配人"
                        ]
                        
                        # 役職と名前のパターンを検出
                        positions = [
                            "代表取締役最高経営責任者", "代表取締役社長兼CEO", "代表取締役社長CEO", 
                            "代表取締役社長", "代表取締役CEO", "代表取締役", "取締役社長", 
                            "社長", "会長", "CEO", "取締役会長", "副社長", "常務取締役", "監査役"
                        ]
                        
                        # 役職+名前のパターンを特定
                        combined_pattern = r'((' + '|'.join(positions) + r')\s*([一-龯ぁ-んァ-ヶー]{1,10}\s*[一-龯ぁ-んァ-ヶー]{1,10}))'
                        combined_matches = re.findall(combined_pattern, best_section)
                        for match in combined_matches:
                            full_match, position, name = match
                            if name and len(name) >= 2:
                                # 名前が単なる役職ではないことを確認
                                if name not in position_terms:
                                    executives_data.append([company_name, company_url, position, name.strip()])
                                    self.logger.debug(f"Found name with combined pattern: {name} after {position}")
                        
                        # 他の抽出パターン...
                        if not executives_data:
                            # 日本人の名前の正規表現パターン
                            japanese_name_pattern = r'([一-龯ぁ-んァ-ヶー]{1,10}\s*[一-龯ぁ-んァ-ヶー]{1,10})'
                            
                            for position in positions:
                                if position in best_section:
                                    text_after_position = best_section[best_section.find(position) + len(position):]
                                    japanese_names = re.findall(japanese_name_pattern, text_after_position[:50])
                                    if japanese_names:
                                        name = japanese_names[0]
                                        # 名前が単なる役職でないことを確認
                                        if name not in position_terms:
                                            executives_data.append([company_name, company_url, position, name.strip()])
                                            self.logger.debug(f"Found Japanese name after {position}: {name}")
                        
                        if executives_data:
                            return executives_data  # 複数役員を返す
                        else:
                            return [company_name, company_url, "役員名抽出失敗", "情報なし"]
                        
                    except Exception as e:
                        self.logger.error(f"従来の抽出ロジックで失敗: {str(e)}")
                        return [company_name, company_url, "役員名抽出失敗", "情報なし"]
                
            except Exception as e:
                self.logger.error(f"Failed to extract executive information: {str(e)}")
                return [company_name, "取得失敗", f"エラー: 役員情報の抽出に失敗: {str(e)}", "エラー"]
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
    
    def query(self, text: str) -> str:
       prompt = f"""
与えられた文脈から役員情報をできるだけ多く抽出してJSON形式で返してください。

文脈:
{text}

出力形式:
{{
    "executives": [
        {{
            "役職": "役職名",
            "氏名": "氏名"
        }},
        {{
            "役職": "役職名2",
            "氏名": "氏名2"
        }}
        // 他の役員情報...
    ]
}}

抽出ルール:
1. 文脈に明示的に記載されている情報のみを使用すること
2. できるだけ多くの役員情報を抽出すること
3. 代表取締役社長、CEO、取締役、監査役などの役職と氏名のペアを優先的に抽出すること
4. 以下の場合は該当フィールドを空文字列("")として返す：
   - 情報が不明確な場合
   - 情報が欠落している場合
   - 情報の信頼性が低い場合
5. 「竹林 元也」と「竹林 基哉」のような似た名前が出てきた場合は、オリジナルの表記を正確に保持すること
6. 名前に含まれる漢字や仮名は変換や置き換えをせず、そのまま保持すること
7. 人名が含まれる場合は、正確に抽出すること（名前をローマ字や他の方法に変更しないこと）

日本語の役職の例:
- 代表取締役社長
- 代表取締役会長
- 取締役
- 常務取締役
- 専務取締役
- 社外取締役
- 監査役
- 社外監査役
- 執行役員

エラー処理:
- 文脈が空の場合: {{"executives": []}}
- 役員情報が見つからない場合: {{"executives": []}}
- 不正なフォーマットの場合: {{"error": "Invalid format in source text"}}

注意事項:
- 推測や外部知識は使用しないこと
- 文脈に明示的に記載されている情報のみを使用すること
- できるだけ多くの役員を抽出すること
- 人名の漢字を変更しないこと。例えば「竹林 基哉」を「竹林 元也」に変更しないこと
"""
       response = self.model.generate_content(prompt)
       return response.text
    
    def clean_url(self, url: str) -> str:
        """
        Clean up URL by removing tracking parameters and getting the base domain
        Args:
            url (str): The URL to clean
        Returns:
            str: Cleaned URL
        """
        try:
            parsed = urlparse(url)
            # Remove query parameters
            clean_url = urlunparse((parsed.scheme, parsed.netloc, parsed.path, '', '', ''))
            # If it's a specific page, get the base domain
            if len(clean_url.split('/')) > 4:
                base_domain = '/'.join(clean_url.split('/')[:3])
                return base_domain
            return clean_url
        except Exception as e:
            self.logger.error(f"URL cleaning failed: {str(e)}")
            return url

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
                        # Clean the URL and get base domain for official site
                        clean_url = self.clean_url(row[1])
                        # Add the cleaned official URL as the last column
                        writer.writerow(row + [clean_url])
                        self.logger.debug(f"書き込み完了: {row[0]} - {row[2]} - {row[3]}")
                self.logger.info(f"{len(company_data)}件のデータを書き込みました")
                self.logger.debug(f"ファイルの場所: {os.path.abspath(self.output_file)}")
        except Exception as e:
            self.logger.error(f"ファイル書き込み中にエラーが発生: {str(e)}")
            print(f"ファイル書き込みエラー: {str(e)}")

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


if __name__ == "__main__":
    GOOGLE_API_KEY = "AIzaSyAAZratHSyw71DkAyk_WHcUkwkXW-yksGk"
    scraper = CompanySalesScraper(api_key=GOOGLE_API_KEY)
    scraper.start_scraping()