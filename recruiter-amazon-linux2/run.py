import csv
import time
import random
import os
import glob
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

class CompanyRecruiterScraper:
    def __init__(self, api_key: str):
        self.input_file = './data/input.csv'
        self.output_file = './data/output_recruiter.csv'

        # ログ設定
        logging.basicConfig(
            filename='recruiter_scraper.log',
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

        self.logger.info("Initializing CompanyRecruiterScraper...")

        # 出力ディレクトリの確認と作成
        output_dir = os.path.dirname(self.output_file)
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
            self.logger.debug(f"Created output directory: {output_dir}")

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
                writer.writerow(['会社名', 'URL', '採用担当者名', 'メールアドレス', '電話番号'])
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
            # 出力ディレクトリの確認と作成
            output_dir = os.path.dirname(self.output_file)
            if not os.path.exists(output_dir):
                os.makedirs(output_dir)
                self.logger.debug(f"Created output directory: {output_dir}")

            with open(self.output_file, 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerow(['会社名', 'URL', '採用担当者名', 'メールアドレス', '電話番号'])
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
                        
                        # scrape_company_dataがリスト（単一採用担当者）または採用担当者のリストのリスト（複数採用担当者）を返す可能性がある
                        company_data = self.scrape_company_data(driver, company_name)
                        
                        # company_dataがリストのリストである場合（複数採用担当者）
                        if company_data and isinstance(company_data, list):
                            if len(company_data) > 0:
                                # 最初の要素が採用担当者情報のリストか確認
                                if isinstance(company_data[0], list) and len(company_data) > 1:
                                    # 重複した名前のチェックと削除を行う
                                    company_data = self.remove_duplicate_recruiters(company_data)
                                    # 複数採用担当者のリスト
                                    self.write_company_data(company_data)
                                else:
                                    # 単一採用担当者
                                    self.write_company_data([company_data])
                        else:
                            # データが取得できなかった場合も出力に含める
                            self.write_company_data([[company_name, "取得失敗", "情報取得失敗", "情報なし", "情報なし"]])
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
    
    def remove_duplicate_recruiters(self, recruiters_data):
        """Remove duplicate recruiters based on both name and email"""
        unique_recruiters = []
        seen_combinations = set()
        
        for recruiter_data in recruiters_data:
            if len(recruiter_data) < 4:
                self.logger.warning(f"不完全なデータをスキップ: {recruiter_data}")
                continue
            
            company_name = recruiter_data[0]
            url = recruiter_data[1]
            name = recruiter_data[2]
            email = recruiter_data[3]
            
            # Create a unique key combining company, name and email
            key = (company_name.strip(), name.strip(), email.strip())
            
            if key not in seen_combinations:
                seen_combinations.add(key)
                unique_recruiters.append(recruiter_data)
                self.logger.debug(f"追加: {company_name} - {name} - {email}")
            else:
                self.logger.debug(f"スキップ (重複): {company_name} - {name} - {email}")
        
        removed_count = len(recruiters_data) - len(unique_recruiters)
        if removed_count > 0:
            self.logger.info(f"{removed_count}件の重複採用担当者データを除外しました")
        
        self.logger.debug(f"重複チェック後の採用担当者数: {len(unique_recruiters)}")
        return unique_recruiters
        
    def scrape_company_data(self, driver, company_name):
        """Scrape recruiter data for a company"""
        self.logger.debug(f"Starting scrape_company_data for {company_name}")
        self.driver = driver  # Store driver reference
        
        # First try to get data from company website using LLM
        company_url = self.get_company_url(company_name)
        if company_url:
            self.logger.debug(f"Attempting to access company website: {company_url}")
            try:
                self.driver.get(company_url)
                time.sleep(5)  # Wait for page to load
                
                # Take screenshot of company page
                screenshot_path = f"screenshot_company_{company_name}.png"
                self.driver.save_screenshot(screenshot_path)
                self.logger.debug(f"Saved company page screenshot to {screenshot_path}")
                
                # Get page content
                page_content = self.driver.page_source
                self.logger.debug(f"Page content length: {len(page_content)}")
                
                # Use LLM to extract recruiter information
                llm_response = self.query(page_content)
                self.logger.debug(f"LLM Response: {llm_response}")
                
                try:
                    import json
                    recruiters_data_json = json.loads(llm_response)
                    if "recruiters" in recruiters_data_json and recruiters_data_json["recruiters"]:
                        recruiters = recruiters_data_json["recruiters"]
                        if recruiters:
                            self.logger.debug(f"Extracted recruiter information: {recruiters}")
                            for recruiter in recruiters:
                                name = recruiter.get('採用担当者名', '採用担当')
                                email = recruiter.get('メールアドレス', '')
                                phone = recruiter.get('電話番号', '')
                                if email or phone:
                                    self.write_company_data([[company_name, company_url, name, email, phone]])
                            return
                except json.JSONDecodeError as e:
                    self.logger.error(f"JSON解析エラー: {str(e)}")
                
                self.logger.warning(f"LLM method failed to find recruiters for {company_name}, trying Google search as fallback")
                
            except Exception as e:
                self.logger.error(f"Error accessing company website: {str(e)}")
                self.logger.warning("Trying Google search as fallback")
        
        # If LLM method failed or no company URL found, try Google search
        self.logger.debug("Attempting Google search as fallback")
        search_url = self.construct_search_url(company_name)
        self.logger.debug(f"Search URL constructed: {search_url}")
        
        try:
            self.driver.get(search_url)
            time.sleep(5)  # Wait for page to load
            
            # Take screenshot of search results
            screenshot_path = f"screenshot_{company_name}.png"
            self.driver.save_screenshot(screenshot_path)
            self.logger.debug(f"Saved screenshot to {screenshot_path}")
            
            # Extract emails and phone numbers from search results
            emails, phones = self.extract_emails_from_search_results()
            if emails or phones:
                self.logger.debug(f"Found email(s) in search results: {emails}")
                self.logger.debug(f"Found phone number(s) in search results: {phones}")
                for email in emails:
                    phone = phones[0] if phones else ""
                    self.write_company_data([[company_name, search_url, "採用担当", email, phone]])
                    self.logger.debug(f"Added recruiter from search results: 採用担当 - {email} - {phone}")
        
        except Exception as e:
            self.logger.error(f"Error during Google search: {str(e)}")
            self.write_company_data([[company_name, "", "情報取得失敗", "情報なし", "情報なし"]])
    
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
                
            # 採用担当者情報が含まれる可能性の高いセクションに優先度を置く
            priority_content = []
            
            # 採用担当者情報を含む可能性の高いセクションを探す
            recruiter_keywords = ['採用担当', '採用責任者', '採用窓口', '採用', 'recruit', 'recruitment', '採用情報']
            
            # テーブルから採用担当者情報を抽出 (テーブルを最優先)
            for table in soup.find_all('table'):
                # テーブルのHTMLを保存して後で処理
                table_html = str(table)
                table_text = []
                for row in table.find_all('tr'):
                    cells = [cell.get_text().strip() for cell in row.find_all(['td', 'th'])]
                    if cells and any(keyword in ' '.join(cells).lower() for keyword in recruiter_keywords):
                        table_text.append(' | '.join(cells))
                
                if table_text:
                    # テーブルデータを文字列化
                    table_data = "\n".join(table_text)
                    # テーブルHTMLと文字列化データの両方を追加
                    priority_content.append(f"テーブルデータ:\n{table_data}\n\nテーブルHTML:\n{table_html}")
            
            # 見出し要素を探す
            for heading in soup.find_all(['h1', 'h2', 'h3', 'h4', 'h5', 'h6']):
                heading_text = heading.get_text().strip()
                if any(keyword in heading_text for keyword in recruiter_keywords):
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
                
                # クラス名、ID、またはテキスト内容に採用担当者関連のキーワードが含まれているセクションを探す
                if any(any(keyword in str(attr).lower() for keyword in recruiter_keywords) 
                       for attr in [section_class, section_id, section_text]):
                    # セクションのHTMLと平文の両方を保存
                    section_html = str(section)
                    section_plain = section.get_text().strip()
                    priority_content.append(f"セクションHTML:\n{section_html}\n\nセクション平文:\n{section_plain}")
            
            # DLリストから採用担当者情報を抽出
            for dl in soup.find_all('dl'):
                dl_html = str(dl)
                dl_text = []
                dts = dl.find_all('dt')
                dds = dl.find_all('dd')
                
                if len(dts) == len(dds):  # dtとddの数が一致する場合のみ
                    for dt, dd in zip(dts, dds):
                        dt_text = dt.get_text().strip()
                        dd_text = dd.get_text().strip()
                        if any(keyword in dt_text.lower() for keyword in recruiter_keywords) or any(keyword in dd_text.lower() for keyword in recruiter_keywords):
                            dl_text.append(f"{dt_text}: {dd_text}")
                    
                    if dl_text:
                        priority_content.append(f"DLデータ:\n" + "\n".join(dl_text) + f"\n\nDL HTML:\n{dl_html}")
            
            # ULリストから採用担当者情報を抽出
            for ul in soup.find_all('ul'):
                ul_html = str(ul)
                ul_items = []
                list_items = ul.find_all('li')
                
                # リスト内のテキストをチェック
                list_text = ul.get_text().lower()
                if any(keyword in list_text for keyword in recruiter_keywords):
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
以下は企業の採用担当者情報を含むウェブページからの抽出テキストです。採用担当者名とメールアドレスのペアを特定してください。
特に注意: ウェブページにある氏名の漢字は正確にそのまま抽出してください。メールアドレスは完全な形式で抽出してください。

本文:
""" + extracted_text
            
            return prefixed_text
            
        except Exception as e:
            return f"Error occurred: {str(e)}"
    
    def query(self, text: str) -> str:
       prompt = f"""
与えられた文脈から採用担当者情報をできるだけ多く抽出してJSON形式で返してください。

文脈:
{text}

出力形式:
{{
    "recruiters": [
        {{
            "採用担当者名": "氏名",
            "メールアドレス": "メールアドレス",
            "電話番号": "電話番号"
        }},
        {{
            "採用担当者名": "氏名2",
            "メールアドレス": "メールアドレス2",
            "電話番号": "電話番号2"
        }}
        // 他の採用担当者情報...
    ]
}}

抽出ルール:
1. 文脈に明示的に記載されている情報のみを使用すること
2. できるだけ多くの採用担当者情報を抽出すること
3. 採用担当者名、メールアドレス、電話番号のペアを優先的に抽出すること
4. 以下の場合は該当フィールドを空文字列("")として返す：
   - 情報が不明確な場合
   - 情報が欠落している場合
   - 情報の信頼性が低い場合
5. 名前に含まれる漢字や仮名は変換や置き換えをせず、そのまま保持すること
6. メールアドレスは完全な形式で抽出すること（例: example@company.co.jp）
7. 電話番号は日本形式で抽出すること（例: 03-1234-5678）
   - 市外局番は必ず含めること
   - ハイフンで区切ること
   - 10桁または11桁の形式にすること
8. 採用担当者名が不明な場合は空文字列("")として返す

エラー処理:
- 文脈が空の場合: {{"recruiters": []}}
- 採用担当者情報が見つからない場合: {{"recruiters": []}}
- 不正なフォーマットの場合: {{"error": "Invalid format in source text"}}

注意事項:
- 推測や外部知識は使用しないこと
- 文脈に明示的に記載されている情報のみを使用すること
- できるだけ多くの採用担当者を抽出すること
- 人名の漢字を変更しないこと
- メールアドレスは完全な形式で抽出すること
- 電話番号は必ず市外局番を含め、ハイフンで区切った形式で抽出すること
"""
       response = self.model.generate_content(prompt)
       return response.text
    
    def write_company_data(self, company_data):
        """
        会社データをファイルに直接書き込む
        Args:
            company_data (list): 書き込む会社データのリスト [[会社名, URL, 採用担当者名, メールアドレス, 電話番号], ...]
        """
        if not company_data:
            self.logger.warning("書き込むデータがありません")
            return

        try:
            with open(self.output_file, 'a', newline='', encoding='utf-8') as f_out:
                writer = csv.writer(f_out)
                valid_rows = 0
                skipped_rows = 0

                for row in company_data:
                    if not isinstance(row, (list, tuple)) or len(row) != 5:
                        self.logger.warning(f"不正なデータ形式をスキップ: {row}")
                        skipped_rows += 1
                        continue

                    # データの検証と整形
                    company_name = str(row[0]).strip()
                    url = str(row[1]).strip()
                    recruiter_name = str(row[2]).strip()
                    email = str(row[3]).strip() if row[3] else ""
                    phone = str(row[4]).strip() if row[4] else ""

                    # 必須フィールドの検証
                    if not (company_name and recruiter_name):
                        self.logger.warning(f"必須フィールドが欠落しているデータをスキップ: {row}")
                        skipped_rows += 1
                        continue

                    # 失敗メッセージを含む行をスキップ
                    if any(msg in [url, recruiter_name, email, phone] for msg in ["取得失敗", "情報取得失敗", "情報なし"]):
                        self.logger.debug(f"失敗メッセージを含む行をスキップ: {row}")
                        skipped_rows += 1
                        continue

                    # メールアドレスのクリーニング
                    if email:
                        email = email.replace('u003e', '').replace('u300c', '').replace('mailto:', '')
                        email = re.sub(r'[<>]', '', email)

                    cleaned_row = [company_name, url, recruiter_name, email, phone]
                    writer.writerow(cleaned_row)
                    valid_rows += 1
                    self.logger.debug(f"書き込み完了: {company_name} - {recruiter_name} - {email} - {phone}")

                self.logger.info(f"{valid_rows}件のデータを書き込みました（スキップ: {skipped_rows}件）")
                self.logger.debug(f"ファイルの場所: {os.path.abspath(self.output_file)}")

        except PermissionError as pe:
            self.logger.error(f"ファイルへのアクセス権限がありません: {str(pe)}")
        except Exception as e:
            self.logger.error(f"ファイル書き込み中にエラーが発生しました: {str(e)}")

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

    def get_company_url(self, company_name):
        """Get company URL from Google search results"""
        search_url = self.construct_search_url(company_name)
        self.logger.debug(f"Search URL constructed: {search_url}")
        
        try:
            self.driver.get(search_url)
            time.sleep(5)  # Wait for page to load
            
            # Try different selectors for search results
            selectors = [
                "div.g",  # Traditional selector
                "div[data-sokoban-container]",  # New selector
                "div.tF2Cxc",  # Another new selector
                "div.yuRUbf",  # Another new selector
                "#search .g",  # Another common selector
                ".rc",  # Previously used selector
                "div.hlcw0c",  # Mobile view selector
                "div.MjjYud",  # New structure
                "h3.LC20lb"  # Search from headings
            ]
            
            for selector in selectors:
                try:
                    self.logger.debug(f"Trying selector: {selector}")
                    WebDriverWait(self.driver, 3).until(
                        EC.presence_of_element_located((By.CSS_SELECTOR, selector))
                    )
                    search_results = self.driver.find_elements(By.CSS_SELECTOR, selector)
                    if search_results and len(search_results) > 0:
                        self.logger.debug(f"Found search results using selector: {selector}")
                        first_result = search_results[0]
                        
                        # Try different link selectors
                        link_selectors = ["a", "a[href]", "a[jsname='UWckNb']"]
                        for link_selector in link_selectors:
                            try:
                                link = first_result.find_element(By.CSS_SELECTOR, link_selector)
                                if link:
                                    company_url = link.get_attribute("href")
                                    if company_url:
                                        self.logger.debug(f"Successfully extracted company URL: {company_url}")
                                        return company_url
                            except:
                                continue
                except:
                    continue
            
            # If no URL found through search, try to guess it
            company_simple_name = company_name.split()[0].lower()
            for ja, en in {"スターバックス": "starbucks", "ファーストリテイリング": "fastretailing", 
                          "コメダ": "komeda", "はま寿司": "hamazushi", "ドトール": "doutor"}.items():
                if ja in company_name:
                    company_simple_name = en
                    break
            guessed_url = f"https://www.{company_simple_name}.co.jp/"
            self.logger.debug(f"Falling back to guessed URL: {guessed_url}")
            return guessed_url
            
        except Exception as e:
            self.logger.error(f"Error getting company URL: {str(e)}")
            return None

    def construct_search_url(self, company_name):
        """Construct Google search URL for company"""
        search_query = f"{company_name} 採用担当者メールアドレス"
        return f"https://www.google.co.jp/search?q={quote_plus(search_query)}&hl=ja"

    def extract_emails_from_search_results(self):
        """Extract emails and phone numbers from search results page"""
        page_source = self.driver.page_source
        
        # Extract emails
        email_pattern = r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}'
        emails_in_search = re.findall(email_pattern, page_source)
        
        # Enhanced phone number patterns
        phone_patterns = [
            # Standard format: 03-1234-5678
            r'0\d{1,3}-\d{1,4}-\d{4}',
            # Without hyphens: 0312345678
            r'0\d{9,10}',
            # With parentheses: (03)1234-5678
            r'\(\d{1,4}\)\d{1,4}-\d{4}',
            # With Japanese hyphen: 03ー1234ー5678
            r'0\d{1,3}[ー－]\d{1,4}[ー－]\d{4}',
            # Toll-free numbers: 0120-123-456
            r'0120-\d{3}-\d{3}',
            # Mobile numbers: 090-1234-5678
            r'0[789]\d-\d{4}-\d{4}',
            # Area code with space: 03 1234 5678
            r'0\d{1,3}\s+\d{1,4}\s+\d{4}'
        ]
        
        # Find all potential phone numbers
        phones_in_search = []
        for pattern in phone_patterns:
            phones_in_search.extend(re.findall(pattern, page_source))
        
        # Clean and format phone numbers
        cleaned_phones = []
        for phone in phones_in_search:
            # Remove any non-numeric characters except hyphens
            cleaned = re.sub(r'[^\d-]', '', phone)
            
            # Skip if the number is too short (less than 10 digits)
            if len(cleaned) < 10:
                continue
                
            # Format as 03-1234-5678 style
            if len(cleaned) == 10:  # e.g., 0312345678
                cleaned = f"{cleaned[:3]}-{cleaned[3:7]}-{cleaned[7:]}"
            elif len(cleaned) == 11:  # e.g., 09012345678
                cleaned = f"{cleaned[:3]}-{cleaned[3:7]}-{cleaned[7:]}"
            elif len(cleaned) == 12:  # e.g., 0120123456
                cleaned = f"{cleaned[:4]}-{cleaned[4:7]}-{cleaned[7:]}"
            
            # Additional validation for Japanese phone numbers
            if not re.match(r'^0\d{1,3}-\d{1,4}-\d{4}$', cleaned):
                continue
                
            # Check if it's a valid area code
            area_code = cleaned.split('-')[0]
            if len(area_code) == 3 and not area_code.startswith(('080', '090', '070')):
                # For 3-digit area codes, second digit should be 1-9
                if area_code[1] not in '123456789':
                    continue
            elif len(area_code) == 4 and area_code.startswith('0120'):
                # For toll-free numbers, format should be 0120-XXX-XXX
                if not re.match(r'^0120-\d{3}-\d{3}$', cleaned):
                    continue
            
            cleaned_phones.append(cleaned)
        
        # Remove duplicates while preserving order
        cleaned_phones = list(dict.fromkeys(cleaned_phones))
        
        if emails_in_search:
            # Remove duplicates and prioritize recruitment-related emails
            unique_emails = list(dict.fromkeys(emails_in_search))
            preferred_email = None
            for email in unique_emails:
                if any(keyword in email.lower() for keyword in ['recruit', 'career', 'personnel', 'hr', 'jinji', 'saiyou']):
                    preferred_email = email
                    break
            if not preferred_email and unique_emails:
                preferred_email = unique_emails[0]
            return [preferred_email] if preferred_email else [], cleaned_phones
        return [], cleaned_phones

if __name__ == "__main__":
    GOOGLE_API_KEY = "AIzaSyAAZratHSyw71DkAyk_WHcUkwkXW-yksGk"
    scraper = CompanyRecruiterScraper(api_key=GOOGLE_API_KEY)
    scraper.start_scraping()