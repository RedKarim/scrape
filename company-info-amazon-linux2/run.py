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
        self.output_file = './data/output_company_info.csv'

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
                            self.write_company_data([[company_name, "取得失敗", "情報取得失敗", "情報なし", "不明", "情報なし", "情報なし"]])
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
        """
        会社のデータをスクレイピングする
        Args:
            driver: Selenium WebDriver
            company_name (str): 会社名
        Returns:
            list: [会社名, 業種, 公式サイトURL, 年商]
        """
        self.logger.debug(f"Starting scrape_company_data for {company_name}")
        try:
            # 業種の検索と抽出
            industry = self.search_and_extract_industry(driver, company_name)
            industry = self.clean_text(industry)
            
            # 公式サイトURLの取得
            official_url = self.find_official_website(driver, company_name)
            official_url = self.clean_text(official_url)
            
            # 年商の検索と抽出
            annual_sales = self.search_and_extract_annual_sales(driver, company_name)
            annual_sales = self.clean_text(annual_sales)
            
            # 会社名のクリーンアップ
            company_name = self.clean_text(company_name)
            
            # データを返す
            return [[company_name, industry, official_url, annual_sales]]
            
        except Exception as e:
            self.logger.error(f"Error in scrape_company_data: {str(e)}")
            return [[company_name, "", "", ""]]

    def search_and_extract_industry(self, driver, company_name):
        """
        会社の業種を検索して抽出する
        """
        try:
            search_query = f"{company_name} 業種"
            search_url = f"https://www.google.co.jp/search?q={quote_plus(search_query)}&hl=ja"
            self.logger.debug(f"Searching for industry: {search_url}")
            
            driver.get(search_url)
            time.sleep(5)  # ページの読み込みを待つ
            
            # 検索結果ページのテキストを取得
            page_text = driver.find_element(By.TAG_NAME, "body").text
            
            # LLMを使用して業種を抽出
            prompt = f"""
以下のGoogle検索結果から、{company_name}の業種を抽出してください。
業種は以下のような形式で表現されることが多いです：
- "○○業" "○○製造" "○○サービス"などの形式
- "主な事業" "主要な事業"などの説明の後に続く説明

検索結果:
{page_text}

回答は業種のみを1行で返してください。
業種が不明な場合は空文字列を返してください。
"""
            response = self.model.generate_content(prompt)
            industry = response.text.strip()
            
            # 余分な文字を削除
            industry = industry.strip('[]\'"{}')
            return industry
            
        except Exception as e:
            self.logger.error(f"Error extracting industry: {str(e)}")
            return ""

    def search_and_extract_annual_sales(self, driver, company_name):
        """
        会社の年商を検索して抽出する
        """
        try:
            search_query = f"{company_name} 年商"
            search_url = f"https://www.google.co.jp/search?q={quote_plus(search_query)}&hl=ja"
            self.logger.debug(f"Searching for annual sales: {search_url}")
            
            driver.get(search_url)
            time.sleep(5)  # ページの読み込みを待つ
            
            # 検索結果ページのテキストを取得
            page_text = driver.find_element(By.TAG_NAME, "body").text
            
            # LLMを使用して年商を抽出
            prompt = f"""
以下のGoogle検索結果から、{company_name}の年商を抽出してください。
年商は以下のような形式で表現されることが多いです：
- "売上高" "年商" "年間売上高"などの後に続く数値
- "○○億円" "○○万円"などの形式
- "連結売上高" "単体売上高"などの区別がある場合は連結を優先

検索結果:
{page_text}

回答は数値のみを返してください（例："5,000億円" "100億円"）
年商が不明な場合は空文字列を返してください。
"""
            response = self.model.generate_content(prompt)
            annual_sales = response.text.strip()
            
            # 余分な文字を削除
            annual_sales = annual_sales.strip('[]\'"{}')
            return annual_sales
            
        except Exception as e:
            self.logger.error(f"Error extracting annual sales: {str(e)}")
            return ""
    
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
                
            # 企業情報が含まれる可能性の高いセクションに優先度を置く
            priority_content = []
            
            # 企業情報を含む可能性の高いキーワード
            company_keywords = [
                '企業概要', '会社概要', '事業内容', '業種', '業態',
                '売上高', '年商', '年間売上', '連結売上', '事業領域',
                '主な事業', '主要な事業', '事業分野'
            ]
            
            # テーブルから企業情報を抽出 (テーブルを最優先)
            for table in soup.find_all('table'):
                # テーブルのHTMLを保存して後で処理
                table_html = str(table)
                table_text = []
                for row in table.find_all('tr'):
                    cells = [cell.get_text().strip() for cell in row.find_all(['td', 'th'])]
                    if cells and any(keyword in ' '.join(cells).lower() for keyword in company_keywords):
                        table_text.append(' | '.join(cells))
                
                if table_text:
                    # テーブルデータを文字列化
                    table_data = "\n".join(table_text)
                    # テーブルHTMLと文字列化データの両方を追加
                    priority_content.append(f"テーブルデータ:\n{table_data}\n\nテーブルHTML:\n{table_html}")
            
            # 見出し要素を探す
            for heading in soup.find_all(['h1', 'h2', 'h3', 'h4', 'h5', 'h6']):
                heading_text = heading.get_text().strip()
                if any(keyword in heading_text for keyword in company_keywords):
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
                
                # クラス名、ID、またはテキスト内容に企業情報関連のキーワードが含まれているセクションを探す
                if any(any(keyword in str(attr).lower() for keyword in company_keywords) 
                       for attr in [section_class, section_id, section_text]):
                    # セクションのHTMLと平文の両方を保存
                    section_html = str(section)
                    section_plain = section.get_text().strip()
                    priority_content.append(f"セクションHTML:\n{section_html}\n\nセクション平文:\n{section_plain}")
            
            # DLリストから企業情報を抽出
            for dl in soup.find_all('dl'):
                dl_html = str(dl)
                dl_text = []
                dts = dl.find_all('dt')
                dds = dl.find_all('dd')
                
                if len(dts) == len(dds):  # dtとddの数が一致する場合のみ
                    for dt, dd in zip(dts, dds):
                        dt_text = dt.get_text().strip()
                        dd_text = dd.get_text().strip()
                        if any(keyword in dt_text.lower() for keyword in company_keywords) or any(keyword in dd_text.lower() for keyword in company_keywords):
                            dl_text.append(f"{dt_text}: {dd_text}")
                    
                    if dl_text:
                        priority_content.append(f"DLデータ:\n" + "\n".join(dl_text) + f"\n\nDL HTML:\n{dl_html}")
            
            # ULリストから企業情報を抽出
            for ul in soup.find_all('ul'):
                ul_html = str(ul)
                ul_items = []
                list_items = ul.find_all('li')
                
                # リスト内のテキストをチェック
                list_text = ul.get_text().lower()
                if any(keyword in list_text for keyword in company_keywords):
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
以下は企業の基本情報を含むウェブページからの抽出テキストです。
以下の情報を特定してください：
1. 業種・事業内容
2. 売上高・年商情報

特に注意: 数値や業種の表記は正確にそのまま抽出してください。

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
5. "竹林 元也"と"竹林 基哉"のような似た名前が出てきた場合は、オリジナルの表記を正確に保持すること
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
- 人名の漢字を変更しないこと。例えば"竹林 基哉"を"竹林 元也"に変更しないこと
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
        Write company data to the output file
        Args:
            company_data (list): List of company data rows
        """
        try:
            with open(self.output_file, 'a', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                for row in company_data:
                    if len(row) >= 4:
                        # Clean up the data
                        company_name = self.clean_text(row[0])
                        industry = self.clean_text(row[1])
                        website = self.clean_text(row[2])
                        sales = self.clean_text(row[3])
                        
                        # Write the cleaned data
                        cleaned_row = [company_name, industry, website, sales]
                        writer.writerow(cleaned_row)
                        self.logger.debug(f"Wrote data for {company_name}: {cleaned_row}")
                    else:
                        self.logger.warning(f"Skipping row with insufficient data: {row}")
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

    def find_official_website(self, driver, company_name: str) -> str:
        """
        Search for the company's official website URL using Google search
        Args:
            driver: Selenium WebDriver instance
            company_name (str): Company name to search for
        Returns:
            str: Official website URL or empty string if not found
        """
        try:
            # Construct search query for official website
            search_query = f"{company_name} 公式サイト"
            search_url = f"https://www.google.co.jp/search?q={quote_plus(search_query)}&hl=ja"
            self.logger.debug(f"Searching for official website: {search_url}")

            # Access Google search page
            driver.get(search_url)
            time.sleep(5)  # Wait for page load

            # Try multiple selectors for search results
            selectors = [
                "div.g",  # Traditional selector
                "div[data-sokoban-container]",  # New selector
                "div.tF2Cxc",  # Another new selector
                "div.yuRUbf",  # Another new selector
                "#search .g",  # Another common selector
                ".rc",  # Previously used selector
                "div.hlcw0c",  # Mobile view selector
                "div.MjjYud",  # New structure
                "h3.LC20lb",  # Heading search
                "a[href]"  # Direct link search
            ]

            # Find search results
            search_results = None
            for selector in selectors:
                try:
                    WebDriverWait(driver, 3).until(
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

            # Get the first result's URL
            try:
                first_result = search_results[0]
                link = None
                
                # Try to find a link element
                try:
                    link = first_result.find_element(By.TAG_NAME, "a")
                except:
                    # If no direct link found, try to find any link in the result
                    links = first_result.find_elements(By.TAG_NAME, "a")
                    if links:
                        link = links[0]

                if not link:
                    return ""

                official_url = link.get_attribute("href")
                if not official_url:
                    return ""

                # Clean the URL
                clean_url = self.clean_url(official_url)
                self.logger.debug(f"Found official website URL: {clean_url}")
                return clean_url

            except Exception as e:
                self.logger.error(f"Failed to extract official website URL: {str(e)}")
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

    def clean_text(self, text: str) -> str:
        """Clean text by removing prefixes and formatting"""
        text = text.replace('"', '').replace("'", "").replace("{", "").replace("}", "").strip()
        prefixes = ['業種:', 'industry:', 'revenue:', '年商:', 'year_sales:', 'annual_revenue:']
        for prefix in prefixes:
            if text.startswith(prefix):
                text = text.replace(prefix, '').strip()
        if text.startswith('[') and text.endswith(']'):
            text = text[1:-1].strip()
        return text

if __name__ == "__main__":
    GOOGLE_API_KEY = "AIzaSyAAZratHSyw71DkAyk_WHcUkwkXW-yksGk"
    scraper = CompanySalesScraper(api_key=GOOGLE_API_KEY)
    scraper.start_scraping()