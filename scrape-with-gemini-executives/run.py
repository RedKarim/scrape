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
        self.input_file = './data/no_executives.csv'
        self.output_file = './data/output_トリドール.csv'

        # ログ設定
        logging.basicConfig(
            filename='sales_scraper.log',
            filemode='a',
            format='%(asctime)s - %(levelname)s - %(message)s',
            level=logging.INFO
        )

        self.chrome_options = webdriver.ChromeOptions()
        # ヘッドレスモードを有効にする場合はコメントを外してください
        self.chrome_options.add_argument('--headless=new')
        self.chrome_options.add_argument('--no-sandbox')
        self.chrome_options.add_argument('--disable-dev-shm-usage')
        self.chrome_options.add_argument('--disable-gpu')
        self.chrome_options.add_argument('--window-size=1920,1080')
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
    
    def start_scraping(self):
        try:
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
                        print(f"会社名: {company_name} のスクレイピングを開始します。")
                        self.scrape_company_data(driver, company_name)
                        # ランダムな遅延（5〜10秒）
                        time.sleep(random.uniform(4, 8))
        except Exception as e:
            logging.critical(f"スクレイピングが予期せず終了しました: {str(e)}")
            print(f"エラーが発生しました: {str(e)}")
        finally:
            # バッファに残ったデータを書き込む
            if self.buffer:
                self.write_to_file()
            logging.info("スクレイピングプロセスが完了しました。")
            print(f"スクレイピングが完了しました。結果は {self.output_file} に保存されています。")
        
    def scrape_company_data(self, driver, company_name):
        search_query = f"{company_name} 役員"
        search_url = f"https://www.google.com/search?q={quote_plus(search_query)}"
        
        logging.info(f"検索URL: {search_url}")
        print(f"検索URL: {search_url}")
        
        company_data = []  # この会社のデータを保持する一時リスト

        try:
            driver.get(search_url)
            print(f"検索ページにアクセスしました: {search_url}")
            time.sleep(random.uniform(4, 8))

            # URLの取得（検索結果の最初のリンク）
            try:
                results = WebDriverWait(driver, 10).until(
                    EC.presence_of_all_elements_located((By.CSS_SELECTOR, 'div.yuRUbf a'))
                )
                url = results[0].get_attribute('href')
                if url:
                    logging.info(f"{company_name} のURL: {url}")
                    print(f"{company_name} のURL: {url}")
                    
                    # コンテンツ取得と役員情報抽出
                    cleaned_content = self.extract_cleaned_content(driver, url)
                    print(f"公式サイトにアクセスしました: {url}")
                    time.sleep(random.uniform(4, 8))

                    result = self.query(cleaned_content)
                    print('役員抽出結果:', result)

                    # JSON文字列を辞書に変換（もし文字列で返ってきた場合）
                    if isinstance(result, str):
                        import json
                        result = json.loads(result)

                    # 結果の処理
                    if isinstance(result, dict):
                        if 'executives' in result and result['executives']:
                            # 役員情報が存在する場合、各役員のデータを追加
                            for executive in result['executives']:
                                if executive.get('役職') and executive.get('氏名'):  # 役職と氏名の両方があるか確認
                                    company_data.append([
                                        company_name,
                                        url,
                                        executive['役職'],
                                        executive['氏名']
                                    ])
                        else:
                            # 役員情報が空の場合
                            company_data.append([
                                company_name,
                                url,
                                '役員情報なし',
                                '役員情報なし'
                            ])
                    else:
                        # 不正な形式の場合
                        company_data.append([
                            company_name,
                            url,
                            'データ形式エラー',
                            'データ形式エラー'
                        ])
                else:
                    company_data.append([
                        company_name,
                        '取得失敗',
                        'URL未設定',
                        'URL未設定'
                    ])
            except Exception as e:
                print(f"エラーが発生: {str(e)}")
                company_data.append([
                    company_name,
                    '取得失敗',
                    f'エラー: {str(e)}',
                    'エラー'
                ])
                
        except Exception as e:
            logging.error(f"{company_name} のデータ取得中にエラーが発生: {str(e)}")
            company_data.append([
                company_name,
                '取得失敗',
                f'エラー: {str(e)}',
                'エラー'
            ])
        
        # デバッグ出力を追加
        print(f"保存するデータ数: {len(company_data)}")
        print(f"保存するデータ内容: {company_data}")
        
        # 会社のデータを即座に書き込む
        if company_data:
            self.write_company_data(company_data)
            print(f"{company_name} のデータを書き込みました。")
    
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
                print(f"{len(company_data)}件のデータを書き込みました")
        except Exception as e:
            logging.error(f"ファイル書き込み中にエラーが発生: {str(e)}")
            print(f"ファイル書き込みエラー: {str(e)}")


if __name__ == "__main__":
    GOOGLE_API_KEY = "AIzaSyAAZratHSyw71DkAyk_WHcUkwkXW-yksGk"
    scraper = CompanySalesScraper(api_key=GOOGLE_API_KEY)
    scraper.start_scraping()