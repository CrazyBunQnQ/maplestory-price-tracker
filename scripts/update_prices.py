#!/usr/bin/env python3
import json
import time
import os
import logging
import sys
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
import re
from datetime import datetime
import functools
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def retry_on_error(max_retries=3, delay=2):
    """エラー時に指定回数リトライするデコレータ"""
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            last_exception = None
            for attempt in range(1, max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    last_exception = e
                    if attempt < max_retries:
                        logger.warning(f"Retry {attempt}/{max_retries}: {args[1] if len(args) > 1 else 'Unknown'}")
                        time.sleep(delay)
                    else:
                        logger.error(f"Max retries reached: {args[1] if len(args) > 1 else 'Unknown'}")
            raise last_exception
        return wrapper
    return decorator

class GitHubActionsUpdater:
    def __init__(self, json_file_path="data/equipment_prices.json"):
        self.json_file_path = json_file_path
        self.target_items_input = os.getenv('TARGET_ITEMS', 'ALL')
        self.updated_count = 0
        self.lock = threading.Lock()
        
        # 全件処理か制限処理かを判定
        if self.target_items_input.upper() == 'ALL':
            self.target_items = None  # 制限なし
            self.use_parallel = True  # 並行処理使用
        else:
            try:
                self.target_items = int(self.target_items_input)
                self.use_parallel = False  # シングルスレッド処理
            except ValueError:
                self.target_items = 10  # デフォルト
                self.use_parallel = False

    def setup_driver(self):
        """Seleniumドライバーの設定（GitHub Actions + 並行処理対応）"""
        chrome_options = Options()
        
        # GitHub Actions用設定
        chrome_options.add_argument("--headless=new")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--window-size=1920,1080")
        
        # paste-2.txtの並行処理対応設定
        chrome_options.add_argument("--remote-debugging-port=0")
        chrome_options.add_argument("--disable-extensions")
        chrome_options.add_argument("--disable-plugins")
        chrome_options.add_argument("--disable-logging")
        chrome_options.add_argument("--log-level=3")
        chrome_options.add_argument("--silent")
        
        # ボット検出回避設定
        chrome_options.add_argument("--disable-blink-features=AutomationControlled")
        chrome_options.add_experimental_option("excludeSwitches", ["enable-automation", "enable-logging"])
        chrome_options.add_experimental_option('useAutomationExtension', False)
        chrome_options.add_argument('--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36')
        
        try:
            service = Service('/usr/local/bin/chromedriver')
            service.log_path = os.devnull
            driver = webdriver.Chrome(service=service, options=chrome_options)
            logger.info("ChromeDriver initialized successfully")
        except Exception as e:
            logger.error(f"ChromeDriver initialization failed: {e}")
            raise

        driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
        return driver

    def search_equipment_js(self, driver, equipment_name):
        """JavaScriptを使用した検索実行（paste.txt/paste-2.txtより）"""
        try:
            driver.get("https://msu.io/navigator")
            WebDriverWait(driver, 15).until(
                lambda d: d.execute_script("return document.readyState") == "complete"
            )
            time.sleep(3)

            search_success = driver.execute_script("""
                let searchField = null;
                const selectors = [
                    '#form_search_input',
                    'input[id="form_search_input"]',
                    'input[type="text"]',
                    'input[placeholder*="search"]',
                    'input[placeholder*="Search"]'
                ];
                for (const selector of selectors) {
                    searchField = document.querySelector(selector);
                    if (searchField) break;
                }
                if (!searchField) return false;
                
                searchField.value = '';
                searchField.focus();
                searchField.value = arguments[0];
                searchField.dispatchEvent(new Event('input', { bubbles: true }));
                searchField.dispatchEvent(new Event('change', { bubbles: true }));
                
                const enterEvent = new KeyboardEvent('keydown', {
                    key: 'Enter',
                    keyCode: 13,
                    bubbles: true
                });
                searchField.dispatchEvent(enterEvent);
                return true;
            """, equipment_name)

            if not search_success:
                raise Exception("Search field not found")

            time.sleep(2)  # GitHub Actions用に短縮
            return True

        except Exception as e:
            raise Exception(f"検索エラー: {equipment_name}, {e}")

    def extract_prices(self, driver):
        """最新の価格情報5件を抽出"""
        try:
            price_elements = driver.find_elements(
                By.CSS_SELECTOR,
                "p._typography-point-body-m-medium_15szf_134._kartrider_3m7yu_9.NesoBox_text__lvOcl"
            )

            if not price_elements:
                return []

            prices = []
            for element in price_elements[:5]:
                try:
                    price_text = driver.execute_script(
                        "return arguments[0].textContent || arguments[0].innerText || '';",
                        element
                    ).strip()

                    if price_text:
                        price_match = re.search(r'[\d,]+', price_text)
                        if price_match:
                            price_str = price_match.group().replace(',', '')
                            if price_str.isdigit():
                                price = int(price_str)
                                prices.append(price)
                except Exception:
                    continue

            prices.sort()
            logger.info(f"最新5件から取得した価格: {[f'{p:,}' for p in prices]}")
            return prices

        except Exception as e:
            raise Exception(f"価格抽出エラー: {e}")

    def select_optimal_price(self, prices):
        """最新5件から100,000以下を除外し最安値を選定"""
        if not prices:
            return None

        logger.info(f"取得した最新5件の価格: {[f'{p:,}' for p in prices]}")

        filtered_prices = [price for price in prices if price > 100000]

        if not filtered_prices:
            logger.warning("全ての価格が100,000以下のため、元の価格を使用")
            return prices[0] if prices else None
        else:
            excluded_count = len(prices) - len(filtered_prices)
            if excluded_count > 0:
                logger.info(f"100,000以下の価格を{excluded_count}件除外")

        optimal_price = filtered_prices[0]
        logger.info(f"選定された最安値: {optimal_price:,}")
        
        return optimal_price

    @retry_on_error(max_retries=3, delay=2)
    def update_equipment_price_with_retry(self, equipment_id, equipment_name):
        """特定の装備の価格を更新（paste.txt/paste-2.txtベース）"""
        driver = None
        try:
            driver = self.setup_driver()
            
            if not self.search_equipment_js(driver, equipment_name):
                raise Exception("検索失敗")

            prices = self.extract_prices(driver)
            if not prices:
                raise Exception("価格が見つかりません")

            optimal_price = self.select_optimal_price(prices)
            if optimal_price:
                logger.info(f"Success: {equipment_name}: {optimal_price:,} NESO")
                return {
                    'equipment_id': equipment_id,
                    'equipment_name': equipment_name,
                    'price': optimal_price,
                    'success': True
                }
            else:
                raise Exception("適切な価格が選定できません")

        except Exception as e:
            logger.error(f"Failed: {equipment_name}: {str(e)}")
            return {
                'equipment_id': equipment_id,
                'equipment_name': equipment_name,
                'success': False,
                'error': str(e)
            }
        finally:
            if driver:
                try:
                    driver.quit()
                except:
                    pass

    def process_equipment_batch(self, equipment_items):
        """装備アイテムのバッチ処理（paste-2.txtの並行処理ロジック）"""
        results = []
        for equipment_id, equipment_info in equipment_items:
            equipment_name = equipment_info.get("item_name", "")
            if not equipment_name:
                continue

            try:
                result = self.update_equipment_price_with_retry(equipment_id, equipment_name)
                results.append(result)
                logger.info(f"✅ {equipment_name}: {result.get('price', 'ERROR'):,}")
            except Exception as e:
                results.append({
                    'equipment_id': equipment_id,
                    'equipment_name': equipment_name,
                    'success': False,
                    'error': str(e)
                })
                logger.error(f"❌ {equipment_name}: エラー")

            time.sleep(3)  # GitHub Actions制限対応

        return results

    def run_update(self):
        """価格更新実行（全件 or 制限件数対応）"""
        if self.target_items is None:
            logger.info(f"GitHub Actions price update started - Target: ALL items (parallel processing)")
        else:
            logger.info(f"GitHub Actions price update started - Target: {self.target_items} items")
        
        try:
            with open(self.json_file_path, 'r', encoding='utf-8') as f:
                equipment_data = json.load(f)
        except Exception as e:
            logger.error(f"JSON loading failed: {e}")
            sys.exit(1)

        items = [(k, v) for k, v in equipment_data.items() 
                if v.get("item_name") and k != ""]
        
        # 処理対象を制限（必要に応じて）
        if self.target_items is not None:
            items = items[:self.target_items]

        total = len(items)
        logger.info(f"Processing {total} items")

        if self.use_parallel and total > 10:
            # paste-2.txtの並行処理ロジック
            chunk = total // 4
            batches = [
                items[0:chunk],
                items[chunk:chunk*2], 
                items[chunk*2:chunk*3],
                items[chunk*3:]
            ]

            logger.info(f"並行処理開始: 4ワーカー, 合計 {total}件")

            all_results = []
            with ThreadPoolExecutor(max_workers=4) as executor:
                futures = {
                    executor.submit(self.process_equipment_batch, batch): idx
                    for idx, batch in enumerate(batches, start=1)
                }

                for future in as_completed(futures):
                    batch_no = futures[future]
                    try:
                        results = future.result()
                        all_results.extend(results)
                        logger.info(f"✅ バッチ{batch_no} 完了")
                    except Exception as e:
                        logger.error(f"❌ バッチ{batch_no} エラー: {e}")

        else:
            # シングルスレッド処理
            all_results = []
            for i, (equipment_id, equipment_info) in enumerate(items, 1):
                equipment_name = equipment_info.get("item_name", "")
                logger.info(f"[{i}/{total}] Processing: {equipment_name}")
                
                result = self.update_equipment_price_with_retry(equipment_id, equipment_name)
                all_results.append(result)
                
                time.sleep(5)  # GitHub Actions制限対応

        # JSONデータに反映
        for result in all_results:
            if result.get('success'):
                equipment_data[result['equipment_id']]["item_price"] = f"{result['price']:,}"
                equipment_data[result['equipment_id']]["status"] = "価格更新済み"
                self.updated_count += 1
            else:
                equipment_data[result['equipment_id']]["status"] = "価格取得失敗"

        try:
            with open(self.json_file_path, 'w', encoding='utf-8') as f:
                json.dump(equipment_data, f, ensure_ascii=False, indent=2)
            logger.info(f"JSON saved successfully: {self.updated_count} items updated")
        except Exception as e:
            logger.error(f"Failed to save JSON: {e}")
            sys.exit(1)

        logger.info(f"Update completed: {self.updated_count}/{total} items successful")
        sys.exit(0)

def main():
    updater = GitHubActionsUpdater()
    try:
        updater.run_update()
    except Exception as e:
        logger.error(f"System error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
