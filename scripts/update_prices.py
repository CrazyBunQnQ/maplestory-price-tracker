#!/usr/bin/env python3
import json
import time
import os
import logging
import sys
import numpy as np
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import re
from datetime import datetime
import functools
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

# webdriver-managerの安全なインポート
try:
    from webdriver_manager.chrome import ChromeDriverManager
    WEBDRIVER_MANAGER_AVAILABLE = True
except ImportError:
    WEBDRIVER_MANAGER_AVAILABLE = False

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def retry_on_error(max_retries=2, delay=1):
    """エラー時にリトライするデコレータ（高速化版）"""
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
        
        # 高速化設定
        self.iqr_multiplier = 1.5
        self.minimum_data_points = 3  # 削減
        self.minimum_price_threshold = 10000
        
        # 並行処理復活（高速化優先）
        if self.target_items_input.upper() == 'ALL':
            self.target_items = None
            self.use_parallel = True  # 並行処理を復活
            self.max_workers = 6  # ワーカー数増加
        else:
            try:
                self.target_items = int(self.target_items_input)
                self.use_parallel = self.target_items > 20
                self.max_workers = 4
            except ValueError:
                self.target_items = 10
                self.use_parallel = False
                self.max_workers = 2

    def setup_driver(self):
        """Seleniumドライバーの設定（高速化版）"""
        chrome_options = Options()
        
        # GitHub Actions用基本設定（最適化）
        chrome_options.add_argument("--headless=new")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--window-size=1920,1080")
        
        # 高速化設定
        chrome_options.add_argument("--disable-web-security")
        chrome_options.add_argument("--disable-features=VizDisplayCompositor")
        chrome_options.add_argument("--disable-ipc-flooding-protection")
        chrome_options.add_argument("--disable-renderer-backgrounding")
        chrome_options.add_argument("--disable-backgrounding-occluded-windows")
        chrome_options.add_argument("--disable-background-timer-throttling")
        
        # リソース節約
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
        chrome_options.add_argument('--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36')
        
        # メモリ制限（並列処理対応）
        chrome_options.add_argument("--memory-pressure-off")
        chrome_options.add_argument("--max_old_space_size=2048")
        
        try:
            # システムのChromeDriverを使用
            service = Service('/usr/local/bin/chromedriver')
            service.log_path = os.devnull
            
            driver = webdriver.Chrome(service=service, options=chrome_options)
            
            # タイムアウト設定（高速化）
            driver.set_page_load_timeout(20)
            driver.implicitly_wait(5)
            
            # ボット検出対策（最小限）
            driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
            
            return driver
            
        except Exception as e:
            logger.error(f"ChromeDriver initialization failed: {e}")
            raise

    def search_equipment_js(self, driver, equipment_name):
        """JavaScriptを使用した検索実行（高速化版）"""
        try:
            # ページロード
            driver.get("https://msu.io/navigator")
            
            # ページの完全読み込みを待機（短縮）
            WebDriverWait(driver, 15).until(
                lambda d: d.execute_script("return document.readyState") == "complete"
            )
            
            # 安定化待機（短縮）
            time.sleep(2)
            
            # 検索フィールドの検出と入力（簡略化）
            search_success = driver.execute_script("""
                const searchSelectors = [
                    '#form_search_input',
                    'input[id="form_search_input"]',
                    'input[type="text"]'
                ];
                
                let searchField = null;
                for (const selector of searchSelectors) {
                    searchField = document.querySelector(selector);
                    if (searchField && searchField.offsetParent !== null) {
                        break;
                    }
                }
                
                if (!searchField) return false;
                
                searchField.value = '';
                searchField.focus();
                searchField.value = arguments[0];
                searchField.dispatchEvent(new Event('input', { bubbles: true }));
                
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

            # 検索結果の読み込みを待機（短縮）
            time.sleep(3)
            
            return True

        except Exception as e:
            raise Exception(f"検索エラー: {equipment_name}, {e}")

    def extract_prices(self, driver):
        """価格情報を抽出（高速化版）"""
        try:
            # 主要セレクターのみ使用
            price_selectors = [
                "p._typography-point-body-m-medium_15szf_134._kartrider_3m7yu_9.NesoBox_text__lvOcl",
                "p._typography-point-body-m-medium_15szf_134"
            ]
            
            all_prices = []
            
            for selector in price_selectors:
                try:
                    price_elements = driver.find_elements(By.CSS_SELECTOR, selector)
                    
                    if price_elements:
                        for element in price_elements:
                            try:
                                price_text = driver.execute_script(
                                    "return arguments[0].textContent || '';",
                                    element
                                ).strip()

                                if price_text:
                                    price_match = re.search(r'[\d,]+', price_text)
                                    if price_match:
                                        price_str = price_match.group().replace(',', '')
                                        if price_str.isdigit():
                                            price = int(price_str)
                                            if price > 1000:
                                                all_prices.append(price)
                            except Exception:
                                continue
                        
                        if all_prices:
                            break
                            
                except Exception:
                    continue

            # フィルタリング処理（簡略化）
            filtered_prices = [price for price in all_prices if price > self.minimum_price_threshold]
            filtered_prices.sort()
            final_prices = filtered_prices[:5]
            
            if len(final_prices) < 2:
                logger.warning(f"価格データが不足（{len(final_prices)}件）")
                
            return final_prices

        except Exception as e:
            raise Exception(f"価格抽出エラー: {e}")

    def parse_previous_price(self, price_str):
        """前回価格を数値に変換"""
        if not price_str or price_str in ['未取得', 'undefined', '']:
            return None
        
        try:
            cleaned_price = str(price_str).replace(',', '').replace(' NESO', '').strip()
            return int(cleaned_price)
        except (ValueError, TypeError):
            return None

    def detect_outliers_iqr(self, prices):
        """IQR法による外れ値検出（簡略化）"""
        if len(prices) < self.minimum_data_points:
            return [], prices
        
        prices_array = np.array(prices)
        Q1 = np.percentile(prices_array, 25)
        Q3 = np.percentile(prices_array, 75)
        IQR = Q3 - Q1
        
        if IQR == 0:
            return [], prices
        
        lower_bound = Q1 - self.iqr_multiplier * IQR
        upper_bound = Q3 + self.iqr_multiplier * IQR
        
        outliers = []
        normal_prices = []
        
        for price in prices:
            if price < lower_bound or price > upper_bound:
                outliers.append(price)
            else:
                normal_prices.append(price)
        
        return outliers, normal_prices

    def select_optimal_price(self, prices, previous_price):
        """最適価格の選定（簡略化）"""
        if not prices:
            return None, "価格データなし"

        outliers, normal_prices = self.detect_outliers_iqr(prices)
        
        if not normal_prices:
            if previous_price and previous_price > self.minimum_price_threshold:
                return previous_price, "前回価格維持"
            else:
                return int(np.median(prices)), "中央値使用"

        optimal_price = min(normal_prices)
        return optimal_price, "正常価格"

    @retry_on_error(max_retries=2, delay=1)
    def update_equipment_price_with_retry(self, equipment_id, equipment_name, current_equipment_data):
        """装備価格の更新（高速化版）"""
        driver = None
        try:
            previous_price = self.parse_previous_price(
                current_equipment_data.get('item_price', '')
            )
            
            driver = self.setup_driver()
            
            if not self.search_equipment_js(driver, equipment_name):
                raise Exception("検索失敗")

            prices = self.extract_prices(driver)
            if not prices:
                raise Exception("価格が見つかりません")

            optimal_price, price_status = self.select_optimal_price(prices, previous_price)
            
            if optimal_price:
                with self.lock:
                    logger.info(f"Success: {equipment_name}: {optimal_price:,} NESO")
                return {
                    'equipment_id': equipment_id,
                    'equipment_name': equipment_name,
                    'price': optimal_price,
                    'price_status': price_status,
                    'success': True
                }
            else:
                raise Exception("適切な価格が選定できません")

        except Exception as e:
            with self.lock:
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
        """装備アイテムのバッチ処理（高速化版）"""
        results = []
        for equipment_id, equipment_info in equipment_items:
            equipment_name = equipment_info.get("item_name", "")
            if not equipment_name:
                continue

            try:
                result = self.update_equipment_price_with_retry(
                    equipment_id, equipment_name, equipment_info
                )
                results.append(result)
                    
            except Exception as e:
                results.append({
                    'equipment_id': equipment_id,
                    'equipment_name': equipment_name,
                    'success': False,
                    'error': str(e)
                })

            # 高速化：待機時間短縮
            time.sleep(1)

        return results

    def run_update(self):
        """価格更新実行（並列処理復活版）"""
        start_time = time.time()
        
        if self.target_items is None:
            logger.info("GitHub Actions price update started - Target: ALL items (parallel processing)")
        else:
            logger.info(f"GitHub Actions price update started - Target: {self.target_items} items")
        
        logger.info("高速化設定:")
        logger.info(f"  並行処理: {self.use_parallel} (ワーカー数: {self.max_workers})")
        logger.info(f"  最小データ数: {self.minimum_data_points}件")
        logger.info(f"  リトライ回数: 2回")
        
        try:
            if not os.path.exists("data"):
                os.makedirs("data", exist_ok=True)
            
            with open(self.json_file_path, 'r', encoding='utf-8') as f:
                equipment_data = json.load(f)
        except Exception as e:
            logger.error(f"JSON loading failed: {e}")
            sys.exit(1)

        items = [(k, v) for k, v in equipment_data.items() 
                if v.get("item_name") and k != ""]
        
        if self.target_items is not None:
            items = items[:self.target_items]

        total = len(items)
        logger.info(f"Processing {total} items")

        # 並行処理復活
        if self.use_parallel and total > 10:
            # バッチサイズを動的に調整
            batch_size = max(10, total // self.max_workers)
            batches = [items[i:i + batch_size] for i in range(0, len(items), batch_size)]

            logger.info(f"並行処理開始: {self.max_workers}ワーカー, {len(batches)}バッチ")

            all_results = []
            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                futures = {
                    executor.submit(self.process_equipment_batch, batch): idx
                    for idx, batch in enumerate(batches, start=1)
                }

                for future in as_completed(futures):
                    batch_no = futures[future]
                    try:
                        results = future.result()
                        all_results.extend(results)
                        logger.info(f"バッチ{batch_no} 完了 ({len(results)}件)")
                    except Exception as e:
                        logger.error(f"バッチ{batch_no} エラー: {e}")

        else:
            # シングルスレッド処理
            all_results = []
            for i, (equipment_id, equipment_info) in enumerate(items, 1):
                equipment_name = equipment_info.get("item_name", "")
                logger.info(f"[{i}/{total}] Processing: {equipment_name}")
                
                result = self.update_equipment_price_with_retry(
                    equipment_id, equipment_name, equipment_info
                )
                all_results.append(result)
                
                # 高速化：短い待機時間
                time.sleep(2)

        # JSONデータに反映
        normal_updates = 0
        failed_updates = 0
        
        for result in all_results:
            if result.get('success'):
                equipment_data[result['equipment_id']]["item_price"] = f"{result['price']:,}"
                equipment_data[result['equipment_id']]["status"] = "価格更新済み"
                equipment_data[result['equipment_id']]["last_updated"] = datetime.now().isoformat()
                self.updated_count += 1
                normal_updates += 1
            else:
                equipment_data[result['equipment_id']]["status"] = "価格取得失敗"
                failed_updates += 1

        try:
            with open(self.json_file_path, 'w', encoding='utf-8') as f:
                json.dump(equipment_data, f, ensure_ascii=False, indent=2)
            logger.info(f"JSON saved successfully: {self.updated_count} items updated")
        except Exception as e:
            logger.error(f"Failed to save JSON: {e}")
            sys.exit(1)

        elapsed_time = time.time() - start_time
        logger.info("=" * 50)
        logger.info("📊 高速並列処理統計:")
        logger.info(f"  実行時間: {elapsed_time:.1f}秒")
        logger.info(f"  正常更新: {normal_updates}件")
        logger.info(f"  更新失敗: {failed_updates}件")
        logger.info(f"  合計処理: {total}件")
        logger.info(f"  処理速度: {total/elapsed_time:.1f}件/秒")
        logger.info("=" * 50)

        logger.info(f"Update completed: {self.updated_count}/{total} items successful")

def main():
    updater = GitHubActionsUpdater()
    try:
        updater.run_update()
    except Exception as e:
        logger.error(f"System error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
