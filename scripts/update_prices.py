#!/usr/bin/env python3
import json
import time
import os
import logging
import sys
import numpy as np  # 追加
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
        
        # IQR法の設定
        self.iqr_multiplier = 1.5  # IQR法の倍率（標準的な値）
        self.minimum_data_points = 4  # IQR法適用に必要な最小データ数
        
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
        
        # 並行処理対応設定
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
        """JavaScriptを使用した検索実行"""
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

            time.sleep(2)
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

    def parse_previous_price(self, price_str):
        """前回価格を数値に変換"""
        if not price_str or price_str in ['未取得', 'undefined', '']:
            return None
        
        try:
            # カンマ区切りの数値文字列を処理
            cleaned_price = str(price_str).replace(',', '').replace(' NESO', '').strip()
            return int(cleaned_price)
        except (ValueError, TypeError):
            return None

    def detect_outliers_iqr(self, prices):
        """IQR法による外れ値検出"""
        if len(prices) < self.minimum_data_points:
            logger.info(f"データ数不足（{len(prices)}件）: IQR法をスキップ")
            return [], prices
        
        # numpy配列に変換して統計計算
        prices_array = np.array(prices)
        
        # 四分位数を計算
        Q1 = np.percentile(prices_array, 25)
        Q3 = np.percentile(prices_array, 75)
        IQR = Q3 - Q1
        
        # 外れ値の境界を計算
        lower_bound = Q1 - self.iqr_multiplier * IQR
        upper_bound = Q3 + self.iqr_multiplier * IQR
        
        # 外れ値と正常値を分離
        outliers = []
        normal_prices = []
        
        for price in prices:
            if price < lower_bound or price > upper_bound:
                outliers.append(price)
            else:
                normal_prices.append(price)
        
        logger.info(f"IQR統計: Q1={Q1:,.0f}, Q3={Q3:,.0f}, IQR={IQR:,.0f}")
        logger.info(f"外れ値境界: {lower_bound:,.0f} - {upper_bound:,.0f}")
        logger.info(f"外れ値{len(outliers)}件, 正常値{len(normal_prices)}件")
        
        return outliers, normal_prices

    def select_optimal_price(self, prices, previous_price):
        """IQR法による外れ値除去後、最安値を選定"""
        if not prices:
            return None, "価格データなし"

        logger.info(f"取得した最新5件の価格: {[f'{p:,}' for p in prices]}")
        
        if previous_price:
            logger.info(f"前回価格: {previous_price:,}")
        else:
            logger.info("前回価格: 未取得")

        # IQR法による外れ値検出
        outliers, normal_prices = self.detect_outliers_iqr(prices)
        
        # 結果をログ出力
        logger.info("IQR法による外れ値検出結果:")
        for price in prices:
            if price in outliers:
                logger.info(f"  ❌ {price:,} NESO: 外れ値")
            else:
                logger.info(f"  ✅ {price:,} NESO: 正常値")

        # 有効な価格がない場合の処理
        if not normal_prices:
            logger.warning("全ての価格が外れ値と判定されました")
            
            # 前回価格が存在する場合はそれを維持
            if previous_price and previous_price > 0:
                logger.info(f"前回価格を維持: {previous_price:,}")
                return previous_price, "前回価格維持（全価格外れ値）"
            else:
                # 前回価格もない場合は中央値を使用（最後の手段）
                median_price = int(np.median(prices))
                logger.warning(f"中央値を使用: {median_price:,}")
                return median_price, "中央値使用（全価格外れ値）"

        # 正常価格から最安値を選択
        optimal_price = min(normal_prices)
        excluded_count = len(outliers)
        
        if excluded_count > 0:
            logger.info(f"外れ値を{excluded_count}件除外")
        
        logger.info(f"選定された最適価格: {optimal_price:,}")
        
        return optimal_price, "IQR法正常価格"

    @retry_on_error(max_retries=3, delay=2)
    def update_equipment_price_with_retry(self, equipment_id, equipment_name, current_equipment_data):
        """特定の装備の価格を更新（IQR法対応版）"""
        driver = None
        try:
            # 前回価格を取得
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
                logger.info(f"Success: {equipment_name}: {optimal_price:,} NESO ({price_status})")
                return {
                    'equipment_id': equipment_id,
                    'equipment_name': equipment_name,
                    'price': optimal_price,
                    'price_status': price_status,
                    'previous_price': previous_price,
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
        """装備アイテムのバッチ処理"""
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
                
                if result.get('success'):
                    status_info = result.get('price_status', '')
                    logger.info(f"✅ {equipment_name}: {result.get('price', 'ERROR'):,} ({status_info})")
                else:
                    logger.error(f"❌ {equipment_name}: エラー")
                    
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
        """価格更新実行（IQR法対応）"""
        if self.target_items is None:
            logger.info(f"GitHub Actions price update started - Target: ALL items (parallel processing)")
        else:
            logger.info(f"GitHub Actions price update started - Target: {self.target_items} items")
        
        logger.info(f"IQR法設定: 倍率{self.iqr_multiplier}, 最小データ数{self.minimum_data_points}件")
        
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
            # 並行処理ロジック
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
                
                result = self.update_equipment_price_with_retry(
                    equipment_id, equipment_name, equipment_info
                )
                all_results.append(result)
                
                time.sleep(5)  # GitHub Actions制限対応

        # JSONデータに反映（IQR法結果を含む）
        normal_updates = 0
        outlier_updates = 0
        failed_updates = 0
        
        for result in all_results:
            if result.get('success'):
                equipment_data[result['equipment_id']]["item_price"] = f"{result['price']:,}"
                
                # 価格ステータスに応じてステータスを設定
                price_status = result.get('price_status', '')
                if '外れ値' in price_status or '維持' in price_status or '中央値' in price_status:
                    equipment_data[result['equipment_id']]["status"] = f"価格更新済み（{price_status}）"
                    outlier_updates += 1
                else:
                    equipment_data[result['equipment_id']]["status"] = "価格更新済み"
                    normal_updates += 1
                    
                # 最終更新時刻を記録
                equipment_data[result['equipment_id']]["last_updated"] = datetime.now().isoformat()
                self.updated_count += 1
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

        # 最終統計
        logger.info("=" * 50)
        logger.info("📊 IQR法価格更新統計:")
        logger.info(f"  正常更新: {normal_updates}件")
        logger.info(f"  外れ値処理: {outlier_updates}件")
        logger.info(f"  更新失敗: {failed_updates}件")
        logger.info(f"  合計処理: {total}件")
        logger.info("=" * 50)

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
