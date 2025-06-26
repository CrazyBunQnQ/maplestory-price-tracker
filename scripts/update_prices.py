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
import re
from datetime import datetime
import functools
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

# webdriver-managerのインポートを安全に処理
try:
    from webdriver_manager.chrome import ChromeDriverManager
    WEBDRIVER_MANAGER_AVAILABLE = True
except ImportError:
    WEBDRIVER_MANAGER_AVAILABLE = False
    logging.warning("webdriver-manager not available, using fallback method")

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def retry_on_error(max_retries=5, delay=3):
    """エラー時に指定回数リトライするデコレータ（強化版）"""
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
                        time.sleep(delay * attempt)
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
        self.iqr_multiplier = 1.5
        self.minimum_data_points = 4
        self.minimum_price_threshold = 10000
        
        # 全件処理か制限処理かを判定
        if self.target_items_input.upper() == 'ALL':
            self.target_items = None
            self.use_parallel = True
        else:
            try:
                self.target_items = int(self.target_items_input)
                self.use_parallel = False
            except ValueError:
                self.target_items = 10
                self.use_parallel = False

    def setup_driver(self):
        """Seleniumドライバーの設定（GitHub Actions強化版）"""
        chrome_options = Options()
        
        # GitHub Actions用基本設定
        chrome_options.add_argument("--headless=new")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--window-size=1920,1080")
        chrome_options.add_argument("--disable-web-security")
        chrome_options.add_argument("--allow-running-insecure-content")
        chrome_options.add_argument("--disable-features=VizDisplayCompositor")
        chrome_options.add_argument("--disable-ipc-flooding-protection")
        chrome_options.add_argument("--disable-renderer-backgrounding")
        chrome_options.add_argument("--disable-backgrounding-occluded-windows")
        chrome_options.add_argument("--disable-client-side-phishing-detection")
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
        
        # ChromeDriverの初期化（複数の方法を試行）
        driver = None
        methods = []
        
        # 方法1: WebDriverManagerを使用
        if WEBDRIVER_MANAGER_AVAILABLE:
            methods.append(("WebDriverManager", lambda: Service(ChromeDriverManager().install())))
        
        # 方法2: システムにインストールされたChromeDriverを使用
        chromedriver_paths = [
            '/usr/local/bin/chromedriver',
            '/usr/bin/chromedriver',
            'chromedriver'
        ]
        
        for path in chromedriver_paths:
            methods.append((f"System ChromeDriver ({path})", lambda p=path: Service(p)))
        
        # 各方法を順番に試行
        for method_name, service_func in methods:
            try:
                logger.info(f"Trying {method_name}...")
                service = service_func()
                service.log_path = os.devnull
                driver = webdriver.Chrome(service=service, options=chrome_options)
                
                # 追加のJavaScript設定
                driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
                driver.execute_script("Object.defineProperty(navigator, 'languages', {get: () => ['en-US', 'en']})")
                driver.execute_script("Object.defineProperty(navigator, 'plugins', {get: () => [1, 2, 3, 4, 5]})")
                
                logger.info(f"✅ ChromeDriver initialized successfully with {method_name}")
                return driver
                
            except Exception as e:
                logger.warning(f"❌ {method_name} failed: {e}")
                if driver:
                    try:
                        driver.quit()
                    except:
                        pass
                driver = None
                continue
        
        # 全ての方法が失敗した場合
        raise Exception("All ChromeDriver initialization methods failed")

    def search_equipment_js(self, driver, equipment_name):
        """JavaScriptを使用した検索実行（改善版）"""
        max_attempts = 3
        
        for attempt in range(max_attempts):
            try:
                logger.info(f"検索試行 {attempt + 1}/{max_attempts}: {equipment_name}")
                
                driver.get("https://msu.io/navigator")
                
                # ページ読み込み完了を確実に待機
                WebDriverWait(driver, 30).until(
                    lambda d: d.execute_script("return document.readyState") == "complete"
                )
                
                time.sleep(5)
                
                # JavaScriptによる検索実行
                search_success = driver.execute_script("""
                    let searchField = null;
                    const selectors = [
                        '#form_search_input',
                        'input[id="form_search_input"]',
                        'input[type="text"]',
                        'input[placeholder*="search"]',
                        'input[placeholder*="Search"]'
                    ];
                    
                    for (let i = 0; i < 10; i++) {
                        for (const selector of selectors) {
                            searchField = document.querySelector(selector);
                            if (searchField && searchField.offsetParent !== null) break;
                        }
                        if (searchField && searchField.offsetParent !== null) break;
                        await new Promise(resolve => setTimeout(resolve, 500));
                    }
                    
                    if (!searchField || searchField.offsetParent === null) return false;
                    
                    searchField.value = '';
                    searchField.focus();
                    searchField.value = arguments[0];
                    
                    ['input', 'change', 'keyup'].forEach(eventType => {
                        searchField.dispatchEvent(new Event(eventType, { bubbles: true }));
                    });
                    
                    const enterEvent = new KeyboardEvent('keydown', {
                        key: 'Enter',
                        keyCode: 13,
                        bubbles: true
                    });
                    searchField.dispatchEvent(enterEvent);
                    return true;
                """, equipment_name)

                if not search_success:
                    if attempt < max_attempts - 1:
                        logger.warning(f"検索フィールドが見つかりません。再試行します...")
                        time.sleep(3)
                        continue
                    else:
                        raise Exception("Search field not found after all attempts")

                time.sleep(5)
                return True

            except Exception as e:
                if attempt < max_attempts - 1:
                    logger.warning(f"検索試行 {attempt + 1} 失敗: {e}. 再試行します...")
                    time.sleep(5)
                else:
                    raise Exception(f"検索エラー: {equipment_name}, {e}")
        
        return False

    def extract_prices(self, driver):
        """価格情報を抽出（改善版）"""
        try:
            # 複数のセレクターを試行
            price_selectors = [
                "p._typography-point-body-m-medium_15szf_134._kartrider_3m7yu_9.NesoBox_text__lvOcl",
                "p[class*='NesoBox_text']",
                "p[class*='_typography-point-body-m-medium']",
                "p[class*='_kartrider_']",
                ".NesoBox_text__lvOcl"
            ]
            
            price_elements = []
            for selector in price_selectors:
                try:
                    elements = driver.find_elements(By.CSS_SELECTOR, selector)
                    if elements:
                        price_elements = elements
                        logger.info(f"価格要素を発見: {selector} ({len(elements)}件)")
                        break
                except Exception as e:
                    logger.warning(f"セレクター {selector} でエラー: {e}")
                    continue

            if not price_elements:
                logger.warning("価格要素が見つかりません")
                return []

            all_prices = []
            for element in price_elements:
                try:
                    price_text = driver.execute_script(
                        "return arguments[0].textContent || arguments[0].innerText || '';",
                        element
                    ).strip()

                    if price_text:
                        price_patterns = [
                            r'(\d{1,3}(?:,\d{3})*)',
                            r'(\d+)',
                        ]
                        
                        for pattern in price_patterns:
                            price_match = re.search(pattern, price_text)
                            if price_match:
                                price_str = price_match.group(1).replace(',', '')
                                if price_str.isdigit():
                                    price = int(price_str)
                                    all_prices.append(price)
                                    break
                except Exception as e:
                    logger.warning(f"価格解析エラー: {e}")
                    continue

            # フィルタリング処理
            filtered_prices = [price for price in all_prices if price > self.minimum_price_threshold]
            excluded_count = len(all_prices) - len(filtered_prices)
            
            if excluded_count > 0:
                excluded_prices = [price for price in all_prices if price <= self.minimum_price_threshold]
                logger.info(f"{self.minimum_price_threshold:,}以下の価格を{excluded_count}件除外")
            
            filtered_prices.sort()
            final_prices = filtered_prices[:5]
            
            logger.info(f"フィルタリング後の価格（5件まで）: {[f'{p:,}' for p in final_prices]}")
            
            return final_prices

        except Exception as e:
            logger.error(f"価格抽出エラー: {e}")
            return []

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
        """IQR法による外れ値検出"""
        if len(prices) < self.minimum_data_points:
            logger.info(f"データ数不足（{len(prices)}件）: IQR法をスキップ")
            return [], prices
        
        prices_array = np.array(prices)
        Q1 = np.percentile(prices_array, 25)
        Q3 = np.percentile(prices_array, 75)
        IQR = Q3 - Q1
        
        if IQR == 0:
            logger.info("IQR=0（全価格が同一）: 外れ値なしと判定")
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
        
        logger.info(f"IQR統計: Q1={Q1:,.0f}, Q3={Q3:,.0f}, IQR={IQR:,.0f}")
        logger.info(f"外れ値境界: {lower_bound:,.0f} - {upper_bound:,.0f}")
        logger.info(f"外れ値{len(outliers)}件, 正常値{len(normal_prices)}件")
        
        return outliers, normal_prices

    def select_optimal_price(self, prices, previous_price):
        """最適価格の選定"""
        if not prices:
            return None, "価格データなし"

        logger.info(f"価格データ（5件まで）: {[f'{p:,}' for p in prices]}")
        
        if previous_price:
            logger.info(f"前回価格: {previous_price:,}")
        else:
            logger.info("前回価格: 未取得")

        outliers, normal_prices = self.detect_outliers_iqr(prices)
        
        logger.info("IQR法による外れ値検出結果:")
        for price in prices:
            if price in outliers:
                logger.info(f"  ❌ {price:,} NESO: 外れ値")
            else:
                logger.info(f"  ✅ {price:,} NESO: 正常値")

        if not normal_prices:
            logger.warning("全ての価格が外れ値と判定されました")
            
            if previous_price and previous_price > self.minimum_price_threshold:
                logger.info(f"前回価格を維持: {previous_price:,}")
                return previous_price, "前回価格維持（全価格外れ値）"
            else:
                median_price = int(np.median(prices))
                logger.warning(f"中央値を使用: {median_price:,}")
                return median_price, "中央値使用（全価格外れ値）"

        optimal_price = min(normal_prices)
        excluded_count = len(outliers)
        
        if excluded_count > 0:
            logger.info(f"IQR法で{excluded_count}件を外れ値として除外")
        
        logger.info(f"選定された最適価格: {optimal_price:,}")
        
        return optimal_price, "二段階フィルタリング正常価格"

    @retry_on_error(max_retries=5, delay=3)
    def update_equipment_price_with_retry(self, equipment_id, equipment_name, current_equipment_data):
        """装備価格の更新"""
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
            time.sleep(2)

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

            time.sleep(3)

        return results

    def run_update(self):
        """価格更新実行"""
        if self.target_items is None:
            logger.info("GitHub Actions price update started - Target: ALL items")
        else:
            logger.info(f"GitHub Actions price update started - Target: {self.target_items} items")
        
        logger.info("二段階フィルタリング設定:")
        logger.info(f"  事前除外閾値: {self.minimum_price_threshold:,} NESO以下")
        logger.info(f"  IQR法倍率: {self.iqr_multiplier}")
        logger.info(f"  最小データ数: {self.minimum_data_points}件")
        
        try:
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

        # シングルスレッド処理（安定性重視）
        all_results = []
        for i, (equipment_id, equipment_info) in enumerate(items, 1):
            equipment_name = equipment_info.get("item_name", "")
            logger.info(f"[{i}/{total}] Processing: {equipment_name}")
            
            result = self.update_equipment_price_with_retry(
                equipment_id, equipment_name, equipment_info
            )
            all_results.append(result)
            
            time.sleep(5)

        # JSONデータに反映
        normal_updates = 0
        outlier_updates = 0
        failed_updates = 0
        
        for result in all_results:
            if result.get('success'):
                equipment_data[result['equipment_id']]["item_price"] = f"{result['price']:,}"
                
                price_status = result.get('price_status', '')
                if '外れ値' in price_status or '維持' in price_status or '中央値' in price_status:
                    equipment_data[result['equipment_id']]["status"] = f"価格更新済み（{price_status}）"
                    outlier_updates += 1
                else:
                    equipment_data[result['equipment_id']]["status"] = "価格更新済み"
                    normal_updates += 1
                    
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

        logger.info("=" * 50)
        logger.info("📊 価格更新統計:")
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
