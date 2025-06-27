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

# webdriver-managerの安全なインポート
try:
    from webdriver_manager.chrome import ChromeDriverManager
    WEBDRIVER_MANAGER_AVAILABLE = True
except ImportError:
    WEBDRIVER_MANAGER_AVAILABLE = False

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def retry_on_error(max_retries=3, delay=2):
    """エラー時にリトライするデコレータ"""
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
                        time.sleep(delay * attempt)  # 指数バックオフ
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
        
        # 設定を簡素化
        self.iqr_multiplier = 1.5
        self.minimum_data_points = 4
        self.minimum_price_threshold = 10000
        
        # 並行処理を無効化（安定性優先）
        if self.target_items_input.upper() == 'ALL':
            self.target_items = None
            self.use_parallel = False  # 並行処理を無効化
        else:
            try:
                self.target_items = int(self.target_items_input)
                self.use_parallel = False
            except ValueError:
                self.target_items = 10
                self.use_parallel = False

    def setup_driver(self):
        """Seleniumドライバーの設定（安定版）"""
        chrome_options = Options()
        
        # GitHub Actions用基本設定
        chrome_options.add_argument("--headless=new")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--window-size=1920,1080")
        
        # 安定性重視の設定
        chrome_options.add_argument("--disable-web-security")
        chrome_options.add_argument("--disable-features=VizDisplayCompositor")
        chrome_options.add_argument("--disable-extensions")
        chrome_options.add_argument("--disable-plugins")
        chrome_options.add_argument("--disable-logging")
        chrome_options.add_argument("--log-level=3")
        chrome_options.add_argument("--silent")
        chrome_options.add_argument("--disable-background-timer-throttling")
        chrome_options.add_argument("--disable-backgrounding-occluded-windows")
        chrome_options.add_argument("--disable-renderer-backgrounding")
        
        # ボット検出回避
        chrome_options.add_argument("--disable-blink-features=AutomationControlled")
        chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
        chrome_options.add_experimental_option('useAutomationExtension', False)
        chrome_options.add_argument('--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36')
        
        # メモリ制限
        chrome_options.add_argument("--memory-pressure-off")
        chrome_options.add_argument("--max_old_space_size=4096")
        
        try:
            # システムのChromeDriverを使用
            service = Service('/usr/local/bin/chromedriver')
            service.log_path = os.devnull
            
            driver = webdriver.Chrome(service=service, options=chrome_options)
            
            # タイムアウト設定
            driver.set_page_load_timeout(30)
            driver.implicitly_wait(10)
            
            # ボット検出対策
            driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
            driver.execute_script("delete window.cdc_adoQpoasnfa76pfcZLmcfl_Array;")
            driver.execute_script("delete window.cdc_adoQpoasnfa76pfcZLmcfl_Promise;")
            driver.execute_script("delete window.cdc_adoQpoasnfa76pfcZLmcfl_Symbol;")
            
            logger.info("ChromeDriver initialized successfully")
            return driver
            
        except Exception as e:
            logger.error(f"ChromeDriver initialization failed: {e}")
            # デバッグ情報を出力
            self.debug_environment()
            raise

    def debug_environment(self):
        """環境のデバッグ情報を出力"""
        try:
            logger.info("=== Environment Debug Information ===")
            logger.info(f"Python version: {sys.version}")
            
            # ChromeDriverの確認
            if os.path.exists('/usr/local/bin/chromedriver'):
                logger.info("ChromeDriver found at /usr/local/bin/chromedriver")
                try:
                    import subprocess
                    result = subprocess.run(['/usr/local/bin/chromedriver', '--version'], 
                                          capture_output=True, text=True, timeout=5)
                    logger.info(f"ChromeDriver version: {result.stdout.strip()}")
                except Exception as e:
                    logger.warning(f"Could not get ChromeDriver version: {e}")
            else:
                logger.error("ChromeDriver not found at /usr/local/bin/chromedriver")
            
            # Chromeの確認
            try:
                import subprocess
                result = subprocess.run(['google-chrome', '--version'], 
                                      capture_output=True, text=True, timeout=5)
                logger.info(f"Chrome version: {result.stdout.strip()}")
            except Exception as e:
                logger.warning(f"Could not get Chrome version: {e}")
                
        except Exception as e:
            logger.error(f"Debug environment error: {e}")

    def search_equipment_js(self, driver, equipment_name):
        """JavaScriptを使用した検索実行（改良版）"""
        try:
            logger.info(f"Searching for: {equipment_name}")
            
            # ページロード
            driver.get("https://msu.io/navigator")
            
            # ページの完全読み込みを待機
            WebDriverWait(driver, 20).until(
                lambda d: d.execute_script("return document.readyState") == "complete"
            )
            
            # 追加の安定化待機
            time.sleep(5)
            
            # 検索フィールドの検出と入力
            search_success = driver.execute_script("""
                const searchSelectors = [
                    '#form_search_input',
                    'input[id="form_search_input"]',
                    'input[type="text"]',
                    'input[placeholder*="search"]',
                    'input[placeholder*="Search"]',
                    '.search-input',
                    '[data-testid="search-input"]'
                ];
                
                let searchField = null;
                for (const selector of searchSelectors) {
                    searchField = document.querySelector(selector);
                    if (searchField && searchField.offsetParent !== null) {
                        break;
                    }
                }
                
                if (!searchField) {
                    console.log('Search field not found with any selector');
                    return false;
                }
                
                console.log('Search field found:', searchField);
                
                // 検索フィールドをクリア
                searchField.value = '';
                searchField.focus();
                
                // 文字を一文字ずつ入力
                const text = arguments[0];
                for (let i = 0; i < text.length; i++) {
                    searchField.value += text[i];
                    searchField.dispatchEvent(new Event('input', { bubbles: true }));
                }
                
                // Enterキーを送信
                const enterEvent = new KeyboardEvent('keydown', {
                    key: 'Enter',
                    keyCode: 13,
                    which: 13,
                    bubbles: true
                });
                searchField.dispatchEvent(enterEvent);
                
                return true;
            """, equipment_name)

            if not search_success:
                raise Exception("Search field not found or not accessible")

            # 検索結果の読み込みを待機
            time.sleep(8)
            
            # 結果が表示されるまで待機
            try:
                WebDriverWait(driver, 15).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "p._typography-point-body-m-medium_15szf_134"))
                )
            except:
                logger.warning("Price elements not found with primary selector")
            
            return True

        except Exception as e:
            logger.error(f"Search error for {equipment_name}: {e}")
            raise Exception(f"検索エラー: {equipment_name}, {e}")

    def extract_prices(self, driver):
        """価格情報を抽出（改良版）"""
        try:
            # 複数のセレクターを試行
            price_selectors = [
                "p._typography-point-body-m-medium_15szf_134._kartrider_3m7yu_9.NesoBox_text__lvOcl",
                "p._typography-point-body-m-medium_15szf_134",
                ".NesoBox_text__lvOcl",
                "[data-testid='price']",
                ".price",
                "*[class*='price']"
            ]
            
            all_prices = []
            
            for selector in price_selectors:
                try:
                    price_elements = driver.find_elements(By.CSS_SELECTOR, selector)
                    
                    if price_elements:
                        logger.info(f"Found {len(price_elements)} price elements with selector: {selector}")
                        
                        for element in price_elements:
                            try:
                                price_text = driver.execute_script(
                                    "return arguments[0].textContent || arguments[0].innerText || '';",
                                    element
                                ).strip()

                                if price_text:
                                    # 価格パターンを抽出
                                    price_match = re.search(r'[\d,]+', price_text)
                                    if price_match:
                                        price_str = price_match.group().replace(',', '')
                                        if price_str.isdigit():
                                            price = int(price_str)
                                            if price > 1000:  # 最小価格フィルター
                                                all_prices.append(price)
                            except Exception:
                                continue
                        
                        if all_prices:
                            break  # 価格が見つかったらループを抜ける
                            
                except Exception as e:
                    logger.warning(f"Error with selector {selector}: {e}")
                    continue

            if not all_prices:
                # JavaScriptで直接価格を探す
                js_prices = driver.execute_script("""
                    const allElements = document.querySelectorAll('*');
                    const prices = [];
                    
                    for (const el of allElements) {
                        const text = el.textContent || el.innerText || '';
                        const matches = text.match(/\\d{1,3}(,\\d{3})+(?:\\s*NESO)?/g);
                        if (matches) {
                            for (const match of matches) {
                                const price = parseInt(match.replace(/[,\\s]/g, ''));
                                if (price > 1000 && price < 1000000000) {
                                    prices.push(price);
                                }
                            }
                        }
                    }
                    
                    return [...new Set(prices)].sort((a, b) => a - b);
                """)
                
                if js_prices:
                    all_prices = js_prices
                    logger.info(f"Found {len(all_prices)} prices using JavaScript extraction")

            # フィルタリング処理
            filtered_prices = [price for price in all_prices if price > self.minimum_price_threshold]
            
            excluded_count = len(all_prices) - len(filtered_prices)
            if excluded_count > 0:
                logger.info(f"{self.minimum_price_threshold:,}以下の価格を{excluded_count}件除外")
            
            # 最安値から5件を取得
            filtered_prices.sort()
            final_prices = filtered_prices[:5]
            
            logger.info(f"抽出された価格（最大5件）: {[f'{p:,}' for p in final_prices]}")
            
            if len(final_prices) < 3:
                logger.warning(f"価格データが不足（{len(final_prices)}件）")
                
            return final_prices

        except Exception as e:
            logger.error(f"価格抽出エラー: {e}")
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

        logger.info(f"取得価格: {[f'{p:,}' for p in prices]}")
        
        if previous_price:
            logger.info(f"前回価格: {previous_price:,}")
        else:
            logger.info("前回価格: 未取得")

        outliers, normal_prices = self.detect_outliers_iqr(prices)
        
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

    @retry_on_error(max_retries=3, delay=3)
    def update_equipment_price_with_retry(self, equipment_id, equipment_name, current_equipment_data):
        """装備価格の更新（改良版）"""
        driver = None
        try:
            previous_price = self.parse_previous_price(
                current_equipment_data.get('item_price', '')
            )
            
            logger.info(f"Processing: {equipment_name}")
            
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
                    time.sleep(2)  # クリーンアップ待機
                except Exception as cleanup_error:
                    logger.warning(f"Driver cleanup error: {cleanup_error}")

    def run_update(self):
        """価格更新実行（安定版）"""
        if self.target_items is None:
            logger.info("GitHub Actions price update started - Target: ALL items (sequential processing)")
        else:
            logger.info(f"GitHub Actions price update started - Target: {self.target_items} items")
        
        logger.info("設定情報:")
        logger.info(f"  事前除外閾値: {self.minimum_price_threshold:,} NESO以下")
        logger.info(f"  IQR法倍率: {self.iqr_multiplier}")
        logger.info(f"  最小データ数: {self.minimum_data_points}件")
        logger.info(f"  並行処理: 無効（安定性優先）")
        
        try:
            # データディレクトリの確認
            if not os.path.exists("data"):
                os.makedirs("data", exist_ok=True)
                logger.info("data directory created")
            
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
        logger.info(f"Processing {total} items sequentially")

        # シーケンシャル処理（安定性優先）
        all_results = []
        for i, (equipment_id, equipment_info) in enumerate(items, 1):
            equipment_name = equipment_info.get("item_name", "")
            logger.info(f"[{i}/{total}] Processing: {equipment_name}")
            
            try:
                result = self.update_equipment_price_with_retry(
                    equipment_id, equipment_name, equipment_info
                )
                all_results.append(result)
                
                if result.get('success'):
                    price_status = result.get('price_status', '')
                    logger.info(f"✅ {equipment_name}: {result.get('price', 'ERROR'):,} ({price_status})")
                else:
                    logger.error(f"❌ {equipment_name}: {result.get('error', 'Unknown error')}")
                    
            except Exception as e:
                logger.error(f"❌ {equipment_name}: Critical error: {e}")
                all_results.append({
                    'equipment_id': equipment_id,
                    'equipment_name': equipment_name,
                    'success': False,
                    'error': str(e)
                })
            
            # 処理間隔を長めに設定（安定性優先）
            if i < total:
                time.sleep(8)

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
                equipment_data[result['equipment_id']]["status"] = f"価格取得失敗: {result.get('error', 'Unknown')}"
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

def main():
    updater = GitHubActionsUpdater()
    try:
        updater.run_update()
    except Exception as e:
        logger.error(f"System error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
