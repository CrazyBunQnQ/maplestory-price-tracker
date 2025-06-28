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
        
        # 🔥 修正1: 7データ対応の設定調整
        self.iqr_multiplier = 1.0              # 1.5 → 1.0 に厳格化
        self.minimum_data_points = 4           # 3 → 4 に調整（7データ対応）
        self.minimum_price_threshold = 10000   # 基本閾値
        
        # 🔥 追加: 上下限フィルタリング設定
        self.median_min_ratio = 10      # 中央値の1/10が下限
        self.median_max_ratio = 20      # 中央値の20倍が上限
        self.top3_min_ratio = 20        # 上位3つ平均の1/20が絶対下限
        self.bottom3_max_ratio = 50     # 下位3つ平均の50倍が絶対上限
        self.final_price_ratio = 30     # 最高価格/最低価格の上限
        
        # 並行処理復活（高速化優先）
        if self.target_items_input.upper() == 'ALL':
            self.target_items = None
            self.use_parallel = True
            self.max_workers = 6
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
        chrome_options.add_argument('--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36')
        
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
        """価格情報を抽出（7データ+上下限フィルタリング対応版）"""
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

            # 🔥 修正2: 事前フィルタリング強化
            pre_filtered = [price for price in all_prices if price > self.minimum_price_threshold]
            pre_filtered.sort()
            
            # 🔥 修正3: 7つのデータを取得（5 → 7）
            raw_prices = pre_filtered[:7]
            
            # 🔥 修正4: 上下限両対応の段階的フィルタリング
            if len(raw_prices) >= 4:
                cleaned_prices = self.advanced_outlier_removal(raw_prices)
                logger.info(f"7データ上下限フィルタリング: {len(raw_prices)}個 → {len(cleaned_prices)}個")
            else:
                cleaned_prices = raw_prices
            
            # 🔥 修正5: データ不足の基準を調整（2 → 3）
            if len(cleaned_prices) < 3:
                logger.warning(f"フィルタリング後データ不足（{len(cleaned_prices)}件）")
                
            return cleaned_prices

        except Exception as e:
            raise Exception(f"価格抽出エラー: {e}")

    def advanced_outlier_removal(self, prices):
        """上下限両対応の高度外れ値除去（7データ対応版）"""
        if len(prices) < 4:
            return prices
        
        original_prices = prices.copy()
        logger.info(f"7データ高度フィルタリング開始: {[f'{p:,}' for p in prices]}")
        
        # 🔥 段階1: 相対的下限チェック（重要！）
        prices = self.remove_relative_low_outliers(prices)
        
        # 🔥 段階2: 相対的上限チェック
        prices = self.remove_relative_high_outliers(prices)
        
        # 🔥 段階3: 厳格IQR法
        prices = self.strict_iqr_filter(prices)
        
        # 🔥 段階4: 最終的な相対チェック
        prices = self.final_relative_check(prices)
        
        removed_count = len(original_prices) - len(prices)
        if removed_count > 0:
            removed_prices = [p for p in original_prices if p not in prices]
            logger.info(f"7データから除外された価格: {[f'{p:,}' for p in removed_prices]}")
        
        logger.info(f"最終7データフィルタリング結果: {[f'{p:,}' for p in sorted(prices)]}")
        return prices

    def remove_relative_low_outliers(self, prices):
        """相対的下限外れ値を除去（7データ対応）"""
        if len(prices) < 4:
            return prices
        
        sorted_prices = sorted(prices)
        
        # 🔥 方法1: 中央値の1/10以下を除外
        median_price = sorted_prices[len(sorted_prices) // 2]
        min_threshold = median_price // self.median_min_ratio
        
        # 🔥 方法2: 上位3つの平均の1/20以下を除外
        top3_avg = sum(sorted_prices[-3:]) // 3
        ultra_min_threshold = top3_avg // self.top3_min_ratio
        
        # より厳格な閾値を選択
        final_min_threshold = max(min_threshold, ultra_min_threshold, self.minimum_price_threshold)
        
        filtered = [p for p in prices if p >= final_min_threshold]
        
        if len(filtered) < len(prices):
            removed = [p for p in prices if p < final_min_threshold]
            logger.info(f"7データ相対的下限除外（閾値: {final_min_threshold:,}）: {[f'{p:,}' for p in removed]}")
        
        return filtered if len(filtered) >= 3 else prices

    def remove_relative_high_outliers(self, prices):
        """相対的上限外れ値を除去（7データ対応）"""
        if len(prices) < 4:
            return prices
        
        sorted_prices = sorted(prices)
        
        # 🔥 方法1: 中央値の20倍以上を除外
        median_price = sorted_prices[len(sorted_prices) // 2]
        max_threshold = median_price * self.median_max_ratio
        
        # 🔥 方法2: 下位3つの平均の50倍以上を除外
        bottom3_avg = sum(sorted_prices[:3]) // 3
        ultra_max_threshold = bottom3_avg * self.bottom3_max_ratio
        
        # より厳格な閾値を選択
        final_max_threshold = min(max_threshold, ultra_max_threshold)
        
        filtered = [p for p in prices if p <= final_max_threshold]
        
        if len(filtered) < len(prices):
            removed = [p for p in prices if p > final_max_threshold]
            logger.info(f"7データ相対的上限除外（閾値: {final_max_threshold:,}）: {[f'{p:,}' for p in removed]}")
        
        return filtered if len(filtered) >= 3 else prices

    def strict_iqr_filter(self, prices):
        """厳格化されたIQR法（7データ対応）"""
        if len(prices) < 4:
            return prices
        
        prices_array = np.array(sorted(prices))
        Q1 = np.percentile(prices_array, 25)
        Q3 = np.percentile(prices_array, 75)
        IQR = Q3 - Q1
        
        if IQR == 0:
            return prices
        
        # 🔥 厳格化: 1.5 → 1.0
        strict_multiplier = self.iqr_multiplier
        lower_bound = Q1 - strict_multiplier * IQR
        upper_bound = Q3 + strict_multiplier * IQR
        
        # 🔥 重要: 下限が負になった場合の対策
        if lower_bound < 0:
            # 最小値の80%を下限とする
            lower_bound = min(prices) * 0.8
            logger.info(f"7データIQR下限調整: 負値 → {lower_bound:,.0f}")
        
        filtered = [p for p in prices if lower_bound <= p <= upper_bound]
        
        if len(filtered) < len(prices):
            removed = [p for p in prices if not (lower_bound <= p <= upper_bound)]
            logger.info(f"7データ厳格IQR除外（{lower_bound:,.0f} - {upper_bound:,.0f}）: {[f'{p:,}' for p in removed]}")
        
        return filtered if len(filtered) >= 3 else prices

    def final_relative_check(self, prices):
        """最終的な相対チェック（7データ対応）"""
        if len(prices) < 3:
            return prices
        
        sorted_prices = sorted(prices)
        
        # 🔥 最終チェック: 最高価格/最低価格が30倍以内
        max_price = max(prices)
        min_price = min(prices)
        ratio = max_price / min_price if min_price > 0 else float('inf')
        
        if ratio > self.final_price_ratio:
            # 最低価格の30倍を超える価格を除外
            ratio_limit = min_price * self.final_price_ratio
            filtered = [p for p in prices if p <= ratio_limit]
            
            if len(filtered) >= 3:
                removed = [p for p in prices if p > ratio_limit]
                logger.info(f"7データ最終比率チェック除外（{self.final_price_ratio}倍ルール）: {[f'{p:,}' for p in removed]}")
                return filtered
        
        return prices

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
        """IQR法による外れ値検出（7データ対応厳格版）"""
        if len(prices) < self.minimum_data_points:
            return [], prices
        
        prices_array = np.array(prices)
        Q1 = np.percentile(prices_array, 25)
        Q3 = np.percentile(prices_array, 75)
        IQR = Q3 - Q1
        
        if IQR == 0:
            return [], prices
        
        # 🔥 厳格化されたIQR法
        lower_bound = Q1 - self.iqr_multiplier * IQR
        upper_bound = Q3 + self.iqr_multiplier * IQR
        
        outliers = []
        normal_prices = []
        
        for price in prices:
            if price < lower_bound or price > upper_bound:
                outliers.append(price)
            else:
                normal_prices.append(price)
        
        # 🔥 詳細ログ（7データ用）
        logger.info(f"7データ厳格IQR法（{self.iqr_multiplier}倍）統計:")
        logger.info(f"  Q1={Q1:,.0f}, Q3={Q3:,.0f}, IQR={IQR:,.0f}")
        logger.info(f"  境界: {lower_bound:,.0f} - {upper_bound:,.0f}")
        logger.info(f"  7つ中 外れ値{len(outliers)}件, 正常値{len(normal_prices)}件")
        
        return outliers, normal_prices

    def select_optimal_price(self, prices, previous_price):
        """最適価格の選定（7データ対応版）"""
        if not prices:
            return None, "価格データなし"

        logger.info(f"取得した7つの価格データ: {[f'{p:,}' for p in prices]}")
        
        if previous_price:
            logger.info(f"前回価格: {previous_price:,}")
        else:
            logger.info("前回価格: 未取得")

        outliers, normal_prices = self.detect_outliers_iqr(prices)
        
        # 🔥 7データでの詳細分析
        logger.info("7データIQR法による外れ値検出結果:")
        for i, price in enumerate(prices, 1):
            status = "外れ値" if price in outliers else "正常値"
            logger.info(f"  {i}/7: {price:,} NESO - {status}")

        if not normal_prices:
            logger.warning("全ての価格が外れ値と判定されました（7データ）")
            
            if previous_price and previous_price > self.minimum_price_threshold:
                logger.info(f"前回価格を維持: {previous_price:,}")
                return previous_price, "前回価格維持（全価格外れ値・7データ）"
            else:
                median_price = int(np.median(prices))
                logger.warning(f"中央値を使用: {median_price:,}")
                return median_price, "中央値使用（全価格外れ値・7データ）"

        optimal_price = min(normal_prices)
        excluded_count = len(outliers)
        
        if excluded_count > 0:
            logger.info(f"7データから{excluded_count}件を外れ値として除外")
        
        logger.info(f"選定された最適価格（7データ上下限解析）: {optimal_price:,}")
        
        return optimal_price, "7データ上下限フィルタリング正常価格"

    @retry_on_error(max_retries=2, delay=1)
    def update_equipment_price_with_retry(self, equipment_id, equipment_name, current_equipment_data):
        """装備価格の更新（7データ対応版）"""
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
                    logger.info(f"Success: {equipment_name}: {optimal_price:,} NESO ({price_status})")
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
        """装備アイテムのバッチ処理（7データ対応版）"""
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
        """価格更新実行（7データ並列処理版）"""
        start_time = time.time()
        
        if self.target_items is None:
            logger.info("GitHub Actions price update started - Target: ALL items (7-data parallel processing)")
        else:
            logger.info(f"GitHub Actions price update started - Target: {self.target_items} items")
        
        logger.info("7データ上下限フィルタリング設定:")
        logger.info(f"  並行処理: {self.use_parallel} (ワーカー数: {self.max_workers})")
        logger.info(f"  データ取得数: 7個（従来5個から改良）")
        logger.info(f"  最小データ数: {self.minimum_data_points}件")
        logger.info(f"  IQR倍率: {self.iqr_multiplier}倍（厳格化）")
        logger.info(f"  下限除去: 中央値の1/{self.median_min_ratio}, 上位3つ平均の1/{self.top3_min_ratio}")
        logger.info(f"  上限除去: 中央値の{self.median_max_ratio}倍, 下位3つ平均の{self.bottom3_max_ratio}倍")
        logger.info(f"  最終比率: {self.final_price_ratio}倍以内")
        
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
        logger.info(f"Processing {total} items with 7-data filtering")

        # 並行処理復活
        if self.use_parallel and total > 10:
            # バッチサイズを動的に調整
            batch_size = max(10, total // self.max_workers)
            batches = [items[i:i + batch_size] for i in range(0, len(items), batch_size)]

            logger.info(f"7データ並行処理開始: {self.max_workers}ワーカー, {len(batches)}バッチ")

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
                        logger.info(f"7データバッチ{batch_no} 完了 ({len(results)}件)")
                    except Exception as e:
                        logger.error(f"7データバッチ{batch_no} エラー: {e}")

        else:
            # シングルスレッド処理
            all_results = []
            for i, (equipment_id, equipment_info) in enumerate(items, 1):
                equipment_name = equipment_info.get("item_name", "")
                logger.info(f"[{i}/{total}] 7データ処理: {equipment_name}")
                
                result = self.update_equipment_price_with_retry(
                    equipment_id, equipment_name, equipment_info
                )
                all_results.append(result)
                
                # 高速化：短い待機時間
                time.sleep(2)

        # JSONデータに反映
        normal_updates = 0
        filtered_updates = 0
        failed_updates = 0
        
        for result in all_results:
            if result.get('success'):
                equipment_data[result['equipment_id']]["item_price"] = f"{result['price']:,}"
                
                price_status = result.get('price_status', '')
                if '上下限' in price_status or '7データ' in price_status:
                    equipment_data[result['equipment_id']]["status"] = f"価格更新済み（{price_status}）"
                    filtered_updates += 1
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

        elapsed_time = time.time() - start_time
        logger.info("=" * 50)
        logger.info("📊 7データ上下限フィルタリング並列処理統計:")
        logger.info(f"  実行時間: {elapsed_time:.1f}秒")
        logger.info(f"  正常更新: {normal_updates}件")
        logger.info(f"  フィルタリング更新: {filtered_updates}件")
        logger.info(f"  更新失敗: {failed_updates}件")
        logger.info(f"  合計処理: {total}件")
        logger.info(f"  処理速度: {total/elapsed_time:.1f}件/秒")
        logger.info(f"  7データ精度向上率: {((normal_updates + filtered_updates) / total * 100):.1f}%")
        logger.info("=" * 50)

        logger.info(f"7-data update completed: {self.updated_count}/{total} items successful")

def main():
    updater = GitHubActionsUpdater()
    try:
        updater.run_update()
    except Exception as e:
        logger.error(f"System error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
