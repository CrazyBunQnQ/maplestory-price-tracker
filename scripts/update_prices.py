#!/usr/bin/env python3
import json
import time
import os
import logging
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
import re
from datetime import datetime
import functools

# GitHub Actions用ログ設定
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
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

class GitHubActionsPriceUpdater:
    def __init__(self, json_file_path="data/equipment_prices.json"):
        self.json_file_path = json_file_path
        self.target_items = int(os.getenv('TARGET_ITEMS', '50'))
        self.updated_count = 0

    def setup_driver(self):
        """GitHub Actions用Seleniumドライバーの設定"""
        chrome_options = Options()
        
        # GitHub Actions用設定
        chrome_options.add_argument("--headless=new")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--window-size=1920,1080")
        chrome_options.add_argument("--disable-extensions")
        chrome_options.add_argument("--disable-plugins")
        chrome_options.add_argument("--disable-logging")
        chrome_options.add_argument("--log-level=3")
        chrome_options.add_argument("--silent")
        
        # ボット検出回避設定
        chrome_options.add_argument("--disable-blink-features=AutomationControlled")
        chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
        chrome_options.add_experimental_option('useAutomationExtension', False)
        chrome_options.add_argument('--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36')
        
        try:
            service = Service('/usr/local/bin/chromedriver')
            driver = webdriver.Chrome(service=service, options=chrome_options)
        except Exception:
            driver = webdriver.Chrome(options=chrome_options)

        driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
        return driver

    def load_equipment_data(self):
        """JSONファイルから装備データを読み込み"""
        try:
            with open(self.json_file_path, 'r', encoding='utf-8') as file:
                return json.load(file)
        except FileNotFoundError:
            logger.error(f"JSON file not found: {self.json_file_path}")
            return {}
        except json.JSONDecodeError:
            logger.error("Invalid JSON file format")
            return {}

    def save_equipment_data(self, data):
        """装備データをJSONファイルに保存"""
        try:
            current_time = datetime.now().isoformat()
            for item_id, item_data in data.items():
                if item_data.get('status') == 'Price updated':
                    item_data['last_updated'] = current_time
            
            with open(self.json_file_path, 'w', encoding='utf-8') as file:
                json.dump(data, file, ensure_ascii=False, indent=2)
            logger.info(f"JSON file saved successfully: {self.updated_count} items updated")
        except Exception as e:
            logger.error(f"Failed to save JSON file: {e}")

    @retry_on_error(max_retries=3, delay=2)
    def update_equipment_price_with_retry(self, equipment_id, equipment_name):
        """特定の装備の価格を更新（GitHub Actions対応版）"""
        driver = self.setup_driver()
        try:
            # MSU Navigatorで検索
            driver.get("https://msu.io/navigator")
            WebDriverWait(driver, 15).until(
                lambda d: d.execute_script("return document.readyState") == "complete"
            )
            time.sleep(2)

            # 検索実行
            search_success = driver.execute_script("""
                let searchField = document.querySelector('#form_search_input') || 
                                document.querySelector('input[type="text"]');
                if (!searchField) return false;
                
                searchField.value = '';
                searchField.focus();
                searchField.value = arguments[0];
                searchField.dispatchEvent(new Event('input', { bubbles: true }));
                
                const enterEvent = new KeyboardEvent('keydown', {
                    key: 'Enter', keyCode: 13, bubbles: true
                });
                searchField.dispatchEvent(enterEvent);
                return true;
            """, equipment_name)

            if not search_success:
                raise Exception("Search field not found")

            time.sleep(3)

            # 価格要素を取得
            price_elements = driver.find_elements(
                By.CSS_SELECTOR,
                "p._typography-point-body-m-medium_15szf_134._kartrider_3m7yu_9.NesoBox_text__lvOcl"
            )

            if not price_elements:
                raise Exception("No price information found")

            prices = []
            for element in price_elements[:5]:  # 最新5件
                try:
                    price_text = driver.execute_script(
                        "return arguments[0].textContent || '';", element
                    ).strip()
                    
                    price_match = re.search(r'[\d,]+', price_text)
                    if price_match:
                        price_str = price_match.group().replace(',', '')
                        if price_str.isdigit():
                            price = int(price_str)
                            if price > 100000:  # 100,000以下除外
                                prices.append(price)
                except Exception:
                    continue

            if not prices:
                raise Exception("No valid prices found")

            optimal_price = min(prices)  # 最安値選択
            logger.info(f"Success: {equipment_name}: {optimal_price:,} NESO")
            
            return {
                'equipment_id': equipment_id,
                'equipment_name': equipment_name,
                'price': optimal_price,
                'success': True
            }

        finally:
            driver.quit()

    def run_github_actions_update(self):
        """GitHub Actions用価格更新実行"""
        logger.info(f"GitHub Actions price update started - Target: {self.target_items} items")
        
        equipment_data = self.load_equipment_data()
        if not equipment_data:
            logger.error("Failed to load equipment data")
            return

        # 処理対象を制限
        items = [(k, v) for k, v in equipment_data.items() 
                if v.get("item_name") and k != ""][:self.target_items]
        
        for equipment_id, equipment_info in items:
            equipment_name = equipment_info.get("item_name", "")
            try:
                result = self.update_equipment_price_with_retry(equipment_id, equipment_name)
                if result.get('success'):
                    equipment_data[equipment_id]["item_price"] = f"{result['price']:,}"
                    equipment_data[equipment_id]["status"] = "Price updated"
                    self.updated_count += 1
                    
            except Exception as e:
                logger.warning(f"Update failed for {equipment_name}")
                equipment_data[equipment_id]["status"] = "Price fetch failed"
            
            # API制限対策
            time.sleep(3)

        self.save_equipment_data(equipment_data)
        logger.info(f"GitHub Actions price update completed: {self.updated_count}/{len(items)} items updated")

def main():
    """GitHub Actions用メイン実行関数"""
    updater = GitHubActionsPriceUpdater()
    try:
        updater.run_github_actions_update()
        print(f"::set-output name=updated_count::{updater.updated_count}")
    except Exception as e:
        logger.error(f"System error: {e}")
        exit(1)

if __name__ == "__main__":
    main()
