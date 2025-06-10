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
import re
from datetime import datetime

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class GitHubActionsUpdater:
    def __init__(self, json_file_path="data/equipment_prices.json"):
        self.json_file_path = json_file_path
        self.target_items = int(os.getenv('TARGET_ITEMS', '10'))
        self.updated_count = 0

    def setup_driver(self):
        """GitHub Actions用Chrome設定"""
        chrome_options = Options()
        
        # GitHub Actions必須設定
        chrome_options.add_argument("--headless=new")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--window-size=1920,1080")
        chrome_options.add_argument("--disable-extensions")
        chrome_options.add_argument("--disable-logging")
        chrome_options.add_argument("--log-level=3")
        
        # ボット検出回避
        chrome_options.add_argument("--disable-blink-features=AutomationControlled")
        chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
        chrome_options.add_experimental_option('useAutomationExtension', False)
        chrome_options.add_argument('--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36')
        
        # Chrome for Testing対応のService設定
        try:
            service = Service('/usr/local/bin/chromedriver')
            driver = webdriver.Chrome(service=service, options=chrome_options)
            logger.info("✅ ChromeDriver initialized successfully")
        except Exception as e:
            logger.error(f"❌ ChromeDriver initialization failed: {e}")
            raise

        driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
        return driver

    def safe_price_update(self, equipment_id, equipment_name):
        """安全な価格更新"""
        driver = None
        try:
            driver = self.setup_driver()
            
            # MSU Navigator接続
            driver.get("https://msu.io/navigator")
            time.sleep(4)
            
            # 検索実行
            search_success = driver.execute_script("""
                let searchField = document.querySelector('#form_search_input') || 
                                document.querySelector('input[type="text"]');
                if (!searchField) return false;
                
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

            time.sleep(5)

            # 価格要素取得
            price_elements = driver.find_elements(
                By.CSS_SELECTOR,
                "p._typography-point-body-m-medium_15szf_134._kartrider_3m7yu_9.NesoBox_text__lvOcl"
            )

            if not price_elements:
                raise Exception("No price elements found")

            prices = []
            for element in price_elements[:3]:
                try:
                    price_text = driver.execute_script(
                        "return arguments[0].textContent || '';", element
                    ).strip()
                    
                    price_match = re.search(r'[\d,]+', price_text)
                    if price_match:
                        price_str = price_match.group().replace(',', '')
                        if price_str.isdigit():
                            price = int(price_str)
                            if price > 100000:
                                prices.append(price)
                except:
                    continue

            if not prices:
                raise Exception("No valid prices found")

            optimal_price = min(prices)
            logger.info(f"✅ {equipment_name}: {optimal_price:,} NESO")
            
            return {
                'equipment_id': equipment_id,
                'equipment_name': equipment_name,
                'price': optimal_price,
                'success': True
            }

        except Exception as e:
            logger.error(f"❌ {equipment_name}: {str(e)}")
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

    def run_update(self):
        """価格更新実行"""
        logger.info(f"🔄 GitHub Actions価格更新開始 - 対象: {self.target_items}件")
        
        try:
            with open(self.json_file_path, 'r', encoding='utf-8') as f:
                equipment_data = json.load(f)
        except Exception as e:
            logger.error(f"❌ JSON読み込み失敗: {e}")
            sys.exit(1)

        items = [(k, v) for k, v in equipment_data.items() 
                if v.get("item_name") and k != ""][:self.target_items]
        
        for i, (equipment_id, equipment_info) in enumerate(items, 1):
            equipment_name = equipment_info.get("item_name", "")
            logger.info(f"[{i}/{len(items)}] 処理中: {equipment_name}")
            
            result = self.safe_price_update(equipment_id, equipment_name)
            
            if result.get('success'):
                equipment_data[equipment_id]["item_price"] = f"{result['price']:,}"
                equipment_data[equipment_id]["status"] = "価格更新済み"
                self.updated_count += 1
            else:
                equipment_data[equipment_id]["status"] = "価格取得失敗"
            
            time.sleep(6)  # GitHub Actions制限対応

        try:
            with open(self.json_file_path, 'w', encoding='utf-8') as f:
                json.dump(equipment_data, f, ensure_ascii=False, indent=2)
            logger.info(f"✅ JSON保存成功: {self.updated_count}件更新")
        except Exception as e:
            logger.error(f"❌ JSON保存失敗: {e}")
            sys.exit(1)

        logger.info(f"🎉 完了: {self.updated_count}/{len(items)}件成功")
        sys.exit(0)

def main():
    updater = GitHubActionsUpdater()
    try:
        updater.run_update()
    except Exception as e:
        logger.error(f"💥 システムエラー: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
