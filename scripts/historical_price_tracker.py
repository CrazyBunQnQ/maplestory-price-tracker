#!/usr/bin/env python3
import json
import time
import os
import logging
from datetime import datetime, timedelta
from collections import deque

# ログ設定の強化（ファイル出力）
def setup_logging():
    log_dir = "logs"
    os.makedirs(log_dir, exist_ok=True)
    
    # ログファイル名に日付を含める
    log_filename = f"{log_dir}/maple_price_tracker_{datetime.now().strftime('%Y%m%d')}.log"
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_filename, encoding='utf-8'),
            logging.StreamHandler()  # コンソールにも出力（GitHub Actions用）
        ]
    )
    return logging.getLogger(__name__)

logger = setup_logging()

class HistoricalPriceTracker:
    def __init__(self, json_file_path="data/equipment_prices.json", 
                 history_dir="data/price_history"):
        self.json_file_path = json_file_path
        self.history_dir = history_dir
        
        # ディレクトリ作成
        os.makedirs(history_dir, exist_ok=True)
        
        logger.info(f"価格追跡システム初期化: {history_dir}")
        
        # 修正された時間間隔とデータ保持期間
        self.price_intervals = {
            '1hour': {
                'interval': timedelta(hours=1),
                'maxlen': 168,  # 1週間分（168時間）
                'description': '1週間分（1時間毎）'
            },
            '12hour': {
                'interval': timedelta(hours=12),
                'maxlen': 60,   # 1ヶ月分（60回 = 30日）
                'description': '1ヶ月分（12時間毎）'
            },
            '1day': {
                'interval': timedelta(days=1),
                'maxlen': 365,  # 1年分（365日）
                'description': '1年分（1日毎）'
            }
        }
        
        # 各アイテムの価格履歴を管理するディクショナリ
        self.price_history = {}
        self.load_existing_history()

    def load_existing_history(self):
        """既存の価格履歴を読み込み"""
        try:
            loaded_items = 0
            for interval_type in self.price_intervals:
                history_file = os.path.join(self.history_dir, f"history_{interval_type}.json")
                if os.path.exists(history_file):
                    with open(history_file, 'r', encoding='utf-8') as f:
                        data = json.load(f)
                        for item_id, history in data.items():
                            if item_id not in self.price_history:
                                self.price_history[item_id] = {}
                            
                            # dequeに変換して最大長を適用
                            self.price_history[item_id][interval_type] = deque(
                                history, 
                                maxlen=self.price_intervals[interval_type]['maxlen']
                            )
                            loaded_items += 1
                    
                    logger.info(f"{interval_type} 履歴ファイル読み込み: {len(data)}アイテム")
                else:
                    logger.warning(f"{interval_type} 履歴ファイルが存在しません: {history_file}")
            
            logger.info(f"価格履歴読み込み完了: {len(self.price_history)}アイテム、{loaded_items}レコード")
            
        except Exception as e:
            logger.error(f"価格履歴読み込みエラー: {e}", exc_info=True)

    def save_history_to_files(self):
        """価格履歴を間隔別ファイルに保存（現在のJSONとは別管理）"""
        try:
            saved_items = 0
            for interval_type in self.price_intervals:
                history_file = os.path.join(self.history_dir, f"history_{interval_type}.json")
                
                # dequeをリストに変換して保存
                interval_data = {}
                for item_id, intervals in self.price_history.items():
                    if interval_type in intervals:
                        interval_data[item_id] = list(intervals[interval_type])
                        saved_items += 1
                
                with open(history_file, 'w', encoding='utf-8') as f:
                    json.dump(interval_data, f, ensure_ascii=False, indent=2)
                
                logger.info(f"{interval_type} 履歴保存完了: {len(interval_data)}アイテム ({history_file})")
            
            logger.info(f"全履歴保存完了: {saved_items}レコード")
                
        except Exception as e:
            logger.error(f"価格履歴保存エラー: {e}", exc_info=True)

    def update_price_history(self, item_id, item_name, current_price):
        """価格履歴を更新（現在のJSONとは独立）"""
        try:
            timestamp = datetime.now().isoformat()
            price_point = {
                'timestamp': timestamp,
                'price': current_price,
                'item_name': item_name  # チャート表示用
            }
            
            # アイテム初期化
            if item_id not in self.price_history:
                self.price_history[item_id] = {}
            
            # 各間隔での更新判定と追加
            updated_intervals = []
            for interval_type, config in self.price_intervals.items():
                if self.should_update_interval(item_id, interval_type):
                    if interval_type not in self.price_history[item_id]:
                        self.price_history[item_id][interval_type] = deque(maxlen=config['maxlen'])
                    
                    self.price_history[item_id][interval_type].append(price_point)
                    updated_intervals.append(interval_type)
            
            if updated_intervals:
                logger.info(f"価格履歴更新 [{item_id}] {item_name}: {updated_intervals} - {current_price:,} NESO")
            
            return updated_intervals
            
        except Exception as e:
            logger.error(f"価格履歴更新エラー [{item_id}] {item_name}: {e}", exc_info=True)
            return []

    def get_statistics(self):
        """履歴統計情報を取得"""
        try:
            stats = {
                'total_items': len(self.price_history),
                'intervals': {},
                'timestamp': datetime.now().isoformat()
            }
            
            for interval_type, config in self.price_intervals.items():
                item_count = sum(1 for item in self.price_history.values() 
                               if interval_type in item and len(item[interval_type]) > 0)
                total_records = sum(len(item.get(interval_type, [])) 
                                  for item in self.price_history.values())
                
                stats['intervals'][interval_type] = {
                    'items_with_data': item_count,
                    'total_records': total_records,
                    'description': config['description'],
                    'max_points': config['maxlen']
                }
            
            logger.info(f"履歴統計: {stats['total_items']}アイテム、各間隔の詳細は統計データ参照")
            return stats
            
        except Exception as e:
            logger.error(f"統計取得エラー: {e}", exc_info=True)
            return {}

def main():
    """メイン実行：現在の価格から履歴を更新"""
    try:
        start_time = datetime.now()
        logger.info("="*50)
        logger.info("MapleStory価格履歴更新開始")
        logger.info("="*50)
        
        tracker = HistoricalPriceTracker()
        
        # 現在の価格データから履歴更新
        updated = tracker.update_from_current_prices()
        
        # 統計表示
        stats = tracker.get_statistics()
        logger.info(f"📊 価格履歴統計:")
        logger.info(f"  総アイテム数: {stats.get('total_items', 0)}")
        
        for interval, data in stats.get('intervals', {}).items():
            logger.info(f"  {interval}: {data['items_with_data']}件 "
                       f"({data['total_records']}レコード, {data['description']})")
        
        execution_time = (datetime.now() - start_time).total_seconds()
        logger.info(f"✅ 更新完了: {updated}アイテム (実行時間: {execution_time:.2f}秒)")
        logger.info("="*50)
        
    except Exception as e:
        logger.error(f"メイン実行エラー: {e}", exc_info=True)
        raise

if __name__ == "__main__":
    main()
