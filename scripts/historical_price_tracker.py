#!/usr/bin/env python3
import json
import time
import os
from datetime import datetime, timedelta
from collections import deque
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class HistoricalPriceTracker:
    def __init__(self, json_file_path="data/equipment_prices.json", 
                 history_dir="data/price_history"):
        self.json_file_path = json_file_path
        self.history_dir = history_dir
        
        # ディレクトリ作成
        os.makedirs(history_dir, exist_ok=True)
        
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
            logger.info(f"価格履歴読み込み完了: {len(self.price_history)}アイテム")
        except Exception as e:
            logger.error(f"価格履歴読み込みエラー: {e}")

    def save_history_to_files(self):
        """価格履歴を間隔別ファイルに保存（現在のJSONとは別管理）"""
        try:
            for interval_type in self.price_intervals:
                history_file = os.path.join(self.history_dir, f"history_{interval_type}.json")
                
                # dequeをリストに変換して保存
                interval_data = {}
                for item_id, intervals in self.price_history.items():
                    if interval_type in intervals:
                        interval_data[item_id] = list(intervals[interval_type])
                
                with open(history_file, 'w', encoding='utf-8') as f:
                    json.dump(interval_data, f, ensure_ascii=False, indent=2)
                
                logger.info(f"{interval_type} 履歴保存完了: {len(interval_data)}アイテム")
        except Exception as e:
            logger.error(f"価格履歴保存エラー: {e}")

    def should_update_interval(self, item_id, interval_type):
        """指定した間隔での更新が必要かチェック"""
        if item_id not in self.price_history:
            return True
        
        if interval_type not in self.price_history[item_id]:
            return True
        
        history = self.price_history[item_id][interval_type]
        if not history:
            return True
        
        last_entry = history[-1]
        last_time = datetime.fromisoformat(last_entry['timestamp'])
        now = datetime.now()
        
        required_interval = self.price_intervals[interval_type]['interval']
        return now - last_time >= required_interval

    def update_price_history(self, item_id, item_name, current_price):
        """価格履歴を更新（現在のJSONとは独立）"""
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
            logger.info(f"{item_name} 価格履歴更新: {updated_intervals}")
        
        return updated_intervals

    def generate_chart_data(self, item_id, interval='1hour'):
        """Chart.js用のデータを生成（デフォルトは1hour/1週間分）"""
        if item_id not in self.price_history:
            return None
        
        if interval not in self.price_history[item_id]:
            return None
        
        history = list(self.price_history[item_id][interval])
        if not history:
            return None
        
        # 時刻フォーマットを間隔に応じて調整
        if interval == '1hour':
            time_format = lambda t: datetime.fromisoformat(t).strftime('%m/%d %H:%M')
        elif interval == '12hour':
            time_format = lambda t: datetime.fromisoformat(t).strftime('%m/%d %H:%M')
        else:  # 1day
            time_format = lambda t: datetime.fromisoformat(t).strftime('%m/%d')
        
        return {
            'labels': [time_format(point['timestamp']) for point in history],
            'datasets': [{
                'label': f'価格 ({self.price_intervals[interval]["description"]})',
                'data': [point['price'] for point in history],
                'borderColor': '#2c3e50',
                'backgroundColor': 'rgba(44, 62, 80, 0.1)',
                'borderWidth': 2,
                'fill': True,
                'tension': 0.3
            }]
        }

    def generate_comparison_chart_data(self, item_id_a, item_id_b, interval='1hour'):
        """A/B比較用のチャートデータを生成"""
        data_a = self.generate_chart_data(item_id_a, interval)
        data_b = self.generate_chart_data(item_id_b, interval)
        
        if not data_a or not data_b:
            return None
        
        # 時刻軸を統一（より多くのデータポイントを持つ方に合わせる）
        labels = data_a['labels'] if len(data_a['labels']) >= len(data_b['labels']) else data_b['labels']
        
        return {
            'labels': labels,
            'datasets': [
                {
                    **data_a['datasets'][0],
                    'label': f'{self.get_item_name(item_id_a)} (セットA)',
                    'borderColor': '#28a745',
                    'backgroundColor': 'rgba(40, 167, 69, 0.1)'
                },
                {
                    **data_b['datasets'][0],
                    'label': f'{self.get_item_name(item_id_b)} (セットB)',
                    'borderColor': '#ffc107',
                    'backgroundColor': 'rgba(255, 193, 7, 0.1)'
                }
            ]
        }

    def get_item_name(self, item_id):
        """現在のJSONからアイテム名を取得"""
        try:
            with open(self.json_file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
                return data.get(item_id, {}).get('item_name', f'Item {item_id}')
        except:
            return f'Item {item_id}'

    def update_from_current_prices(self):
        """現在の価格JSONから履歴を更新"""
        try:
            with open(self.json_file_path, 'r', encoding='utf-8') as f:
                current_data = json.load(f)
            
            updated_count = 0
            for item_id, item_data in current_data.items():
                if not item_data.get('item_name') or not item_data.get('item_price'):
                    continue
                
                # 価格文字列を数値に変換
                price_str = item_data['item_price'].replace(',', '').replace(' NESO', '')
                try:
                    current_price = int(price_str)
                    if current_price > 0:
                        intervals = self.update_price_history(
                            item_id, 
                            item_data['item_name'], 
                            current_price
                        )
                        if intervals:
                            updated_count += 1
                except ValueError:
                    continue
            
            if updated_count > 0:
                self.save_history_to_files()
                logger.info(f"価格履歴更新完了: {updated_count}アイテム")
            
            return updated_count
        except Exception as e:
            logger.error(f"価格履歴更新エラー: {e}")
            return 0

    def export_chart_data_for_web(self, item_id, interval='1hour'):
        """Web用にチャートデータをファイル出力"""
        chart_data = self.generate_chart_data(item_id, interval)
        if not chart_data:
            return False
        
        try:
            chart_file = os.path.join(self.history_dir, f"{item_id}_{interval}.json")
            with open(chart_file, 'w', encoding='utf-8') as f:
                json.dump(chart_data, f, ensure_ascii=False, indent=2)
            return True
        except Exception as e:
            logger.error(f"チャートデータ出力エラー: {e}")
            return False

    def get_statistics(self):
        """履歴統計情報を取得"""
        stats = {
            'total_items': len(self.price_history),
            'intervals': {}
        }
        
        for interval_type, config in self.price_intervals.items():
            item_count = sum(1 for item in self.price_history.values() 
                           if interval_type in item and len(item[interval_type]) > 0)
            stats['intervals'][interval_type] = {
                'items_with_data': item_count,
                'description': config['description'],
                'max_points': config['maxlen']
            }
        
        return stats

def main():
    """メイン実行：現在の価格から履歴を更新"""
    tracker = HistoricalPriceTracker()
    
    # 現在の価格データから履歴更新
    updated = tracker.update_from_current_prices()
    
    # 統計表示
    stats = tracker.get_statistics()
    print(f"📊 価格履歴統計:")
    print(f"  総アイテム数: {stats['total_items']}")
    for interval, data in stats['intervals'].items():
        print(f"  {interval}: {data['items_with_data']}件 ({data['description']})")
    
    print(f"✅ 更新完了: {updated}アイテム")

if __name__ == "__main__":
    main()
