#!/usr/bin/env python3
import json
import time
import os
from datetime import datetime, timedelta
from collections import deque
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class TotalPriceAggregator:
    def __init__(self, source_json_path="data/equipment_prices.json", 
                 history_dir="data/price_history"):
        self.source_json_path = source_json_path
        self.history_dir = history_dir
        
        # ディレクトリ作成
        os.makedirs(history_dir, exist_ok=True)
        
        # 時間間隔設定
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
        
        # 総価格履歴を管理
        self.total_price_history = {}
        
        self.initialize_total_price_history()
        self.load_existing_total_price_history()

    def initialize_total_price_history(self):
        """総価格履歴を初期化"""
        for interval_type in self.price_intervals:
            self.total_price_history[interval_type] = deque(
                maxlen=self.price_intervals[interval_type]['maxlen']
            )
        logger.info("総価格履歴を初期化しました")

    def load_existing_total_price_history(self):
        """既存の総価格履歴を読み込み"""
        try:
            for interval_type in self.price_intervals:
                total_file = os.path.join(self.history_dir, f"total_price_{interval_type}.json")
                if os.path.exists(total_file):
                    with open(total_file, 'r', encoding='utf-8') as f:
                        try:
                            data = json.load(f)
                            
                            if isinstance(data, list) and len(data) > 0:
                                valid_data = []
                                for point in data:
                                    if isinstance(point, dict) and 'timestamp' in point and 'total_price' in point:
                                        valid_data.append(point)
                                
                                if valid_data:
                                    self.total_price_history[interval_type] = deque(
                                        valid_data, 
                                        maxlen=self.price_intervals[interval_type]['maxlen']
                                    )
                                    logger.info(f"総価格履歴読み込み {interval_type}: {len(valid_data)}レコード")
                            else:
                                logger.info(f"総価格履歴ファイルが空 {interval_type}")
                        except json.JSONDecodeError as e:
                            logger.error(f"総価格履歴JSON読み込みエラー {interval_type}: {e}")
                        except Exception as e:
                            logger.error(f"総価格履歴読み込みエラー {interval_type}: {e}")
                else:
                    logger.info(f"総価格履歴ファイルが存在しません {interval_type}")
        except Exception as e:
            logger.error(f"総価格履歴読み込みエラー: {e}")

    def should_update_total_price_interval(self, interval_type):
        """総価格の指定間隔での更新が必要かチェック"""
        if interval_type not in self.total_price_history:
            return True
        
        history = self.total_price_history[interval_type]
        if not history:
            return True
        
        try:
            last_entry = history[-1]
            if not isinstance(last_entry, dict) or 'timestamp' not in last_entry:
                logger.warn(f"総価格履歴データ構造不正 {interval_type}: 更新が必要")
                return True
                
            last_time = datetime.fromisoformat(last_entry['timestamp'].replace('Z', '+00:00'))
            now = datetime.now()
            
            required_interval = self.price_intervals[interval_type]['interval']
            time_diff = now - last_time
            
            logger.debug(f"総価格間隔チェック {interval_type}: 経過時間={time_diff}, 必要間隔={required_interval}")
            
            return time_diff >= required_interval
        except Exception as e:
            logger.error(f"総価格間隔更新チェックエラー ({interval_type}): {e}")
            return True

    def calculate_total_price_from_json(self):
        """JSONファイルから総価格を計算"""
        total_price = 0
        valid_items = 0
        invalid_items = 0
        
        try:
            if not os.path.exists(self.source_json_path):
                logger.error(f"ソースJSONファイルが見つかりません: {self.source_json_path}")
                return 0, 0
            
            with open(self.source_json_path, 'r', encoding='utf-8') as f:
                current_data = json.load(f)
            
            if not isinstance(current_data, dict):
                logger.error(f"JSONデータが辞書型ではありません: {type(current_data)}")
                return 0, 0
            
            for item_id, item_data in current_data.items():
                if not item_data or not isinstance(item_data, dict):
                    continue
                    
                if not item_data.get('item_name') or not item_data.get('item_price'):
                    continue
                
                try:
                    price_str = str(item_data['item_price']).replace(',', '').replace(' NESO', '').strip()
                    
                    # 無効な価格をスキップ
                    if price_str in ['未取得', 'undefined', '0', '', 'None']:
                        invalid_items += 1
                        continue
                        
                    current_price = int(price_str)
                    
                    # 現実的な価格範囲チェック（1万～1億NESO）
                    if 10000 <= current_price <= 100000000:
                        total_price += current_price
                        valid_items += 1
                    else:
                        logger.debug(f"価格範囲外 ({item_id}): {current_price:,} NESO")
                        invalid_items += 1
                        
                except (ValueError, TypeError) as e:
                    logger.debug(f"価格変換エラー ({item_id}): {price_str} -> {e}")
                    invalid_items += 1
                    continue
        except Exception as e:
            logger.error(f"総価格計算エラー: {e}")
        
        logger.info(f"総価格計算結果: {total_price:,} NESO ({valid_items}有効アイテム, {invalid_items}無効アイテム)")
        return total_price, valid_items

    def update_total_price_history(self, total_price, valid_items):
        """総価格履歴を更新"""
        if total_price == 0 or valid_items == 0:
            logger.warn("有効な価格データがないため、総価格履歴を更新しません")
            return []
        
        timestamp = datetime.now().isoformat()
        total_price_point = {
            'timestamp': timestamp,
            'total_price': total_price,
            'item_count': valid_items,
            'average_price': total_price // valid_items if valid_items > 0 else 0
        }
        
        # 各間隔での更新判定と追加
        updated_intervals = []
        for interval_type, config in self.price_intervals.items():
            if self.should_update_total_price_interval(interval_type):
                if interval_type not in self.total_price_history:
                    self.total_price_history[interval_type] = deque(maxlen=config['maxlen'])
                
                self.total_price_history[interval_type].append(total_price_point)
                updated_intervals.append(interval_type)
        
        if updated_intervals:
            logger.info(f"総価格履歴更新: {total_price:,} NESO ({valid_items}アイテム) - {updated_intervals}")
        
        return updated_intervals

    def save_total_price_history_to_files(self):
        """総価格履歴をファイルに保存"""
        try:
            for interval_type in self.price_intervals:
                total_file = os.path.join(self.history_dir, f"total_price_{interval_type}.json")
                if interval_type in self.total_price_history:
                    total_data = list(self.total_price_history[interval_type])
                    valid_total_data = []
                    for point in total_data:
                        if isinstance(point, dict) and 'timestamp' in point and 'total_price' in point:
                            valid_total_data.append(point)
                    
                    with open(total_file, 'w', encoding='utf-8') as f:
                        json.dump(valid_total_data, f, ensure_ascii=False, indent=2)
                    
                    logger.info(f"総価格履歴保存 {interval_type}: {len(valid_total_data)}ポイント")
        except Exception as e:
            logger.error(f"総価格履歴保存エラー: {e}")

    def load_existing_chart_data(self, interval):
        """既存のチャートデータを読み込み（修正版：データ蓄積対応）"""
        chart_file = os.path.join(self.history_dir, f"total_price_{interval}.json")
        
        if not os.path.exists(chart_file):
            logger.info(f"チャートファイルが存在しません: {chart_file}")
            return None
        
        try:
            with open(chart_file, 'r', encoding='utf-8') as f:
                existing_chart_data = json.load(f)
            
            # チャートデータの構造確認
            if (isinstance(existing_chart_data, dict) and 
                'labels' in existing_chart_data and 
                'datasets' in existing_chart_data):
                logger.info(f"既存チャートデータ読み込み成功 {interval}: {len(existing_chart_data['labels'])}ポイント")
                return existing_chart_data
            else:
                logger.warn(f"既存チャートデータの構造が不正: {interval}")
                return None
                
        except json.JSONDecodeError as e:
            logger.error(f"チャートファイル読み込みエラー {interval}: {e}")
            return None
        except Exception as e:
            logger.error(f"チャートデータ読み込みエラー {interval}: {e}")
            return None

    def append_new_data_to_chart(self, existing_chart_data, new_data_point, interval):
        """既存のチャートデータに新しいデータポイントを追加（修正版：データ蓄積対応）"""
        try:
            # 新しいデータポイントを解析
            timestamp = new_data_point['timestamp']
            total_price = new_data_point['total_price']
            average_price = new_data_point['average_price']
            
            # 時刻フォーマット
            def format_time(timestamp_str):
                try:
                    timestamp_obj = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
                    if interval == '1hour':
                        return timestamp_obj.strftime('%m/%d %H:%M')
                    elif interval == '12hour':
                        return timestamp_obj.strftime('%m/%d %H:%M')
                    else:  # 1day
                        return timestamp_obj.strftime('%m/%d')
                except Exception as e:
                    logger.error(f"時刻フォーマットエラー: {timestamp_str} -> {e}")
                    return timestamp_str
            
            formatted_time = format_time(timestamp)
            
            # 既存データに追加
            existing_chart_data['labels'].append(formatted_time)
            existing_chart_data['datasets'][0]['data'].append(total_price)  # 総価格
            existing_chart_data['datasets'][1]['data'].append(average_price)  # 平均価格
            
            # maxlenに基づいて古いデータを削除
            max_points = self.price_intervals[interval]['maxlen']
            
            if len(existing_chart_data['labels']) > max_points:
                # 古いデータから削除
                remove_count = len(existing_chart_data['labels']) - max_points
                existing_chart_data['labels'] = existing_chart_data['labels'][remove_count:]
                existing_chart_data['datasets'][0]['data'] = existing_chart_data['datasets'][0]['data'][remove_count:]
                existing_chart_data['datasets'][1]['data'] = existing_chart_data['datasets'][1]['data'][remove_count:]
            
            logger.info(f"チャートデータ追加完了 {interval}: {len(existing_chart_data['labels'])}ポイント")
            return existing_chart_data
            
        except Exception as e:
            logger.error(f"チャートデータ追加エラー {interval}: {e}")
            return existing_chart_data

    def generate_total_price_chart_data(self, interval='1hour'):
        """総価格チャート用のデータを生成"""
        if interval not in self.total_price_history:
            logger.warn(f"総価格履歴に{interval}が存在しません")
            return None
        
        history = list(self.total_price_history[interval])
        if not history:
            logger.warn(f"総価格履歴{interval}が空です")
            return None
        
        valid_points = []
        for point in history:
            if isinstance(point, dict) and 'timestamp' in point and 'total_price' in point and 'average_price' in point:
                total_price = point['total_price']
                avg_price = point['average_price']
                
                if total_price > 0 and avg_price > 0:
                    valid_points.append(point)
            else:
                logger.warn(f"総価格履歴データ構造不正: {point}")
        
        if len(valid_points) < 1:
            logger.error(f"有効な総価格履歴データがありません ({interval}): {len(valid_points)}ポイント")
            return None
        
        def format_time(timestamp_str):
            try:
                timestamp = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
                if interval == '1hour':
                    return timestamp.strftime('%m/%d %H:%M')
                elif interval == '12hour':
                    return timestamp.strftime('%m/%d %H:%M')
                else:  # 1day
                    return timestamp.strftime('%m/%d')
            except Exception as e:
                logger.error(f"時刻フォーマットエラー: {timestamp_str} -> {e}")
                return timestamp_str
        
        try:
            chart_data = {
                'labels': [format_time(point['timestamp']) for point in valid_points],
                'datasets': [
                    {
                        'label': f'総価格 ({self.price_intervals[interval]["description"]})',
                        'data': [point['total_price'] for point in valid_points],
                        'borderColor': '#e74c3c',
                        'backgroundColor': 'rgba(231, 76, 60, 0.1)',
                        'borderWidth': 3,
                        'fill': True,
                        'tension': 0.3,
                        'yAxisID': 'y'
                    },
                    {
                        'label': f'平均価格 ({self.price_intervals[interval]["description"]})',
                        'data': [point['average_price'] for point in valid_points],
                        'borderColor': '#3498db',
                        'backgroundColor': 'rgba(52, 152, 219, 0.1)',
                        'borderWidth': 2,
                        'fill': False,
                        'tension': 0.3,
                        'yAxisID': 'y1'
                    }
                ]
            }
            logger.info(f"総価格チャートデータ生成完了 {interval}: {len(valid_points)}ポイント")
            return chart_data
        except Exception as e:
            logger.error(f"総価格チャートデータ生成エラー: {e}")
            return None

    def export_total_price_chart_data_for_web(self, interval='1hour'):
        """総価格チャートデータをWeb用に出力（修正版：データ蓄積対応）"""
        try:
            # 新しいデータポイントを取得
            if interval not in self.total_price_history or not self.total_price_history[interval]:
                logger.warn(f"総価格履歴データがありません ({interval})")
                return False
            
            # 最新のデータポイント
            latest_data_point = self.total_price_history[interval][-1]
            
            # 既存のチャートデータを読み込み
            existing_chart_data = self.load_existing_chart_data(interval)
            
            if existing_chart_data:
                # 既存データに新しいポイントを追加
                updated_chart_data = self.append_new_data_to_chart(
                    existing_chart_data, 
                    latest_data_point, 
                    interval
                )
            else:
                # 既存データがない場合は新規作成
                logger.info(f"新規チャートデータ作成: {interval}")
                updated_chart_data = self.generate_total_price_chart_data(interval)
            
            if not updated_chart_data:
                logger.warn(f"総価格チャートデータが生成できませんでした ({interval})")
                return False
            
            # ファイルに保存
            chart_file = os.path.join(self.history_dir, f"total_price_{interval}.json")
            with open(chart_file, 'w', encoding='utf-8') as f:
                json.dump(updated_chart_data, f, ensure_ascii=False, indent=2)
            
            logger.info(f"総価格チャートデータ出力完了: {chart_file} ({len(updated_chart_data['labels'])}ポイント)")
            return True
            
        except Exception as e:
            logger.error(f"総価格チャートデータ出力エラー ({interval}): {e}")
            return False

    def process_total_price_aggregation(self):
        """総価格集計処理を実行"""
        try:
            # JSONファイルから総価格を計算
            total_price, valid_items = self.calculate_total_price_from_json()
            
            # 総価格履歴を更新
            total_intervals = self.update_total_price_history(total_price, valid_items)
            
            if total_intervals:
                # ファイルに保存
                self.save_total_price_history_to_files()
                
                # Web用チャートデータ出力（データ蓄積対応）
                for interval in ['1hour', '12hour', '1day']:
                    try:
                        success = self.export_total_price_chart_data_for_web(interval)
                        if success:
                            logger.info(f"総価格チャートデータ出力成功: {interval}")
                        else:
                            logger.warn(f"総価格チャートデータ出力失敗: {interval}")
                    except Exception as e:
                        logger.error(f"総価格チャートデータ出力エラー ({interval}): {e}")
                
                logger.info(f"総価格集計処理完了: {total_price:,} NESO ({valid_items}アイテム)")
            else:
                logger.info("総価格データの更新は不要でした")
            
            return len(total_intervals)
            
        except Exception as e:
            logger.error(f"総価格集計処理エラー: {e}")
            return 0

    def get_statistics(self):
        """総価格履歴統計情報を取得"""
        stats = {
            'total_price_intervals': {}
        }
        
        for interval_type, config in self.price_intervals.items():
            if interval_type in self.total_price_history:
                total_price_points = len(self.total_price_history[interval_type])
                stats['total_price_intervals'][interval_type] = {
                    'total_price_points': total_price_points,
                    'description': config['description']
                }
            else:
                stats['total_price_intervals'][interval_type] = {
                    'total_price_points': 0,
                    'description': config['description']
                }
        
        return stats

def main():
    """メイン実行：総価格集計処理（データ蓄積対応）"""
    logger.info("=" * 50)
    logger.info("MapleStory総価格集計処理開始（データ蓄積対応）")
    logger.info("=" * 50)
    
    try:
        # システム初期化
        logger.info("総価格集計システム初期化: data/price_history")
        aggregator = TotalPriceAggregator()
        
        # 総価格集計処理実行
        updated = aggregator.process_total_price_aggregation()
        
        # 統計表示
        stats = aggregator.get_statistics()
        logger.info(f"📈 総価格履歴統計:")
        for interval, data in stats['total_price_intervals'].items():
            logger.info(f"  {interval}: {data['total_price_points']}ポイント ({data['description']})")
        
        logger.info("=" * 50)
        logger.info(f"✅ 総価格集計完了: {updated}間隔更新（データ蓄積対応）")
        logger.info("=" * 50)
        
        return updated
        
    except Exception as e:
        logger.error(f"メイン実行エラー: {e}")
        raise

if __name__ == "__main__":
    main()
