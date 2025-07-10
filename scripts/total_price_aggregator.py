#!/usr/bin/env python3
import json
import time
import os
from datetime import datetime, timedelta
from collections import deque
import logging
import statistics

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class TotalPriceAggregator:
    def __init__(self, json_file_path="data/equipment_prices.json", 
                 history_dir="data/price_history"):
        self.json_file_path = json_file_path
        self.history_dir = history_dir
        
        # 強制データリフレッシュ設定
        self.force_data_refresh = os.getenv('FORCE_DATA_REFRESH', 'true').lower() == 'true'
        self.force_rebuild_aggregation = os.getenv('FORCE_REBUILD_AGGREGATION', 'false').lower() == 'true'
        
        # ディレクトリ作成
        os.makedirs(history_dir, exist_ok=True)
        
        # チャート用時間間隔設定（30分毎データから集約）
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
        
        # 30分毎の総価格生データ
        self.total_price_raw_data = deque(maxlen=2880)  # 30日分の30分毎データ
        
        # 集約済み総価格履歴
        self.total_price_history = {}
        
        logger.info("🔧 総価格集約システム初期化（30分毎データ対応）")
        logger.info(f"🔄 強制データリフレッシュ: {'有効' if self.force_data_refresh else '無効'}")
        logger.info(f"🏗️ 強制リビルド: {'有効' if self.force_rebuild_aggregation else '無効'}")
        
        self.load_existing_data()

    def load_existing_data(self):
        """既存の総価格データを読み込み"""
        try:
            # 30分毎の総価格生データを読み込み
            total_raw_file = os.path.join(self.history_dir, "total_price_raw_data.json")
            if os.path.exists(total_raw_file) and not self.force_rebuild_aggregation:
                with open(total_raw_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    self.total_price_raw_data = deque(data, maxlen=2880)
                    logger.info(f"総価格30分毎データ読み込み: {len(self.total_price_raw_data)}レコード")
            else:
                logger.info("総価格30分毎データ: 新規作成または再構築")
            
            # 集約済み総価格データを読み込み
            for interval_type in self.price_intervals:
                total_file = os.path.join(self.history_dir, f"total_price_{interval_type}.json")
                if os.path.exists(total_file) and not self.force_rebuild_aggregation:
                    with open(total_file, 'r', encoding='utf-8') as f:
                        data = json.load(f)
                        self.total_price_history[interval_type] = data
                        logger.info(f"総価格{interval_type}データ読み込み完了")
                else:
                    logger.info(f"総価格{interval_type}データ: 新規作成または再構築")
                        
        except Exception as e:
            logger.warning(f"総価格データ読み込みエラー: {e}")

    def collect_current_total_price(self):
        """現在の総価格を収集して30分毎データに追加"""
        try:
            if not os.path.exists(self.json_file_path):
                logger.error(f"価格ファイルが見つかりません: {self.json_file_path}")
                return False
            
            with open(self.json_file_path, 'r', encoding='utf-8') as f:
                current_data = json.load(f)
            
            # 有効な価格を収集
            valid_prices = []
            for item_id, item_data in current_data.items():
                if not item_data or not isinstance(item_data, dict):
                    continue
                    
                if not item_data.get('item_price'):
                    continue
                
                price_str = str(item_data['item_price']).replace(',', '').replace(' NESO', '').strip()
                try:
                    current_price = int(price_str)
                    if current_price > 0:
                        valid_prices.append(current_price)
                except (ValueError, TypeError):
                    continue
            
            if not valid_prices:
                logger.warning("有効な価格データがありません")
                return False
            
            # 総価格情報を計算
            total_price = sum(valid_prices)
            average_price = int(statistics.mean(valid_prices))
            median_price = int(statistics.median(valid_prices))
            min_price = min(valid_prices)
            max_price = max(valid_prices)
            
            timestamp = datetime.now().isoformat()
            
            # 30分毎の総価格データポイントを作成
            total_point = {
                'timestamp': timestamp,
                'total_price': total_price,
                'average_price': average_price,
                'median_price': median_price,
                'min_price': min_price,
                'max_price': max_price,
                'item_count': len(valid_prices)
            }
            
            # 重複チェック（同じ分の重複を避ける）
            current_minute = datetime.now().replace(second=0, microsecond=0)
            
            # 最新データが同じ分の場合は更新、そうでなければ追加
            if (self.total_price_raw_data and 
                len(self.total_price_raw_data) > 0):
                
                last_point = self.total_price_raw_data[-1]
                try:
                    last_time = datetime.fromisoformat(last_point['timestamp'].replace('Z', '+00:00'))
                    last_minute = last_time.replace(second=0, microsecond=0)
                    
                    if current_minute == last_minute:
                        # 同じ分のデータを更新
                        self.total_price_raw_data[-1] = total_point
                        logger.info(f"総価格データ更新（同分内）: 合計{total_price:,} NESO")
                    else:
                        # 新しい分のデータを追加
                        self.total_price_raw_data.append(total_point)
                        logger.info(f"総価格データ追加: 合計{total_price:,} NESO, 平均{average_price:,} NESO ({len(valid_prices)}アイテム)")
                except Exception:
                    # タイムスタンプ解析エラーの場合は追加
                    self.total_price_raw_data.append(total_point)
            else:
                # 初回データまたは空の場合
                self.total_price_raw_data.append(total_point)
                logger.info(f"総価格データ初回追加: 合計{total_price:,} NESO, 平均{average_price:,} NESO ({len(valid_prices)}アイテム)")
            
            return True
            
        except Exception as e:
            logger.error(f"総価格データ収集エラー: {e}")
            return False

    def aggregate_total_price_for_interval(self, interval_type):
        """総価格データを指定間隔で集約"""
        if not self.total_price_raw_data:
            logger.warning(f"30分毎総価格データが不足: {interval_type}")
            return None
        
        config = self.price_intervals[interval_type]
        interval_duration = config['interval']
        
        raw_data = list(self.total_price_raw_data)
        aggregated_data = []
        current_group = []
        group_start_time = None
        
        for data_point in raw_data:
            try:
                point_time = datetime.fromisoformat(data_point['timestamp'].replace('Z', '+00:00'))
                
                if group_start_time is None:
                    group_start_time = point_time
                    current_group = [data_point]
                elif point_time - group_start_time < interval_duration:
                    current_group.append(data_point)
                else:
                    # 現在のグループを集約
                    if current_group:
                        aggregated_point = self.create_aggregated_point(current_group)
                        aggregated_data.append(aggregated_point)
                    
                    # 新しいグループを開始
                    group_start_time = point_time
                    current_group = [data_point]
                    
            except Exception as e:
                logger.debug(f"総価格データポイント処理エラー: {e}")
                continue
        
        # 最後のグループを処理
        if current_group:
            aggregated_point = self.create_aggregated_point(current_group)
            aggregated_data.append(aggregated_point)
        
        # Chart.js用のデータ形式で返す
        return self.format_total_price_chart_data(aggregated_data, interval_type)

    def create_aggregated_point(self, group):
        """データグループから集約ポイントを作成"""
        if not group:
            return None
        
        # 各指標の平均を計算
        avg_total = int(statistics.mean([p['total_price'] for p in group]))
        avg_average = int(statistics.mean([p['average_price'] for p in group]))
        avg_median = int(statistics.mean([p['median_price'] for p in group]))
        min_of_mins = min([p['min_price'] for p in group])
        max_of_maxs = max([p['max_price'] for p in group])
        avg_count = int(statistics.mean([p['item_count'] for p in group]))
        
        return {
            'timestamp': group[-1]['timestamp'],  # 最新のタイムスタンプを使用
            'total_price': avg_total,
            'average_price': avg_average,
            'median_price': avg_median,
            'min_price': min_of_mins,
            'max_price': max_of_maxs,
            'item_count': avg_count,
            'data_points': len(group)
        }

    def format_total_price_chart_data(self, aggregated_data, interval_type):
        """総価格データをChart.js形式にフォーマット"""
        if not aggregated_data:
            return {"labels": [], "datasets": []}
        
        # 時刻フォーマットを間隔に応じて調整
        def format_time(timestamp_str):
            try:
                timestamp = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
                if interval_type == '1hour':
                    return timestamp.strftime('%m/%d %H:%M')
                elif interval_type == '12hour':
                    return timestamp.strftime('%m/%d %H:%M')
                else:  # 1day
                    return timestamp.strftime('%m/%d')
            except:
                return timestamp_str
        
        labels = [format_time(point['timestamp']) for point in aggregated_data]
        total_prices = [point['total_price'] for point in aggregated_data]
        average_prices = [point['average_price'] for point in aggregated_data]
        
        config = self.price_intervals[interval_type]
        
        return {
            'labels': labels,
            'datasets': [
                {
                    'label': f'総価格 ({config["description"]})',
                    'data': total_prices,
                    'borderColor': '#e74c3c',
                    'backgroundColor': 'rgba(231, 76, 60, 0.1)',
                    'borderWidth': 3,
                    'fill': True,
                    'tension': 0.3,
                    'yAxisID': 'y'
                },
                {
                    'label': f'平均価格 ({config["description"]})',
                    'data': average_prices,
                    'borderColor': '#3498db',
                    'backgroundColor': 'rgba(52, 152, 219, 0.1)',
                    'borderWidth': 2,
                    'fill': False,
                    'tension': 0.3,
                    'yAxisID': 'y1'
                }
            ]
        }

    def save_total_price_data(self):
        """総価格データを全ファイルに保存"""
        try:
            # 30分毎の総価格生データを保存
            total_raw_file = os.path.join(self.history_dir, "total_price_raw_data.json")
            with open(total_raw_file, 'w', encoding='utf-8') as f:
                json.dump(list(self.total_price_raw_data), f, ensure_ascii=False, indent=2)
            
            logger.info(f"総価格30分毎データ保存: {len(self.total_price_raw_data)}ポイント")
            
            # 各間隔の集約済み総価格データを保存
            for interval_type in self.price_intervals:
                if interval_type in self.total_price_history:
                    total_file = os.path.join(self.history_dir, f"total_price_{interval_type}.json")
                    with open(total_file, 'w', encoding='utf-8') as f:
                        json.dump(self.total_price_history[interval_type], f, ensure_ascii=False, indent=2)
                    
                    dataset_count = len(self.total_price_history[interval_type].get('datasets', []))
                    label_count = len(self.total_price_history[interval_type].get('labels', []))
                    
                    logger.info(f"総価格{interval_type}チャートデータ保存: {label_count}ポイント, {dataset_count}データセット")
            
        except Exception as e:
            logger.error(f"総価格データ保存エラー: {e}")

    def update_all_aggregations(self):
        """全ての集約データを更新"""
        try:
            # 現在の総価格を収集
            if not self.collect_current_total_price():
                logger.error("総価格データ収集に失敗しました")
                return False
            
            # 各間隔での集約を実行
            updated_intervals = []
            for interval_type in self.price_intervals:
                chart_data = self.aggregate_total_price_for_interval(interval_type)
                if chart_data:
                    self.total_price_history[interval_type] = chart_data
                    updated_intervals.append(interval_type)
                    
                    # 集約統計をログ出力
                    label_count = len(chart_data.get('labels', []))
                    dataset_count = len(chart_data.get('datasets', []))
                    
                    logger.info(f"総価格{interval_type}集約完了: {label_count}ポイント, {dataset_count}データセット")
            
            if updated_intervals:
                self.save_total_price_data()
                logger.info(f"✅ 総価格集約更新完了: {updated_intervals}")
                return True
            else:
                logger.warning("総価格集約データが更新されませんでした")
                return False
                
        except Exception as e:
            logger.error(f"総価格集約更新エラー: {e}")
            return False

    def get_statistics(self):
        """総価格集約統計情報を取得"""
        stats = {
            'raw_data_points': len(self.total_price_raw_data),
            'intervals': {},
            'configuration': {
                'force_data_refresh': self.force_data_refresh,
                'force_rebuild_aggregation': self.force_rebuild_aggregation,
                'data_collection_interval': '30分毎'
            }
        }
        
        for interval_type, config in self.price_intervals.items():
            if interval_type in self.total_price_history:
                chart_data = self.total_price_history[interval_type]
                label_count = len(chart_data.get('labels', []))
                dataset_count = len(chart_data.get('datasets', []))
                
                stats['intervals'][interval_type] = {
                    'chart_points': label_count,
                    'datasets': dataset_count,
                    'description': config['description'],
                    'max_points': config['maxlen'],
                    'has_data': label_count > 0
                }
            else:
                stats['intervals'][interval_type] = {
                    'chart_points': 0,
                    'datasets': 0,
                    'description': config['description'],
                    'max_points': config['maxlen'],
                    'has_data': False
                }
        
        return stats

def main():
    """メイン実行：総価格集約処理"""
    logger.info("=" * 50)
    logger.info("MapleStory総価格集約処理開始（30分毎データ対応）")
    logger.info("=" * 50)
    
    try:
        # システム初期化
        logger.info("総価格集約システム初期化: data/price_history")
        aggregator = TotalPriceAggregator()
        
        # 全集約データを更新
        success = aggregator.update_all_aggregations()
        
        # 統計表示
        stats = aggregator.get_statistics()
        logger.info(f"📊 総価格集約統計:")
        logger.info(f"  30分毎生データ: {stats['raw_data_points']}ポイント")
        logger.info(f"  設定: 強制リフレッシュ={stats['configuration']['force_data_refresh']}, 強制リビルド={stats['configuration']['force_rebuild_aggregation']}")
        
        for interval, data in stats['intervals'].items():
            status = "✅" if data['has_data'] else "❌"
            logger.info(f"  {interval}: {status} {data['chart_points']}ポイント ({data['description']}) - {data['datasets']}データセット")
        
        logger.info("=" * 50)
        if success:
            logger.info(f"✅ 総価格集約処理完了")
        else:
            logger.info(f"⚠️ 総価格集約処理で問題が発生")
        logger.info("=" * 50)
        
        return success
        
    except Exception as e:
        logger.error(f"メイン実行エラー: {e}")
        raise

if __name__ == "__main__":
    main()
