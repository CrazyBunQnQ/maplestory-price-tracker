#!/usr/bin/env python3
import json
import os
from datetime import datetime, timedelta
from collections import defaultdict
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class TimestampTotalAggregator:
    def __init__(self, history_dir="data/price_history"):
        self.history_dir = history_dir
        self.intervals = ['1hour', '12hour', '1day']
        
    def safe_parse_price(self, price_value):
        """価格データを安全に解析"""
        try:
            if isinstance(price_value, (int, float)):
                return int(price_value) if price_value > 0 else 0
            
            if isinstance(price_value, str):
                clean_price = price_value.replace(',', '').replace(' NESO', '').strip()
                if clean_price in ['', '未取得', 'undefined', 'None', 'null']:
                    return 0
                return int(float(clean_price)) if float(clean_price) > 0 else 0
            
            return 0
            
        except (ValueError, TypeError, AttributeError):
            return 0
    
    def round_to_30min_bucket(self, timestamp_str):
        """30分区切りの時間枠に丸める（検索結果[1]の手法）"""
        try:
            ts = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
            # 30分単位の時間枠に丸める
            bucket_start = ts.replace(
                minute=(ts.minute // 30) * 30,
                second=0,
                microsecond=0
            )
            return bucket_start.isoformat()
        except Exception as e:
            logger.warn(f"時間丸めエラー: {timestamp_str} -> {e}")
            return timestamp_str
    
    def aggregate_prices_per_30min(self, item_data_points):
        """30分毎に区切り、同じ時間枠内の価格を平均化（検索結果[1]ベース）"""
        # 30分区切りの時間枠にデータを集約
        bucketed = defaultdict(list)
        
        for point in item_data_points:
            if not isinstance(point, dict) or 'timestamp' not in point or 'price' not in point:
                continue
            
            bucket_time = self.round_to_30min_bucket(point['timestamp'])
            price = self.safe_parse_price(point['price'])
            
            if price > 0:
                bucketed[bucket_time].append(price)
        
        # 各時間枠の平均価格を計算
        averaged_data = []
        for bucket_time in sorted(bucketed.keys()):
            prices = bucketed[bucket_time]
            if prices:
                avg_price = sum(prices) // len(prices)  # 整数平均
                averaged_data.append({
                    'timestamp': bucket_time,
                    'price': avg_price,
                    'original_count': len(prices)
                })
        
        return averaged_data
    
    def aggregate_by_timestamp(self, interval):
        """指定間隔の履歴データを30分区切りで時間軸集計"""
        history_file = os.path.join(self.history_dir, f"history_{interval}.json")
        
        if not os.path.exists(history_file):
            logger.warn(f"履歴ファイルが見つかりません: {history_file}")
            return []
        
        try:
            with open(history_file, 'r', encoding='utf-8') as f:
                all_items_data = json.load(f)
            
            if not isinstance(all_items_data, dict):
                logger.error(f"履歴データが辞書型ではありません: {type(all_items_data)}")
                return []
            
            # 30分区切りの時間枠別に総価格を集計
            timestamp_totals = defaultdict(lambda: {'total_price': 0, 'item_count': 0, 'total_points': 0})
            
            processed_items = 0
            total_averaged_points = 0
            
            for item_id, item_history in all_items_data.items():
                if not isinstance(item_history, list):
                    continue
                
                processed_items += 1
                
                # アイテム毎に30分区切りで平均化
                averaged_points = self.aggregate_prices_per_30min(item_history)
                total_averaged_points += len(averaged_points)
                
                # 平均化されたポイントを時間軸で合計
                for avg_point in averaged_points:
                    bucket_time = avg_point['timestamp']
                    avg_price = avg_point['price']
                    original_count = avg_point['original_count']
                    
                    timestamp_totals[bucket_time]['total_price'] += avg_price
                    timestamp_totals[bucket_time]['item_count'] += 1
                    timestamp_totals[bucket_time]['total_points'] += original_count
            
            # 時系列データに変換
            total_price_history = []
            for bucket_time in sorted(timestamp_totals.keys()):
                data = timestamp_totals[bucket_time]
                
                if data['item_count'] > 0:
                    total_price_history.append({
                        'timestamp': bucket_time,
                        'total_price': data['total_price'],
                        'item_count': data['item_count'],
                        'average_price': data['total_price'] // data['item_count'],
                        'original_points': data['total_points']  # デバッグ用
                    })
            
            logger.info(f"30分区切り集計完了 {interval}: {len(total_price_history)}時間枠")
            logger.info(f"  処理アイテム: {processed_items}, 平均化後ポイント: {total_averaged_points}")
            
            return total_price_history
            
        except json.JSONDecodeError as e:
            logger.error(f"JSON読み込みエラー {interval}: {e}")
            return []
        except Exception as e:
            logger.error(f"集計エラー {interval}: {e}")
            return []
    
    def generate_chart_data(self, interval, total_data):
        """Chart.js用データを生成（30分区切り対応）"""
        if not total_data:
            logger.error(f"チャートデータなし: {interval}")
            return None
        
        # 30分区切りなので必ず連続したデータになる
        try:
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
                    logger.warn(f"時刻フォーマットエラー: {timestamp_str} -> {e}")
                    return str(timestamp_str)
            
            # データ検証
            valid_points = []
            for point in total_data:
                if (isinstance(point, dict) and 
                    'timestamp' in point and 
                    'total_price' in point and 
                    'average_price' in point and
                    isinstance(point['total_price'], int) and
                    isinstance(point['average_price'], int) and
                    point['total_price'] > 0):
                    valid_points.append(point)
            
            if len(valid_points) == 0:
                logger.error(f"有効なデータポイントがありません: {interval}")
                return None
            
            labels = [format_time(point['timestamp']) for point in valid_points]
            total_prices = [point['total_price'] for point in valid_points]
            average_prices = [point['average_price'] for point in valid_points]
            
            chart_data = {
                'labels': labels,
                'datasets': [
                    {
                        'label': f'総価格 ({interval}) - 30分区切り平均',
                        'data': total_prices,
                        'borderColor': '#e74c3c',
                        'backgroundColor': 'rgba(231, 76, 60, 0.1)',
                        'borderWidth': 3,
                        'fill': True,
                        'tension': 0.3,
                        'pointRadius': 4,
                        'pointHoverRadius': 6,
                        'yAxisID': 'y'
                    },
                    {
                        'label': f'平均価格 ({interval}) - 30分区切り平均',
                        'data': average_prices,
                        'borderColor': '#3498db',
                        'backgroundColor': 'rgba(52, 152, 219, 0.1)',
                        'borderWidth': 2,
                        'fill': False,
                        'tension': 0.3,
                        'pointRadius': 3,
                        'pointHoverRadius': 5,
                        'yAxisID': 'y1'
                    }
                ]
            }
            
            logger.info(f"30分区切りチャートデータ生成完了 {interval}: {len(valid_points)}ポイント")
            logger.info(f"  時間範囲: {labels[0]} ～ {labels[-1]}")
            
            return chart_data
            
        except Exception as e:
            logger.error(f"チャートデータ生成エラー {interval}: {e}")
            return None
    
    def save_total_data(self, interval, total_data):
        """総価格データを保存（30分区切り対応）"""
        if not total_data:
            logger.warn(f"保存するデータがありません: {interval}")
            return False
        
        try:
            # 生データとして保存
            raw_file = os.path.join(self.history_dir, f"total_price_30min_{interval}.json")
            with open(raw_file, 'w', encoding='utf-8') as f:
                json.dump(total_data, f, ensure_ascii=False, indent=2)
            
            # Chart.js用データとして保存
            chart_data = self.generate_chart_data(interval, total_data)
            if chart_data:
                chart_file = os.path.join(self.history_dir, f"total_price_{interval}.json")
                with open(chart_file, 'w', encoding='utf-8') as f:
                    json.dump(chart_data, f, ensure_ascii=False, indent=2)
                
                logger.info(f"30分区切り保存完了 {interval}: {len(total_data)}時間枠 -> {len(chart_data['labels'])}チャートポイント")
            else:
                logger.error(f"チャートデータ生成失敗: {interval}")
                return False
                
            return True
            
        except Exception as e:
            logger.error(f"保存エラー {interval}: {e}")
            return False
    
    def process_all(self):
        """全間隔の処理を実行（30分区切り対応）"""
        results = {}
        
        for interval in self.intervals:
            logger.info(f"30分区切り処理開始: {interval}")
            
            # 30分区切りで時間軸集計
            total_data = self.aggregate_by_timestamp(interval)
            
            if total_data:
                # 保存
                saved = self.save_total_data(interval, total_data)
                
                results[interval] = {
                    'total_buckets': len(total_data),
                    'saved': saved,
                    'latest_total': total_data[-1]['total_price'] if total_data else 0,
                    'latest_count': total_data[-1]['item_count'] if total_data else 0,
                    'time_range': f"{total_data[0]['timestamp']} ～ {total_data[-1]['timestamp']}" if total_data else "N/A"
                }
                
                # データ品質チェック
                if total_data:
                    prices = [p['total_price'] for p in total_data]
                    logger.info(f"  30分区切り価格範囲: {min(prices):,} - {max(prices):,} NESO")
            else:
                results[interval] = {
                    'total_buckets': 0,
                    'saved': False,
                    'latest_total': 0,
                    'latest_count': 0,
                    'time_range': "N/A"
                }
        
        return results
    
    def debug_30min_bucketing(self, interval, max_items=2):
        """30分区切りのデバッグ表示"""
        history_file = os.path.join(self.history_dir, f"history_{interval}.json")
        
        if not os.path.exists(history_file):
            return
        
        try:
            with open(history_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            logger.info(f"30分区切りデバッグ {interval}:")
            
            for i, (item_id, item_history) in enumerate(data.items()):
                if i >= max_items:
                    break
                
                logger.info(f"  アイテム {item_id}:")
                logger.info(f"    元データ数: {len(item_history)}")
                
                # 30分区切り平均化テスト
                averaged = self.aggregate_prices_per_30min(item_history)
                logger.info(f"    30分区切り後: {len(averaged)}時間枠")
                
                for bucket in averaged[:3]:  # 最初の3時間枠を表示
                    logger.info(f"      {bucket['timestamp']}: {bucket['price']:,} NESO (元{bucket['original_count']}ポイント)")
                        
        except Exception as e:
            logger.error(f"30分区切りデバッグエラー {interval}: {e}")

def main():
    """メイン実行：30分区切り平均化総価格集計"""
    logger.info("=" * 50)
    logger.info("MapleStory総価格集計（30分区切り平均化版）開始")
    logger.info("=" * 50)
    
    try:
        aggregator = TimestampTotalAggregator()
        
        # 30分区切りデバッグ表示
        logger.info("🕐 30分区切りデバッグ:")
        for interval in aggregator.intervals:
            aggregator.debug_30min_bucketing(interval)
        
        # 集計処理実行
        results = aggregator.process_all()
        
        # 結果表示
        logger.info("📊 30分区切り集計結果:")
        for interval, result in results.items():
            logger.info(f"  {interval}: {result['total_buckets']}時間枠")
            logger.info(f"    時間範囲: {result['time_range']}")
            if result['latest_total'] > 0:
                logger.info(f"    最新総価格: {result['latest_total']:,} NESO ({result['latest_count']}アイテム)")
        
        successful = sum(1 for r in results.values() if r['saved'])
        logger.info(f"✅ 30分区切り処理完了: {successful}/{len(aggregator.intervals)}間隔成功")
        
        return successful
        
    except Exception as e:
        logger.error(f"❌ エラー: {e}")
        return 0

if __name__ == "__main__":
    main()
