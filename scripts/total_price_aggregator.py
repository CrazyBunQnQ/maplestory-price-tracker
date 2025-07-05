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
        
        # 緩和版設定: より多くのデータポイントを表示
        self.display_limits = {
            '1hour': 168,     # 1週間分全て表示（48→168）
            '12hour': 120,    # 2ヶ月分表示（60→120）
            '1day': 90        # 3ヶ月分表示（30→90）
        }
        
        # 時間区切り設定
        self.bucket_intervals = {
            '1hour': timedelta(hours=1),      # 1時間区切り
            '12hour': timedelta(hours=12),    # 12時間区切り
            '1day': timedelta(days=1)         # 1日区切り
        }
        
        # 緩和版設定
        self.force_aggregation = os.getenv('FORCE_AGGREGATION', 'true').lower() == 'true'
        self.include_zero_prices = os.getenv('INCLUDE_ZERO_PRICES', 'false').lower() == 'true'
        self.min_items_threshold = int(os.getenv('MIN_ITEMS_THRESHOLD', '1'))  # 最小アイテム数
        
        logger.info("🔧 総価格集計システム（緩和版）初期化")
        logger.info(f"🔄 強制集計: {'有効' if self.force_aggregation else '無効'}")
        logger.info(f"💰 ゼロ価格含む: {'有効' if self.include_zero_prices else '無効'}")
        logger.info(f"📊 最小アイテム数: {self.min_items_threshold}")
        
    def safe_parse_price(self, price_value):
        """価格データを安全に解析（緩和版）"""
        try:
            if isinstance(price_value, (int, float)):
                price = int(price_value)
                # 緩和版: ゼロ価格も有効とする設定
                return price if (price >= 0 and self.include_zero_prices) or price > 0 else 0
            
            if isinstance(price_value, str):
                clean_price = price_value.replace(',', '').replace(' NESO', '').strip()
                if clean_price in ['', '未取得', 'undefined', 'None', 'null']:
                    return 0
                
                price = int(float(clean_price))
                return price if (price >= 0 and self.include_zero_prices) or price > 0 else 0
            
            return 0
            
        except (ValueError, TypeError, AttributeError):
            return 0
    
    def round_to_bucket(self, timestamp_str, interval):
        """間隔に応じた時間区切りに丸める"""
        try:
            ts = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
            bucket_interval = self.bucket_intervals[interval]
            
            if interval == '1hour':
                # 1時間区切り
                bucket_start = ts.replace(minute=0, second=0, microsecond=0)
            elif interval == '12hour':
                # 12時間区切り（0時または12時）
                hour_bucket = (ts.hour // 12) * 12
                bucket_start = ts.replace(hour=hour_bucket, minute=0, second=0, microsecond=0)
            else:  # '1day'
                # 1日区切り（0時）
                bucket_start = ts.replace(hour=0, minute=0, second=0, microsecond=0)
            
            return bucket_start.isoformat()
        except Exception as e:
            logger.warn(f"時間丸めエラー: {timestamp_str} -> {e}")
            return timestamp_str
    
    def aggregate_prices_per_bucket(self, item_data_points, interval):
        """時間区切りに応じて価格を平均化（緩和版）"""
        # 時間区切りにデータを集約
        bucketed = defaultdict(list)
        processed_count = 0
        valid_count = 0
        
        for point in item_data_points:
            processed_count += 1
            
            if not isinstance(point, dict) or 'timestamp' not in point or 'price' not in point:
                continue
            
            bucket_time = self.round_to_bucket(point['timestamp'], interval)
            price = self.safe_parse_price(point['price'])
            
            # 緩和版: ゼロ価格も条件次第で含める
            if price > 0 or (price == 0 and self.include_zero_prices):
                bucketed[bucket_time].append(price)
                valid_count += 1
        
        # 各時間区切りの平均価格を計算
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
    
    def limit_data_points(self, data_points, interval):
        """表示最適化のためデータポイントを制限（緩和版）"""
        limit = self.display_limits[interval]
        
        if len(data_points) <= limit:
            return data_points
        
        # 最新のN個を取得
        limited_data = data_points[-limit:]
        logger.info(f"データポイント制限 {interval}: {len(data_points)} -> {len(limited_data)}ポイント")
        
        return limited_data
    
    def aggregate_by_timestamp(self, interval):
        """指定間隔の履歴データを時間区切りで集計（緩和版）"""
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
            
            logger.info(f"履歴データ読み込み {interval}: {len(all_items_data)}アイテム")
            
            # 時間区切りの時間枠別に総価格を集計
            timestamp_totals = defaultdict(lambda: {
                'total_price': 0, 
                'item_count': 0, 
                'total_points': 0,
                'valid_prices': [],
                'zero_prices': 0
            })
            
            processed_items = 0
            total_averaged_points = 0
            error_items = 0
            
            for item_id, item_history in all_items_data.items():
                if not isinstance(item_history, list):
                    error_items += 1
                    continue
                
                processed_items += 1
                
                # アイテム毎に時間区切りで平均化
                try:
                    averaged_points = self.aggregate_prices_per_bucket(item_history, interval)
                    total_averaged_points += len(averaged_points)
                    
                    # 平均化されたポイントを時間軸で合計
                    for avg_point in averaged_points:
                        bucket_time = avg_point['timestamp']
                        avg_price = avg_point['price']
                        original_count = avg_point['original_count']
                        
                        timestamp_totals[bucket_time]['total_price'] += avg_price
                        timestamp_totals[bucket_time]['item_count'] += 1
                        timestamp_totals[bucket_time]['total_points'] += original_count
                        timestamp_totals[bucket_time]['valid_prices'].append(avg_price)
                        
                        if avg_price == 0:
                            timestamp_totals[bucket_time]['zero_prices'] += 1
                
                except Exception as e:
                    error_items += 1
                    logger.debug(f"アイテム処理エラー {item_id}: {e}")
            
            logger.info(f"アイテム処理結果: 成功{processed_items}件、エラー{error_items}件")
            
            # 時系列データに変換
            total_price_history = []
            for bucket_time in sorted(timestamp_totals.keys()):
                data = timestamp_totals[bucket_time]
                
                # 緩和版: 最小アイテム数の閾値を緩和
                if data['item_count'] >= self.min_items_threshold:
                    total_price_history.append({
                        'timestamp': bucket_time,
                        'total_price': data['total_price'],
                        'item_count': data['item_count'],
                        'average_price': data['total_price'] // data['item_count'],
                        'original_points': data['total_points']
                    })
            
            # 表示最適化のためデータポイントを制限
            limited_history = self.limit_data_points(total_price_history, interval)
            
            logger.info(f"時間区切り集計完了 {interval}: {len(limited_history)}時間枠（表示最適化済み）")
            logger.info(f"  処理アイテム: {processed_items}, 区切り間隔: {self.bucket_intervals[interval]}")
            
            # データ品質レポート
            if limited_history:
                prices = [p['total_price'] for p in limited_history]
                item_counts = [p['item_count'] for p in limited_history]
                logger.info(f"  価格範囲: {min(prices):,} - {max(prices):,} NESO")
                logger.info(f"  アイテム数範囲: {min(item_counts)} - {max(item_counts)}件")
                
                # データ期間の計算
                first_time = limited_history[0]['timestamp']
                last_time = limited_history[-1]['timestamp']
                try:
                    first_dt = datetime.fromisoformat(first_time.replace('Z', '+00:00'))
                    last_dt = datetime.fromisoformat(last_time.replace('Z', '+00:00'))
                    span = last_dt - first_dt
                    logger.info(f"  データ期間: {span.days}日{span.seconds//3600}時間")
                except Exception as e:
                    logger.debug(f"期間計算エラー: {e}")
            
            return limited_history
            
        except json.JSONDecodeError as e:
            logger.error(f"JSON読み込みエラー {interval}: {e}")
            return []
        except Exception as e:
            logger.error(f"集計エラー {interval}: {e}")
            return []
    
    def generate_chart_data(self, interval, total_data):
        """Chart.js用データを生成（緩和版）"""
        if not total_data:
            logger.error(f"チャートデータなし: {interval}")
            return None
        
        try:
            def format_time(timestamp_str):
                try:
                    timestamp = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
                    if interval == '1hour':
                        return timestamp.strftime('%m/%d %H:00')
                    elif interval == '12hour':
                        return timestamp.strftime('%m/%d %H:00')
                    else:  # 1day
                        return timestamp.strftime('%m/%d')
                except Exception as e:
                    logger.warn(f"時刻フォーマットエラー: {timestamp_str} -> {e}")
                    return str(timestamp_str)
            
            # データ検証（緩和版）
            valid_points = []
            for point in total_data:
                if (isinstance(point, dict) and 
                    'timestamp' in point and 
                    'total_price' in point and 
                    'average_price' in point and
                    isinstance(point['total_price'], int) and
                    isinstance(point['average_price'], int)):
                    
                    # 緩和版: ゼロ価格も条件次第で有効
                    if point['total_price'] > 0 or (point['total_price'] == 0 and self.include_zero_prices):
                        valid_points.append(point)
            
            if len(valid_points) == 0:
                logger.error(f"有効なデータポイントがありません: {interval}")
                return None
            
            labels = [format_time(point['timestamp']) for point in valid_points]
            total_prices = [point['total_price'] for point in valid_points]
            average_prices = [point['average_price'] for point in valid_points]
            
            # 間隔の説明
            interval_descriptions = {
                '1hour': '1週間分（1時間毎）',
                '12hour': '1ヶ月分（12時間毎）',
                '1day': '1年分（1日毎）'
            }
            
            chart_data = {
                'labels': labels,
                'datasets': [
                    {
                        'label': f'総価格 ({interval_descriptions.get(interval, interval)})',
                        'data': total_prices,
                        'borderColor': '#e74c3c',
                        'backgroundColor': 'rgba(231, 76, 60, 0.1)',
                        'borderWidth': 2,
                        'fill': True,
                        'tension': 0.4,
                        'pointRadius': 2,
                        'pointHoverRadius': 4,
                        'yAxisID': 'y'
                    },
                    {
                        'label': f'平均価格 ({interval_descriptions.get(interval, interval)})',
                        'data': average_prices,
                        'borderColor': '#3498db',
                        'backgroundColor': 'rgba(52, 152, 219, 0.1)',
                        'borderWidth': 2,
                        'fill': False,
                        'tension': 0.4,
                        'pointRadius': 1,
                        'pointHoverRadius': 3,
                        'yAxisID': 'y1'
                    }
                ]
            }
            
            logger.info(f"チャートデータ生成完了 {interval}: {len(valid_points)}ポイント")
            logger.info(f"  時間範囲: {labels[0]} ～ {labels[-1]}")
            
            return chart_data
            
        except Exception as e:
            logger.error(f"チャートデータ生成エラー {interval}: {e}")
            return None
    
    def save_total_data(self, interval, total_data):
        """総価格データを保存（緩和版）"""
        if not total_data:
            logger.warn(f"保存するデータがありません: {interval}")
            return False
        
        try:
            # 生データとして保存（詳細情報保持）
            raw_file = os.path.join(self.history_dir, f"total_price_optimized_{interval}.json")
            with open(raw_file, 'w', encoding='utf-8') as f:
                json.dump(total_data, f, ensure_ascii=False, indent=2)
            
            # Chart.js用データとして保存（従来のファイル名維持）
            chart_data = self.generate_chart_data(interval, total_data)
            if chart_data:
                chart_file = os.path.join(self.history_dir, f"total_price_{interval}.json")
                with open(chart_file, 'w', encoding='utf-8') as f:
                    json.dump(chart_data, f, ensure_ascii=False, indent=2)
                
                logger.info(f"保存完了 {interval}: {len(total_data)}時間枠 -> {len(chart_data['labels'])}チャートポイント")
            else:
                logger.error(f"チャートデータ生成失敗: {interval}")
                return False
                
            return True
            
        except Exception as e:
            logger.error(f"保存エラー {interval}: {e}")
            return False
    
    def process_all(self):
        """全間隔の処理を実行（緩和版）"""
        results = {}
        
        logger.info("🚀 緩和版集計処理開始")
        
        for interval in self.intervals:
            logger.info(f"処理開始: {interval}")
            
            # 時間区切りで集計（緩和版）
            total_data = self.aggregate_by_timestamp(interval)
            
            if total_data:
                # 保存
                saved = self.save_total_data(interval, total_data)
                
                results[interval] = {
                    'total_buckets': len(total_data),
                    'saved': saved,
                    'latest_total': total_data[-1]['total_price'] if total_data else 0,
                    'latest_count': total_data[-1]['item_count'] if total_data else 0,
                    'bucket_interval': str(self.bucket_intervals[interval]),
                    'display_limit': self.display_limits[interval]
                }
                
                # データ品質チェック
                if total_data:
                    prices = [p['total_price'] for p in total_data]
                    logger.info(f"  価格範囲: {min(prices):,} - {max(prices):,} NESO")
            else:
                results[interval] = {
                    'total_buckets': 0,
                    'saved': False,
                    'latest_total': 0,
                    'latest_count': 0,
                    'bucket_interval': str(self.bucket_intervals[interval]),
                    'display_limit': self.display_limits[interval]
                }
        
        return results

def main():
    """メイン実行：緩和版総価格集計"""
    logger.info("=" * 50)
    logger.info("MapleStory総価格集計（緩和版）開始")
    logger.info("=" * 50)
    
    try:
        aggregator = TimestampTotalAggregator()
        
        # 集計処理実行
        results = aggregator.process_all()
        
        # 結果表示
        logger.info("📊 緩和版集計結果:")
        for interval, result in results.items():
            logger.info(f"  {interval}: {result['total_buckets']}時間枠")
            logger.info(f"    区切り間隔: {result['bucket_interval']}")
            logger.info(f"    表示制限: {result['display_limit']}ポイント")
            if result['latest_total'] > 0:
                logger.info(f"    最新総価格: {result['latest_total']:,} NESO ({result['latest_count']}アイテム)")
        
        successful = sum(1 for r in results.values() if r['saved'])
        logger.info(f"✅ 緩和版処理完了: {successful}/{len(aggregator.intervals)}間隔成功")
        
        return successful
        
    except Exception as e:
        logger.error(f"❌ エラー: {e}")
        return 0

if __name__ == "__main__":
    main()
