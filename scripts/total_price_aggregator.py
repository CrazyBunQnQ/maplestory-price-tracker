#!/usr/bin/env python3
import json
import os
from datetime import datetime
from collections import defaultdict
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class TimestampTotalAggregator:
    def __init__(self, history_dir="data/price_history"):
        self.history_dir = history_dir
        self.intervals = ['1hour', '12hour', '1day']
        
    def aggregate_by_timestamp(self, interval):
        """指定間隔の履歴データを時間軸で集計（超シンプル版）"""
        history_file = os.path.join(self.history_dir, f"history_{interval}.json")
        
        if not os.path.exists(history_file):
            logger.warn(f"履歴ファイルが見つかりません: {history_file}")
            return []
        
        try:
            with open(history_file, 'r', encoding='utf-8') as f:
                all_items_data = json.load(f)
            
            # タイムスタンプ別にデータを整理
            timestamp_totals = defaultdict(lambda: {'total_price': 0, 'item_count': 0})
            
            for item_id, item_history in all_items_data.items():
                for data_point in item_history:
                    timestamp = data_point['timestamp']
                    price = data_point['price']
                    
                    # 単純に価格を合計（チェックなし）
                    timestamp_totals[timestamp]['total_price'] += price
                    timestamp_totals[timestamp]['item_count'] += 1
            
            # 時系列データに変換
            total_price_history = []
            for timestamp in sorted(timestamp_totals.keys()):
                data = timestamp_totals[timestamp]
                total_price_history.append({
                    'timestamp': timestamp,
                    'total_price': data['total_price'],
                    'item_count': data['item_count'],
                    'average_price': data['total_price'] // data['item_count'] if data['item_count'] > 0 else 0
                })
            
            logger.info(f"集計完了 {interval}: {len(total_price_history)}タイムスタンプ")
            return total_price_history
            
        except Exception as e:
            logger.error(f"集計エラー {interval}: {e}")
            return []
    
    def save_total_data(self, interval, total_data):
        """総価格データを保存"""
        if not total_data:
            return False
        
        try:
            # 生データとして保存
            raw_file = os.path.join(self.history_dir, f"total_price_raw_{interval}.json")
            with open(raw_file, 'w', encoding='utf-8') as f:
                json.dump(total_data, f, ensure_ascii=False, indent=2)
            
            # Chart.js用データとして保存
            chart_data = self.generate_chart_data(interval, total_data)
            if chart_data:
                chart_file = os.path.join(self.history_dir, f"total_price_{interval}.json")
                with open(chart_file, 'w', encoding='utf-8') as f:
                    json.dump(chart_data, f, ensure_ascii=False, indent=2)
            
            logger.info(f"保存完了 {interval}: {len(total_data)}ポイント")
            return True
            
        except Exception as e:
            logger.error(f"保存エラー {interval}: {e}")
            return False
    
    def generate_chart_data(self, interval, total_data):
        """Chart.js用データを生成（シンプル版）"""
        try:
            def format_time(timestamp_str):
                timestamp = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
                if interval == '1hour':
                    return timestamp.strftime('%m/%d %H:%M')
                elif interval == '12hour':
                    return timestamp.strftime('%m/%d %H:%M')
                else:  # 1day
                    return timestamp.strftime('%m/%d')
            
            return {
                'labels': [format_time(point['timestamp']) for point in total_data],
                'datasets': [
                    {
                        'label': f'総価格 ({interval})',
                        'data': [point['total_price'] for point in total_data],
                        'borderColor': '#e74c3c',
                        'backgroundColor': 'rgba(231, 76, 60, 0.1)',
                        'borderWidth': 3,
                        'fill': True,
                        'tension': 0.3,
                        'yAxisID': 'y'
                    },
                    {
                        'label': f'平均価格 ({interval})',
                        'data': [point['average_price'] for point in total_data],
                        'borderColor': '#3498db',
                        'backgroundColor': 'rgba(52, 152, 219, 0.1)',
                        'borderWidth': 2,
                        'fill': False,
                        'tension': 0.3,
                        'yAxisID': 'y1'
                    }
                ]
            }
        except Exception as e:
            logger.error(f"チャートデータ生成エラー {interval}: {e}")
            return None
    
    def process_all(self):
        """全間隔の処理を実行"""
        results = {}
        
        for interval in self.intervals:
            logger.info(f"処理開始: {interval}")
            
            # 同一時間軸でデータを集計
            total_data = self.aggregate_by_timestamp(interval)
            
            if total_data:
                # 保存
                saved = self.save_total_data(interval, total_data)
                
                results[interval] = {
                    'total_points': len(total_data),
                    'saved': saved,
                    'latest_total': total_data[-1]['total_price'] if total_data else 0,
                    'latest_count': total_data[-1]['item_count'] if total_data else 0
                }
            else:
                results[interval] = {
                    'total_points': 0,
                    'saved': False,
                    'latest_total': 0,
                    'latest_count': 0
                }
        
        return results

def main():
    """メイン実行：超シンプル総価格集計"""
    logger.info("=" * 50)
    logger.info("MapleStory総価格集計（超シンプル版）開始")
    logger.info("=" * 50)
    
    try:
        aggregator = TimestampTotalAggregator()
        results = aggregator.process_all()
        
        # 結果表示
        logger.info("📊 集計結果:")
        for interval, result in results.items():
            logger.info(f"  {interval}: {result['total_points']}ポイント")
            if result['latest_total'] > 0:
                logger.info(f"    最新総価格: {result['latest_total']:,} NESO ({result['latest_count']}アイテム)")
        
        successful = sum(1 for r in results.values() if r['saved'])
        logger.info(f"✅ 完了: {successful}/{len(aggregator.intervals)}間隔成功")
        
        return successful
        
    except Exception as e:
        logger.error(f"❌ エラー: {e}")
        return 0

if __name__ == "__main__":
    main()
