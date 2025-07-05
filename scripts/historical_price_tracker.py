#!/usr/bin/env python3
import json
import time
import os
from datetime import datetime, timedelta
from collections import deque
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class HistoricalPriceTracker:
    def __init__(self, json_file_path="data/equipment_prices.json", 
                 history_dir="data/price_history"):
        self.json_file_path = json_file_path
        self.history_dir = history_dir
        
        # 緩和版設定: デフォルトで強制検出を有効化
        self.force_price_detection = os.getenv('FORCE_PRICE_DETECTION', 'true').lower() == 'true'
        self.force_rebuild_history = os.getenv('FORCE_REBUILD_HISTORY', 'false').lower() == 'true'
        
        # 緩和版設定: より積極的なデータ蓄積
        self.relaxed_mode = os.getenv('RELAXED_MODE', 'true').lower() == 'true'
        self.time_threshold_ratio = float(os.getenv('TIME_THRESHOLD_RATIO', '0.7'))  # 70%経過で更新
        
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
        
        # 各アイテムの価格履歴を管理するディクショナリ（個別アイテムのみ）
        self.price_history = {}
        
        # 現在の価格を記録するためのキャッシュ
        self.current_prices = {}
        
        # 緩和版: 更新統計
        self.update_statistics = {
            'forced_updates': 0,
            'time_based_updates': 0,
            'price_change_updates': 0,
            'first_time_updates': 0
        }
        
        logger.info("🔧 価格履歴追跡システム（緩和版）初期化")
        logger.info(f"🔄 強制価格検出: {'有効' if self.force_price_detection else '無効'}")
        logger.info(f"⚡ 緩和モード: {'有効' if self.relaxed_mode else '無効'}")
        logger.info(f"⏰ 時間閾値: {self.time_threshold_ratio*100:.0f}%経過で更新")
        
        self.load_existing_history()

    def load_existing_history(self):
        """既存の価格履歴を読み込み（個別アイテムのみ）"""
        try:
            total_records = 0
            oldest_record = None
            newest_record = None
            
            for interval_type in self.price_intervals:
                history_file = os.path.join(self.history_dir, f"history_{interval_type}.json")
                if os.path.exists(history_file):
                    with open(history_file, 'r', encoding='utf-8') as f:
                        data = json.load(f)
                        item_count = len(data)
                        for item_id, history in data.items():
                            if item_id not in self.price_history:
                                self.price_history[item_id] = {}
                            
                            # dequeに変換して最大長を適用
                            self.price_history[item_id][interval_type] = deque(
                                history, 
                                maxlen=self.price_intervals[interval_type]['maxlen']
                            )
                            total_records += len(history)
                            
                            # 最古・最新記録の追跡
                            for record in history:
                                timestamp = record.get('timestamp')
                                if timestamp:
                                    if oldest_record is None or timestamp < oldest_record:
                                        oldest_record = timestamp
                                    if newest_record is None or timestamp > newest_record:
                                        newest_record = timestamp
                        
                        logger.info(f"{interval_type} 履歴ファイル読み込み: {item_count}アイテム")
            
            logger.info(f"個別アイテム価格履歴読み込み完了: {len(self.price_history)}アイテム、{total_records}レコード")
            
            if oldest_record and newest_record:
                try:
                    oldest_dt = datetime.fromisoformat(oldest_record.replace('Z', '+00:00'))
                    newest_dt = datetime.fromisoformat(newest_record.replace('Z', '+00:00'))
                    data_span = newest_dt - oldest_dt
                    logger.info(f"データ蓄積期間: {data_span.days}日 {data_span.seconds//3600}時間")
                except Exception as e:
                    logger.debug(f"データ期間計算エラー: {e}")
            
            if self.force_price_detection:
                logger.info("🔄 強制価格検出モードが有効です")
            
        except Exception as e:
            logger.error(f"価格履歴読み込みエラー: {e}")

    def save_history_to_files(self):
        """価格履歴を間隔別ファイルに保存（個別アイテムのみ）"""
        try:
            for interval_type in self.price_intervals:
                history_file = os.path.join(self.history_dir, f"history_{interval_type}.json")
                
                # dequeをリストに変換して保存
                interval_data = {}
                total_points = 0
                
                for item_id, intervals in self.price_history.items():
                    if interval_type in intervals and len(intervals[interval_type]) > 0:
                        interval_data[item_id] = list(intervals[interval_type])
                        total_points += len(intervals[interval_type])
                
                with open(history_file, 'w', encoding='utf-8') as f:
                    json.dump(interval_data, f, ensure_ascii=False, indent=2)
                
                logger.info(f"{interval_type} 個別履歴保存完了: {len(interval_data)}アイテム、{total_points}ポイント")
        except Exception as e:
            logger.error(f"価格履歴保存エラー: {e}")

    def should_update_interval(self, item_id, interval_type, current_price):
        """緩和版: より積極的な更新判定（関数名は維持）"""
        
        # 強制検出モードの場合は無条件更新
        if self.force_price_detection:
            self.update_statistics['forced_updates'] += 1
            return True
        
        # 初回データの場合
        if item_id not in self.price_history:
            self.update_statistics['first_time_updates'] += 1
            return True
        
        if interval_type not in self.price_history[item_id]:
            self.update_statistics['first_time_updates'] += 1
            return True
        
        history = self.price_history[item_id][interval_type]
        if not history:
            self.update_statistics['first_time_updates'] += 1
            return True
        
        last_entry = history[-1]
        last_time = datetime.fromisoformat(last_entry['timestamp'].replace('Z', '+00:00'))
        last_price = last_entry.get('price', 0)
        now = datetime.now()
        
        required_interval = self.price_intervals[interval_type]['interval']
        
        # 緩和版: 時間閾値を70%に設定（より頻繁な更新）
        time_threshold = required_interval.total_seconds() * self.time_threshold_ratio
        elapsed_seconds = (now - last_time).total_seconds()
        time_condition = elapsed_seconds >= time_threshold
        
        # **重要な修正**: 価格変更も検出対象に追加
        price_changed = current_price != last_price and current_price > 0
        
        # 緩和モードでは時間条件を優先
        if self.relaxed_mode:
            if time_condition:
                self.update_statistics['time_based_updates'] += 1
                elapsed_hours = elapsed_seconds / 3600
                logger.debug(f"時間経過更新 {item_id} ({interval_type}): {elapsed_hours:.1f}h >= {time_threshold/3600:.1f}h")
                return True
            elif price_changed:
                self.update_statistics['price_change_updates'] += 1
                logger.info(f"価格変更検出 {item_id} ({interval_type}): {last_price:,} -> {current_price:,}")
                return True
        else:
            # 従来の条件（時間経過 OR 価格変更）
            should_update = time_condition or price_changed
            
            if should_update and price_changed:
                self.update_statistics['price_change_updates'] += 1
                logger.info(f"価格変更検出 {item_id} ({interval_type}): {last_price:,} -> {current_price:,}")
            elif should_update:
                self.update_statistics['time_based_updates'] += 1
                
            return should_update
        
        return False

    def detect_price_changes_from_last_updated(self, item_data):
        """last_updatedフィールドによる最近の更新検出（緩和版）"""
        try:
            last_updated = item_data.get('last_updated')
            if not last_updated:
                return False
            
            # last_updatedの時刻をパース
            update_time = datetime.fromisoformat(last_updated.replace('Z', '+00:00'))
            now = datetime.now()
            
            # 緩和版: 2時間以内の更新を検出（従来は1時間）
            time_diff = (now - update_time).total_seconds()
            is_recent_update = time_diff < 7200  # 2時間
            
            if is_recent_update:
                logger.info(f"最近の更新検出: {item_data.get('item_name')} - {time_diff:.0f}秒前")
                return True
                
            return False
            
        except Exception as e:
            logger.debug(f"last_updated解析エラー: {e}")
            return False

    def update_price_history(self, item_id, item_name, current_price):
        """価格履歴を更新（緩和版・関数名維持）"""
        timestamp = datetime.now().isoformat()
        price_point = {
            'timestamp': timestamp,
            'price': current_price,
            'item_name': item_name
        }
        
        # アイテム初期化
        if item_id not in self.price_history:
            self.price_history[item_id] = {}
        
        # 現在価格をキャッシュに保存
        self.current_prices[item_id] = current_price
        
        # 各間隔での更新判定と追加（緩和版）
        updated_intervals = []
        for interval_type, config in self.price_intervals.items():
            if self.should_update_interval(item_id, interval_type, current_price):
                if interval_type not in self.price_history[item_id]:
                    self.price_history[item_id][interval_type] = deque(maxlen=config['maxlen'])
                
                self.price_history[item_id][interval_type].append(price_point)
                updated_intervals.append(interval_type)
        
        if updated_intervals:
            logger.info(f"{item_name} 価格履歴更新: {updated_intervals}")
        
        return updated_intervals

    def update_from_current_prices(self):
        """現在の価格JSONから履歴を更新（緩和版・関数名維持）"""
        try:
            if not os.path.exists(self.json_file_path):
                logger.error(f"価格ファイルが見つかりません: {self.json_file_path}")
                return 0
            
            # ファイルの最終更新時刻をチェック
            file_mtime = os.path.getmtime(self.json_file_path)
            file_age = time.time() - file_mtime
            
            logger.info(f"価格ファイル確認: {self.json_file_path} (更新から{file_age:.0f}秒経過)")
            
            with open(self.json_file_path, 'r', encoding='utf-8') as f:
                current_data = json.load(f)
            
            logger.info(f"現在の価格データ読み込み: {len(current_data)}アイテム")
            
            # 統計用カウンタリセット
            self.update_statistics = {k: 0 for k in self.update_statistics}
            
            updated_count = 0
            processed_count = 0
            force_updated_count = 0
            recent_update_count = 0
            
            for item_id, item_data in current_data.items():
                processed_count += 1
                
                # データ検証
                if not item_data or not isinstance(item_data, dict):
                    continue
                    
                if not item_data.get('item_name') or not item_data.get('item_price'):
                    continue
                
                # 最近の更新検出（緩和版）
                is_recent_update = self.detect_price_changes_from_last_updated(item_data)
                if is_recent_update:
                    recent_update_count += 1
                
                # 価格文字列を数値に変換
                price_str = str(item_data['item_price']).replace(',', '').replace(' NESO', '').strip()
                try:
                    current_price = int(price_str)
                    if current_price > 0:
                        # 強制検出モードまたは最近の更新の場合は無条件で更新
                        if self.force_price_detection or is_recent_update:
                            if self.force_price_detection:
                                force_updated_count += 1
                            
                            intervals = self.update_price_history(
                                item_id, 
                                item_data['item_name'], 
                                current_price
                            )
                        else:
                            intervals = self.update_price_history(
                                item_id, 
                                item_data['item_name'], 
                                current_price
                            )
                        
                        if intervals:
                            updated_count += 1
                            
                except (ValueError, TypeError) as e:
                    logger.debug(f"価格変換エラー ({item_id}): {price_str} -> {e}")
                    continue
            
            logger.info(f"個別アイテム処理完了: 処理{processed_count}件、更新{updated_count}件")
            
            # 緩和版統計表示
            logger.info(f"更新理由別統計:")
            for reason, count in self.update_statistics.items():
                if count > 0:
                    logger.info(f"  {reason}: {count}回")
            
            if self.force_price_detection:
                logger.info(f"🔄 強制検出による更新: {force_updated_count}件")
            
            if recent_update_count > 0:
                logger.info(f"📅 最近の更新検出: {recent_update_count}件")
            
            if updated_count > 0:
                self.save_history_to_files()
                logger.info(f"✅ 個別価格履歴更新完了: {updated_count}アイテム")
            else:
                if not self.force_price_detection and not self.relaxed_mode:
                    logger.info("💡 価格変更が検出されませんでした（FORCE_PRICE_DETECTION=true を試してください）")
                else:
                    logger.info("⚠️ 緩和設定でも更新がありませんでした")
            
            return updated_count
            
        except FileNotFoundError:
            logger.error(f"価格ファイルが見つかりません: {self.json_file_path}")
            return 0
        except json.JSONDecodeError as e:
            logger.error(f"JSONファイル読み込みエラー: {e}")
            return 0
        except Exception as e:
            logger.error(f"価格履歴更新エラー: {e}")
            return 0

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
        def format_time(timestamp_str):
            try:
                timestamp = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
                if interval == '1hour':
                    return timestamp.strftime('%m/%d %H:%M')
                elif interval == '12hour':
                    return timestamp.strftime('%m/%d %H:%M')
                else:  # 1day
                    return timestamp.strftime('%m/%d')
            except:
                return timestamp_str
        
        return {
            'labels': [format_time(point['timestamp']) for point in history],
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
            logger.error(f"チャートデータ出力エラー ({item_id}, {interval}): {e}")
            return False

    def get_statistics(self):
        """履歴統計情報を取得"""
        stats = {
            'total_items': len(self.price_history),
            'intervals': {},
            'configuration': {
                'force_price_detection': self.force_price_detection,
                'relaxed_mode': self.relaxed_mode,
                'time_threshold_ratio': self.time_threshold_ratio
            }
        }
        
        for interval_type, config in self.price_intervals.items():
            item_count = sum(1 for item in self.price_history.values() 
                           if interval_type in item and len(item[interval_type]) > 0)
            total_points = sum(len(item[interval_type]) for item in self.price_history.values() 
                             if interval_type in item)
            
            stats['intervals'][interval_type] = {
                'items_with_data': item_count,
                'total_data_points': total_points,
                'description': config['description'],
                'max_points': config['maxlen'],
                'average_points_per_item': total_points / max(item_count, 1)
            }
        
        return stats

def main():
    """メイン実行：現在の価格から履歴を更新（緩和版）"""
    logger.info("=" * 50)
    logger.info("MapleStory個別アイテム価格履歴更新開始（緩和版）")
    logger.info("=" * 50)
    
    try:
        # システム初期化
        logger.info("個別アイテム価格追跡システム初期化: data/price_history")
        tracker = HistoricalPriceTracker()
        
        # 現在の価格データから履歴更新
        updated = tracker.update_from_current_prices()
        
        # 統計表示
        stats = tracker.get_statistics()
        logger.info(f"📊 個別アイテム価格履歴統計:")
        logger.info(f"  総アイテム数: {stats['total_items']}")
        logger.info(f"  設定: 強制検出={stats['configuration']['force_price_detection']}, 緩和モード={stats['configuration']['relaxed_mode']}")
        for interval, data in stats['intervals'].items():
            logger.info(f"  {interval}: {data['items_with_data']}件 ({data['description']}) - {data['total_data_points']}ポイント")
        
        # 緩和版結果表示
        if tracker.force_price_detection or tracker.relaxed_mode:
            logger.info(f"🔄 緩和版モード結果: {updated}アイテム更新")
        
        logger.info("=" * 50)
        logger.info(f"✅ 個別アイテム更新完了: {updated}アイテム")
        logger.info("=" * 50)
        
        return updated
        
    except Exception as e:
        logger.error(f"メイン実行エラー: {e}")
        raise

if __name__ == "__main__":
    main()
