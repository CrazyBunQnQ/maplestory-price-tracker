#!/usr/bin/env python3
import json
import time
import os
from datetime import datetime, timedelta
from collections import deque, defaultdict
import logging
import statistics

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
        self.time_threshold_ratio = float(os.getenv('TIME_THRESHOLD_RATIO', '0.9'))  # 70%経過で更新
        
        # ディレクトリ作成
        os.makedirs(history_dir, exist_ok=True)
        
        # 修正: 30分毎データ収集と時間間隔別集約設定
        self.raw_data_interval = timedelta(minutes=30)  # 30分毎の生データ
        
        # チャート用時間間隔設定（30分毎データから集約）
        self.price_intervals = {
            '1hour': {
                'interval': timedelta(hours=1),
                'maxlen': 168,  # 1週間分（168時間）
                'description': '1週間分（1時間毎）',
                'aggregation_points': 2  # 30分×2 = 1時間
            },
            '12hour': {
                'interval': timedelta(hours=12),
                'maxlen': 60,   # 1ヶ月分（60回 = 30日）
                'description': '1ヶ月分（12時間毎）',
                'aggregation_points': 24  # 30分×24 = 12時間
            },
            '1day': {
                'interval': timedelta(days=1),
                'maxlen': 365,  # 1年分（365日）
                'description': '1年分（1日毎）',
                'aggregation_points': 48  # 30分×48 = 24時間
            }
        }
        
        # 30分毎の生データ保存用（個別アイテム）
        self.raw_price_data = {}
        
        # 集約された価格履歴（個別アイテム）
        self.price_history = {}
        
        # 総価格データ（30分毎の生データ）
        self.total_price_raw_data = deque(maxlen=2880)  # 30日分の30分毎データ
        
        # 総価格履歴（集約済み）
        self.total_price_history = {}
        
        # 現在の価格を記録するためのキャッシュ
        self.current_prices = {}
        
        # 緩和版: 更新統計
        self.update_statistics = {
            'forced_updates': 0,
            'time_based_updates': 0,
            'price_change_updates': 0,
            'first_time_updates': 0
        }
        
        logger.info("🔧 価格履歴追跡システム（30分毎データ集約版）初期化")
        logger.info(f"🔄 強制価格検出: {'有効' if self.force_price_detection else '無効'}")
        logger.info(f"⚡ 緩和モード: {'有効' if self.relaxed_mode else '無効'}")
        logger.info(f"⏰ 時間閾値: {self.time_threshold_ratio*100:.0f}%経過で更新")
        logger.info(f"📊 データ収集間隔: 30分毎")
        
        self.load_existing_history()

    def load_existing_history(self):
        """既存の価格履歴を読み込み（30分毎データと集約データ）"""
        try:
            # 30分毎の生データを読み込み
            self.load_raw_data()
            
            # 集約済みデータを読み込み
            self.load_aggregated_data()
            
            # 総価格データを読み込み
            self.load_total_price_data()
            
        except Exception as e:
            logger.error(f"価格履歴読み込みエラー: {e}")

    def load_raw_data(self):
        """30分毎の生データを読み込み"""
        try:
            raw_data_file = os.path.join(self.history_dir, "raw_price_data.json")
            if os.path.exists(raw_data_file):
                with open(raw_data_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    
                for item_id, raw_history in data.items():
                    self.raw_price_data[item_id] = deque(
                        raw_history, 
                        maxlen=2880  # 30日分の30分毎データ
                    )
                    
                logger.info(f"30分毎生データ読み込み: {len(self.raw_price_data)}アイテム")
        except Exception as e:
            logger.warning(f"30分毎生データ読み込みエラー: {e}")

    def load_aggregated_data(self):
        """集約済みデータを読み込み"""
        try:
            total_records = 0
            
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
                        
                        logger.info(f"{interval_type} 集約履歴読み込み: {item_count}アイテム")
            
            logger.info(f"個別アイテム集約履歴読み込み完了: {len(self.price_history)}アイテム、{total_records}レコード")
            
        except Exception as e:
            logger.warning(f"集約データ読み込みエラー: {e}")

    def load_total_price_data(self):
        """総価格データを読み込み"""
        try:
            # 30分毎の総価格生データ
            total_raw_file = os.path.join(self.history_dir, "total_price_raw_data.json")
            if os.path.exists(total_raw_file):
                with open(total_raw_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    self.total_price_raw_data = deque(data, maxlen=2880)
                    logger.info(f"総価格30分毎データ読み込み: {len(self.total_price_raw_data)}レコード")
            
            # 集約済み総価格データ
            for interval_type in self.price_intervals:
                total_file = os.path.join(self.history_dir, f"total_price_{interval_type}.json")
                if os.path.exists(total_file):
                    with open(total_file, 'r', encoding='utf-8') as f:
                        data = json.load(f)
                        if interval_type not in self.total_price_history:
                            self.total_price_history[interval_type] = data
                        logger.info(f"総価格{interval_type}データ読み込み完了")
                        
        except Exception as e:
            logger.warning(f"総価格データ読み込みエラー: {e}")

    def save_history_to_files(self):
        """価格履歴を全ファイルに保存（30分毎生データ + 集約データ + 総価格データ）"""
        try:
            # 30分毎生データの保存
            self.save_raw_data()
            
            # 集約データの保存
            self.save_aggregated_data()
            
            # 総価格データの保存
            self.save_total_price_data()
            
        except Exception as e:
            logger.error(f"価格履歴保存エラー: {e}")

    def save_raw_data(self):
        """30分毎の生データを保存"""
        try:
            raw_data_file = os.path.join(self.history_dir, "raw_price_data.json")
            raw_data = {}
            
            for item_id, raw_history in self.raw_price_data.items():
                if len(raw_history) > 0:
                    raw_data[item_id] = list(raw_history)
            
            with open(raw_data_file, 'w', encoding='utf-8') as f:
                json.dump(raw_data, f, ensure_ascii=False, indent=2)
                
            logger.info(f"30分毎生データ保存: {len(raw_data)}アイテム")
        except Exception as e:
            logger.error(f"30分毎生データ保存エラー: {e}")

    def save_aggregated_data(self):
        """集約データを保存"""
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
                
                logger.info(f"{interval_type} 集約履歴保存: {len(interval_data)}アイテム、{total_points}ポイント")
        except Exception as e:
            logger.error(f"集約データ保存エラー: {e}")

    def save_total_price_data(self):
        """総価格データを保存"""
        try:
            # 30分毎の総価格生データ
            total_raw_file = os.path.join(self.history_dir, "total_price_raw_data.json")
            with open(total_raw_file, 'w', encoding='utf-8') as f:
                json.dump(list(self.total_price_raw_data), f, ensure_ascii=False, indent=2)
            
            # 集約済み総価格データ
            for interval_type in self.price_intervals:
                if interval_type in self.total_price_history:
                    total_file = os.path.join(self.history_dir, f"total_price_{interval_type}.json")
                    with open(total_file, 'w', encoding='utf-8') as f:
                        json.dump(self.total_price_history[interval_type], f, ensure_ascii=False, indent=2)
                    
            logger.info("総価格データ保存完了")
        except Exception as e:
            logger.error(f"総価格データ保存エラー: {e}")

    def add_raw_price_data(self, item_id, item_name, current_price):
        """30分毎の生データを追加"""
        timestamp = datetime.now().isoformat()
        price_point = {
            'timestamp': timestamp,
            'price': current_price,
            'item_name': item_name
        }
        
        if item_id not in self.raw_price_data:
            self.raw_price_data[item_id] = deque(maxlen=2880)
        
        self.raw_price_data[item_id].append(price_point)
        logger.debug(f"30分毎データ追加: {item_name} - {current_price:,}")

    def aggregate_price_data_for_interval(self, item_id, interval_type):
        """30分毎データから指定間隔で集約"""
        if item_id not in self.raw_price_data:
            return []
        
        raw_data = list(self.raw_price_data[item_id])
        if not raw_data:
            return []
        
        config = self.price_intervals[interval_type]
        interval_duration = config['interval']
        
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
                        avg_price = statistics.mean([p['price'] for p in current_group])
                        aggregated_data.append({
                            'timestamp': current_group[-1]['timestamp'],  # 最新のタイムスタンプを使用
                            'price': int(avg_price),
                            'item_name': current_group[0]['item_name'],
                            'data_points': len(current_group)
                        })
                    
                    # 新しいグループを開始
                    group_start_time = point_time
                    current_group = [data_point]
                    
            except Exception as e:
                logger.debug(f"データポイント処理エラー: {e}")
                continue
        
        # 最後のグループを処理
        if current_group:
            avg_price = statistics.mean([p['price'] for p in current_group])
            aggregated_data.append({
                'timestamp': current_group[-1]['timestamp'],
                'price': int(avg_price),
                'item_name': current_group[0]['item_name'],
                'data_points': len(current_group)
            })
        
        return aggregated_data

    def update_total_price_data(self, all_current_prices):
        """総価格データを更新（30分毎 + 集約）"""
        timestamp = datetime.now().isoformat()
        
        # 有効な価格のみを計算
        valid_prices = [price for price in all_current_prices.values() if price > 0]
        
        if not valid_prices:
            logger.warning("有効な価格データがありません")
            return
        
        total_price = sum(valid_prices)
        average_price = int(statistics.mean(valid_prices))
        
        # 30分毎の総価格データを追加
        total_point = {
            'timestamp': timestamp,
            'total_price': total_price,
            'average_price': average_price,
            'item_count': len(valid_prices)
        }
        
        self.total_price_raw_data.append(total_point)
        
        # 各間隔での集約済み総価格データを更新
        for interval_type in self.price_intervals:
            self.aggregate_total_price_for_interval(interval_type)
        
        logger.info(f"総価格データ更新: 合計{total_price:,} NESO, 平均{average_price:,} NESO ({len(valid_prices)}アイテム)")

    def aggregate_total_price_for_interval(self, interval_type):
        """総価格データを指定間隔で集約"""
        if not self.total_price_raw_data:
            return
        
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
                        avg_total = int(statistics.mean([p['total_price'] for p in current_group]))
                        avg_average = int(statistics.mean([p['average_price'] for p in current_group]))
                        avg_count = int(statistics.mean([p['item_count'] for p in current_group]))
                        
                        aggregated_data.append({
                            'timestamp': current_group[-1]['timestamp'],
                            'total_price': avg_total,
                            'average_price': avg_average,
                            'item_count': avg_count,
                            'data_points': len(current_group)
                        })
                    
                    # 新しいグループを開始
                    group_start_time = point_time
                    current_group = [data_point]
                    
            except Exception as e:
                logger.debug(f"総価格データポイント処理エラー: {e}")
                continue
        
        # 最後のグループを処理
        if current_group:
            avg_total = int(statistics.mean([p['total_price'] for p in current_group]))
            avg_average = int(statistics.mean([p['average_price'] for p in current_group]))
            avg_count = int(statistics.mean([p['item_count'] for p in current_group]))
            
            aggregated_data.append({
                'timestamp': current_group[-1]['timestamp'],
                'total_price': avg_total,
                'average_price': avg_average,
                'item_count': avg_count,
                'data_points': len(current_group)
            })
        
        # Chart.js用のデータ形式で保存
        chart_data = self.format_total_price_chart_data(aggregated_data, interval_type)
        self.total_price_history[interval_type] = chart_data

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

    def should_update_interval(self, item_id, interval_type, current_price):
        """緩和版: より積極的な更新判定（30分毎データ対応）"""
        
        # 強制検出モードの場合は無条件更新
        if self.force_price_detection:
            self.update_statistics['forced_updates'] += 1
            return True
        
        # 30分毎の生データは常に追加
        return True

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
        """価格履歴を更新（30分毎データ + 集約処理）"""
        # 30分毎の生データを追加
        self.add_raw_price_data(item_id, item_name, current_price)
        
        # 現在価格をキャッシュに保存
        self.current_prices[item_id] = current_price
        
        # 各間隔での集約データを更新
        updated_intervals = []
        for interval_type in self.price_intervals:
            aggregated_data = self.aggregate_price_data_for_interval(item_id, interval_type)
            
            if aggregated_data:
                if item_id not in self.price_history:
                    self.price_history[item_id] = {}
                
                if interval_type not in self.price_history[item_id]:
                    config = self.price_intervals[interval_type]
                    self.price_history[item_id][interval_type] = deque(maxlen=config['maxlen'])
                
                # 最新の集約データのみを追加（重複を避けるため）
                latest_data = aggregated_data[-1]
                
                # 重複チェック
                current_history = list(self.price_history[item_id][interval_type])
                if not current_history or current_history[-1]['timestamp'] != latest_data['timestamp']:
                    self.price_history[item_id][interval_type].append(latest_data)
                    updated_intervals.append(interval_type)
        
        if updated_intervals:
            logger.debug(f"{item_name} 集約履歴更新: {updated_intervals}")
        
        return updated_intervals

    def update_from_current_prices(self):
        """現在の価格JSONから履歴を更新（30分毎データ対応版）"""
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
            all_current_prices = {}
            
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
                        all_current_prices[item_id] = current_price
                        
                        # 30分毎のデータ更新（常に実行）
                        intervals = self.update_price_history(
                            item_id, 
                            item_data['item_name'], 
                            current_price
                        )
                        
                        if intervals or self.force_price_detection:
                            updated_count += 1
                            if self.force_price_detection:
                                force_updated_count += 1
                            
                except (ValueError, TypeError) as e:
                    logger.debug(f"価格変換エラー ({item_id}): {price_str} -> {e}")
                    continue
            
            # 総価格データを更新
            if all_current_prices:
                self.update_total_price_data(all_current_prices)
            
            logger.info(f"30分毎データ処理完了: 処理{processed_count}件、更新{updated_count}件")
            
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
                logger.info(f"✅ 30分毎データ + 集約履歴更新完了: {updated_count}アイテム")
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
        """Chart.js用のデータを生成（30分毎データから集約）"""
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
        # 30分毎生データの統計
        raw_data_count = len(self.raw_price_data)
        total_raw_points = sum(len(data) for data in self.raw_price_data.values())
        
        stats = {
            'total_items': len(self.price_history),
            'raw_data_items': raw_data_count,
            'total_raw_data_points': total_raw_points,
            'total_price_raw_points': len(self.total_price_raw_data),
            'intervals': {},
            'configuration': {
                'force_price_detection': self.force_price_detection,
                'relaxed_mode': self.relaxed_mode,
                'time_threshold_ratio': self.time_threshold_ratio,
                'data_collection_interval': '30分毎'
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
                'average_points_per_item': total_points / max(item_count, 1),
                'aggregation_from': f"30分毎データから{config['aggregation_points']}ポイント平均"
            }
        
        return stats

def main():
    """メイン実行：30分毎データ収集と集約処理"""
    logger.info("=" * 50)
    logger.info("MapleStory価格履歴更新開始（30分毎データ集約版）")
    logger.info("=" * 50)
    
    try:
        # システム初期化
        logger.info("30分毎データ集約システム初期化: data/price_history")
        tracker = HistoricalPriceTracker()
        
        # 現在の価格データから履歴更新
        updated = tracker.update_from_current_prices()
        
        # 統計表示
        stats = tracker.get_statistics()
        logger.info(f"📊 30分毎データ集約統計:")
        logger.info(f"  総アイテム数: {stats['total_items']}")
        logger.info(f"  30分毎生データ: {stats['raw_data_items']}アイテム、{stats['total_raw_data_points']}ポイント")
        logger.info(f"  総価格30分毎データ: {stats['total_price_raw_points']}ポイント")
        logger.info(f"  設定: 強制検出={stats['configuration']['force_price_detection']}, 緩和モード={stats['configuration']['relaxed_mode']}")
        
        for interval, data in stats['intervals'].items():
            logger.info(f"  {interval}: {data['items_with_data']}件 ({data['description']}) - {data['total_data_points']}ポイント")
            logger.info(f"    集約方法: {data['aggregation_from']}")
        
        # 緩和版結果表示
        if tracker.force_price_detection or tracker.relaxed_mode:
            logger.info(f"🔄 30分毎データ集約結果: {updated}アイテム更新")
        
        logger.info("=" * 50)
        logger.info(f"✅ 30分毎データ集約更新完了: {updated}アイテム")
        logger.info("=" * 50)
        
        return updated
        
    except Exception as e:
        logger.error(f"メイン実行エラー: {e}")
        raise

if __name__ == "__main__":
    main()
