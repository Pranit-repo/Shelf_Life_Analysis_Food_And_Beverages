import sqlite3
import json
import uuid
from datetime import datetime

class AnomalyService:
    def __init__(self, db_path='anomalies.db'):
        self.db_path = db_path
        self.init_db()

    def init_db(self):
        with sqlite3.connect(self.db_path) as conn:
            conn.execute('''
                CREATE TABLE IF NOT EXISTS anomalies (
                    anomaly_id TEXT PRIMARY KEY,
                    dataset_name TEXT,
                    anomaly_type TEXT,
                    severity TEXT,
                    explanation TEXT,
                    timestamp TEXT,
                    stage TEXT
                )
            ''')

    def save_anomalies(self, anomaly_list):
        if not anomaly_list: return
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            for a in anomaly_list:
                cursor.execute('''
                    INSERT INTO anomalies (anomaly_id, dataset_name, anomaly_type, severity, explanation, timestamp, stage)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                ''', (str(uuid.uuid4()), "Chain Analysis", "Multiple", "HIGH", json.dumps(a), datetime.now().isoformat(), a.get('stage', 'Unknown')))
            conn.commit()

    def get_anomalies(self, limit=100):
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.row_factory = sqlite3.Row
                cursor = conn.cursor()
                cursor.execute("SELECT * FROM anomalies ORDER BY timestamp DESC LIMIT ?", (limit,))
                return [dict(row) for row in cursor.fetchall()]
        except:
            return []