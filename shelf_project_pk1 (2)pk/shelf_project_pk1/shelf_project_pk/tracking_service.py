import sqlite3
import json
import requests
import time
from datetime import datetime, timedelta

class TrackingService:
    def __init__(self, db_path='tracking_data.db'):
        self.db_path = db_path
        self.init_db()

    def init_db(self):
        with sqlite3.connect(self.db_path, check_same_thread=False) as conn:
            cursor = conn.cursor()
            # Table for Device Info
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS devices (
                    device_id TEXT PRIMARY KEY,
                    name TEXT,
                    status TEXT,
                    last_update TEXT
                )
            ''')
            # Table for Telemetry (Merged Traccar + MQTT data)
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS telemetry (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    device_id TEXT,
                    timestamp TEXT,
                    latitude REAL,
                    longitude REAL,
                    speed REAL,
                    temperature REAL,
                    humidity REAL,
                    battery_level REAL,
                    FOREIGN KEY(device_id) REFERENCES devices(device_id)
                )
            ''')
            conn.commit()

    def update_device(self, device_id, name, status):
        try:
            with sqlite3.connect(self.db_path, check_same_thread=False) as conn:
                conn.execute('''
                    INSERT OR REPLACE INTO devices (device_id, name, status, last_update)
                    VALUES (?, ?, ?, ?)
                ''', (str(device_id), name, status, datetime.now().isoformat()))
                conn.commit()
        except Exception as e:
            print(f"[DB Error] Update Device: {e}")

    def log_telemetry(self, data):
        """
        Logs telemetry data. 'data' should be a dict with keys:
        device_id, lat, lon, speed, temp, humidity, battery
        """
        try:
            with sqlite3.connect(self.db_path, check_same_thread=False) as conn:
                conn.execute('''
                    INSERT INTO telemetry (device_id, timestamp, latitude, longitude, speed, temperature, humidity, battery_level)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    str(data.get('device_id')),
                    datetime.now().isoformat(),
                    data.get('lat', 0.0),
                    data.get('lon', 0.0),
                    data.get('speed', 0.0),
                    data.get('temp', 0.0),
                    data.get('humidity', 0.0),
                    data.get('battery', 0.0)
                ))
                conn.commit()
        except Exception as e:
            print(f"[DB Error] Log Telemetry: {e}")

    def get_latest_telemetry(self, device_id):
        try:
            with sqlite3.connect(self.db_path, check_same_thread=False) as conn:
                conn.row_factory = sqlite3.Row
                cursor = conn.execute('''
                    SELECT * FROM telemetry WHERE device_id = ? ORDER BY timestamp DESC LIMIT 1
                ''', (str(device_id),))
                row = cursor.fetchone()
                return dict(row) if row else {}
        except: return {}

    def get_all_devices(self):
        try:
            with sqlite3.connect(self.db_path, check_same_thread=False) as conn:
                conn.row_factory = sqlite3.Row
                cursor = conn.execute('SELECT * FROM devices')
                return [dict(row) for row in cursor.fetchall()]
        except: return []

    def sync_traccar_data(self, traccar_url, token=None, username=None, password=None):
        """
        Fetches all devices and their latest positions from Traccar and updates the local DB.
        """
        print(f"[Traccar Sync] Connecting to {traccar_url}...")
        
        auth = None
        if username and password:
            auth = (username, password)
        
        headers = {}
        # If token is provided instead of Basic Auth (depends on Traccar config)
        # Note: Traccar usually uses Basic Auth for API or a session cookie.
        # We'll try Basic Auth if provided, or rely on public access if configured.

        try:
            # 1. Fetch Devices
            devices_url = f"{traccar_url}/api/devices"
            resp = requests.get(devices_url, auth=auth, headers=headers, timeout=10)
            
            if resp.status_code == 200:
                devices = resp.json()
                print(f"[Traccar Sync] Found {len(devices)} devices.")
                
                for dev in devices:
                    d_id = str(dev.get('id'))
                    d_name = dev.get('name', 'Unknown')
                    d_status = dev.get('status', 'offline')
                    self.update_device(d_id, d_name, d_status)
            else:
                print(f"[Traccar Sync] Failed to fetch devices. Status: {resp.status_code}")
                return

            # 2. Fetch Latest Positions
            positions_url = f"{traccar_url}/api/positions"
            resp = requests.get(positions_url, auth=auth, headers=headers, timeout=10)
            
            if resp.status_code == 200:
                positions = resp.json()
                print(f"[Traccar Sync] Found {len(positions)} positions.")
                
                for pos in positions:
                    d_id = str(pos.get('deviceId'))
                    telemetry = {
                        'device_id': d_id,
                        'lat': pos.get('latitude'),
                        'lon': pos.get('longitude'),
                        'speed': pos.get('speed'),
                        'temp': pos.get('attributes', {}).get('temp', 0), # Try to get temp from attributes
                        'humidity': pos.get('attributes', {}).get('humidity', 0),
                        'battery': pos.get('attributes', {}).get('batteryLevel', 0)
                    }
                    self.log_telemetry(telemetry)
            else:
                 print(f"[Traccar Sync] Failed to fetch positions. Status: {resp.status_code}")

        except Exception as e:
            print(f"[Traccar Sync] Error: {e}")