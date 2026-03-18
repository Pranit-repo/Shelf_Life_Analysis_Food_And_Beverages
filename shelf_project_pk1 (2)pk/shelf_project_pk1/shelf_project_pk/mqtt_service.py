import json
import threading
import time
import os
import random
import paho.mqtt.client as mqtt
from datetime import datetime

class MQTTService:
    def __init__(self, callback=None):
        self.broker = os.getenv('MQTT_BROKER', 'broker.hivemq.com') # Public broker for testing
        self.port = int(os.getenv('MQTT_PORT', 1883))
        self.topic = os.getenv('MQTT_TOPIC', 'supply_chain/sensors/#')
        self.client = mqtt.Client(client_id=f"sc_backend_{random.randint(1000,9999)}")
        self.callback = callback
        self.running = False

    def on_connect(self, client, userdata, flags, rc):
        """Called when connected to the broker."""
        if rc == 0:
            print(f"[MQTT] Connected to {self.broker}")
            client.subscribe(self.topic)
        else:
            print(f"[MQTT] Connection Failed. Code: {rc}")

    def on_message(self, client, userdata, msg):
        """Called when a message is received."""
        try:
            payload = msg.payload.decode()
            data = json.loads(payload)
            
            # Add timestamp if missing
            if 'timestamp' not in data:
                data['timestamp'] = datetime.now().strftime('%H:%M:%S')
            
            # Extract device ID from topic if not in payload (topic: supply_chain/sensors/DEVICE_01)
            if 'device_id' not in data:
                topic_parts = msg.topic.split('/')
                if len(topic_parts) > 2:
                    data['device_id'] = topic_parts[-1]

            # Trigger the callback (sends to Frontend via SocketIO)
            if self.callback:
                self.callback(data)
                
        except json.JSONDecodeError:
            print(f"[MQTT] Error: Received non-JSON message: {msg.payload}")
        except Exception as e:
            print(f"[MQTT] Processing Error: {e}")

    def start(self):
        """Start the MQTT loop in a background thread."""
        try:
            self.client.on_connect = self.on_connect
            self.client.on_message = self.on_message
            
            print(f"[MQTT] Connecting to {self.broker}:{self.port}...")
            self.client.connect(self.broker, self.port, 60)
            
            # Run loop in a separate thread so it doesn't block Flask
            self.running = True
            mqtt_thread = threading.Thread(target=self.client.loop_start)
            mqtt_thread.daemon = True
            mqtt_thread.start()
            
        except Exception as e:
            print(f"[MQTT] Init Failed: {e}")

    def publish_command(self, device_id, command):
        """Send a command back to a specific device."""
        topic = f"supply_chain/control/{device_id}"
        self.client.publish(topic, json.dumps(command))