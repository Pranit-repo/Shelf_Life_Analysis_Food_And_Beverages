import os
import pandas as pd
import json
import numpy as np
from flask import Flask, render_template, request, jsonify, redirect, url_for, flash
from flask_socketio import SocketIO, emit
from data_manager import DataManager
from anomaly_service import AnomalyService
from map_service import MapService
from forecasting_service import ForecastingService
from procurement import ProcurementService 
from forecasting_service import ForecastingService
from mqtt_service import MQTTService
from tracking_service import TrackingService
from dotenv import load_dotenv
from openai import OpenAI

# Load Environment Variables
load_dotenv()

app = Flask(__name__)
app.secret_key = 'supply_chain_super_secret'
socketio = SocketIO(app, cors_allowed_origins="*")

# --- CONFIGURE KEYS ---
OPENROUTER_API_KEY = os.getenv('OPENROUTER_API_KEY')
TRACCAR_API_KEY = os.getenv('TRACCAR_API_KEY')

client = None
if OPENROUTER_API_KEY:
    try:
        client = OpenAI(
            base_url="https://openrouter.ai/api/v1",
            api_key=OPENROUTER_API_KEY,
        )
    except Exception as e:
        print(f"Error initializing OpenAI client: {e}")

# Initialize Services
data_manager = DataManager()
anomaly_service = AnomalyService()
procurement_service = ProcurementService(data_manager, client)
map_service = MapService()
forecasting_service = ForecastingService(client)
tracking_service = TrackingService()

# Define a callback for MQTT updates
def mqtt_callback(data):
    with app.app_context():
        socketio.emit('sensor_update', data)

# Pass the callback to MQTT Service
mqtt_service = MQTTService(callback=mqtt_callback)

# Start MQTT in background
try:
    mqtt_service.start()
except Exception as e:
    print(f"MQTT Start Warning: {e}")

# --- STARTUP SYNC: TRACCAR ---
try:
    traccar_url = os.getenv('TRACCAR_URL', 'http://localhost:8082')
    t_user = os.getenv('TRACCAR_USER')
    t_pass = os.getenv('TRACCAR_PASS')
    tracking_service.sync_traccar_data(traccar_url, username=t_user, password=t_pass)
except Exception as e:
    pass

DATASET_KEYS = [
    'factory_manufacturing', 'factory_dispatch', 
    'dealer_receipt', 'dealer_dispatch', 
    'retailer_receipt', 'retailer_stock'
]

# --- ROUTES ---

@app.route('/')
def index(): return render_template('upload.html')

@app.route('/preload', methods=['POST'])
def preload_datasets():
    loaded_count = 0
    for key in DATASET_KEYS:
        path = request.form.get(f'{key}_path', '').strip()
        if not path: 
            path = f"D:/datasets/{key}.csv" # Default Fallback
            
        if path and os.path.exists(path):
            if data_manager.register_dataset(key, path): 
                loaded_count += 1
    
    if loaded_count > 0:
        return redirect(url_for('results'))
    
    return redirect(url_for('index'))

@app.route('/results')
def results():
    res = {}
    if hasattr(data_manager, 'dataset_previews'):
        for k, v in data_manager.dataset_previews.items():
            res[k] = {
                'dataset_name': k.replace('_', ' ').title(),
                'analysis': {'basic_stats': {'columns_available': len(v.get('columns', []))}}
            }
    return render_template('results_strict.html', results=res)

@app.route('/map')
def map_view(): return render_template('map.html', dataset_config={k: {'name': k} for k in DATASET_KEYS})

@app.route('/anomalies')
def anomalies_view():
    return render_template('anomalies.html', anomalies=anomaly_service.get_anomalies(), dataset_config={k: {'name': k} for k in DATASET_KEYS})

@app.route('/tracking')
def tracking_view():
    return render_template('tracking.html', traccar_token=TRACCAR_API_KEY or '')

@app.route('/forecasting')
def forecasting_view(): return render_template('forecasting.html')

# Data Access
@app.route('/get_columns/<dataset_key>')
def get_columns(dataset_key): return jsonify({'columns': data_manager.get_columns(dataset_key)})

@app.route('/get_column_values/<dataset_key>/<column_name>')
def get_column_values(dataset_key, column_name): 
    # Increased limit to 5000 to ensure all Batch IDs appear in dropdowns
    return jsonify({'values': data_manager.get_unique_values_paged(dataset_key, column_name, per_page=5000)})

# --- NEW: END-TO-END BATCH FLOW MAP API ---
@app.route('/api/map/batch_flow', methods=['POST'])
def get_batch_flow():
    try:
        req = request.json
        batch_id = req.get('batch_id')
        view_mode = req.get('view_mode', 'all') # 'all', 'inventory', 'transit'
        
        if not batch_id:
            return jsonify({'error': 'Missing batch_id'}), 400

        # Fetch all data related to this batch across all datasets
        batch_data = data_manager.scan_for_entity('Batch_ID', batch_id)
        
        if not batch_data:
             return jsonify({'error': 'Batch ID not found in any dataset'}), 404

        # Generate End-to-End Flow Data with Mode Filtering
        flow_data = map_service.generate_end_to_end_flow(batch_id, batch_data, view_mode)
        
        # Dynamic AI Prompts based on View Mode
        if client and flow_data.get('metrics'):
            m = flow_data['metrics']
            
            prompt_context = ""
            if view_mode == 'inventory':
                prompt_context = """
                Focus on INVENTORY Stages (Factory, Dealer Warehouse):
                1. Inventory Holding Costs & Risks
                2. Spoilage/Shelf-Life Impact (Stock Age)
                3. Warehouse Efficiency & Dispatch Readiness
                """
            elif view_mode == 'transit':
                prompt_context = """
                Focus on TRANSIT Stages (Factory->Dealer, Dealer->Retailer):
                1. Transit Route Efficiency & Delays
                2. Risk of Goods Damage during Transport
                3. Last Mile Optimization
                """
            else:
                prompt_context = """
                Focus on END-TO-END Lifecycle:
                1. Overall Supply Chain Speed
                2. Bottleneck Identification
                3. Cost Optimization (Holding vs Transit)
                """

            prompt = f"""
            Analyze Supply Chain Batch {batch_id}. 
            View Mode: {view_mode.upper()}.
            Dealers Involved: {m['dealers_involved']}.
            
            {prompt_context}
            
            Provide 3 HTML formatted strategic recommendations (<li>...</li>) strictly related to the focus area.
            Keep it professional and concise.
            """
            try:
                completion = client.chat.completions.create(
                    model="google/gemini-2.0-flash-001",
                    messages=[{"role": "user", "content": prompt}]
                )
                flow_data['ai_recommendations'] = completion.choices[0].message.content
            except Exception as e:
                flow_data['ai_recommendations'] = "<li>AI Service currently unavailable.</li>"
        else:
            flow_data['ai_recommendations'] = "<li>Select a valid batch to see AI insights.</li>"

        return jsonify(flow_data)

    except Exception as e:
        print(f"Batch Map Error: {e}")
        return jsonify({'error': str(e)}), 500

# Map API (Legacy/General)
@app.route('/api/map/movements', methods=['POST'])
def get_map_movements():
    try:
        req = request.json
        dataset = req.get('dataset')
        column = req.get('column')
        value = req.get('value')
        
        if not column or not value:
            return jsonify({'error': 'Missing parameters'}), 400
        
        found_data = data_manager.scan_for_entity(column, value)
        if not found_data:
            return jsonify({'error': 'No data found'})
        
        movements, locations, summary = map_service.generate_movements(found_data, start_stage_key=dataset)
        
        ai_recommendation = "AI Analysis Unavailable."
        if client and movements:
            try:
                prompt = f"""
                Analyze supply chain path for {value}. 
                Transit: {summary['total_transit_days']} days. 
                Stages Covered: {summary['stage_count']}.
                
                Provide 3 short, punchy bullet points:
                1. Status (Normal/Delay)
                2. Potential Risk
                3. Efficiency Tip
                Max 50 words.
                """
                completion = client.chat.completions.create(
                    model="google/gemini-2.0-flash-001",
                    messages=[{"role": "user", "content": prompt}]
                )
                ai_recommendation = completion.choices[0].message.content
            except Exception as e:
                ai_recommendation = f"AI Error: {str(e)}"

        return jsonify({
            'movements': movements, 
            'locations': locations,
            'summary': summary,
            'ai_insights': ai_recommendation,
            'count': len(movements)
        })
    except Exception as e:
        print(f"Map Error: {e}")
        return jsonify({'error': str(e)}), 500

# Anomaly API (Original Chain Logic)
@app.route('/api/analyze/chain', methods=['POST'])
def analyze_chain():
    req = request.json
    found_data = data_manager.scan_for_entity(req.get('column'), req.get('value'))
    if not found_data: return jsonify({'error': 'No data found'}), 404

    # Build Chain
    stage_order = [
        'factory_manufacturing', 
        'factory_dispatch', 
        'dealer_receipt', 
        'dealer_dispatch', 
        'retailer_receipt', 
        'retailer_stock'
    ]
    chain = []
    
    for stage in stage_order:
        if stage in found_data:
            df = found_data[stage]
            # Smart Date Finding
            date_col = next((c for c in df.columns if 'date' in c.lower()), None)
            if date_col and not df.empty:
                try:
                    # Handle date parsing
                    val = df.iloc[0][date_col]
                    dt = pd.to_datetime(val, dayfirst=True, errors='coerce')
                    if pd.notna(dt): 
                         chain.append({'stage': stage.replace('_',' ').title(), 'date': dt})
                except: pass

    breakdown = []
    anomalies = []
    
    if len(chain) > 0:
        chain.sort(key=lambda x: x['date'])
        for i in range(len(chain) - 1):
            curr, next_s = chain[i], chain[i+1]
            days = (next_s['date'] - curr['date']).days
            
            types = []
            is_shelf_life = 'Stock' in next_s['stage']
            
            # Logic
            if days > 7 and not is_shelf_life: 
                types.append("Transit-Time Anomaly")
            
            # Special Rule for Shelf Life
            if is_shelf_life and days > 30:
                types.append("Expired/Stagnant Stock")
                
            if days < 0: types.append("Data Consistency Anomaly")
            
            # Rename stage for clarity if it's the final step
            if is_shelf_life:
                stage_name = "Shelf Life (Stock Age)"
            else:
                stage_name = f"{curr['stage']} → {next_s['stage']}"
            
            is_anomaly = len(types) > 0
            
            breakdown.append({'stage': stage_name, 'days': days, 'is_anomaly': is_anomaly, 'anomalies': types})
            
            if is_anomaly:
                anomalies.append({'stage': stage_name, 'types': types, 'days': days})
        
        # Handle single stage case
        if len(chain) == 1:
             breakdown.append({'stage': f"{chain[0]['stage']} (Start)", 'days': 0, 'is_anomaly': False, 'anomalies': []})

    fastest = min(breakdown, key=lambda x: x['days']) if breakdown else {'stage': 'N/A', 'days': 0}
    slowest = max(breakdown, key=lambda x: x['days']) if breakdown else {'stage': 'N/A', 'days': 0}

    # OpenRouter AI for Anomalies
    ai_insights = []
    if client and anomalies:
        try:
            issues_summary = []
            for a in anomalies:
                issues_summary.append(f"{a['stage']}: {', '.join(a['types'])}")
            
            issues_text = "; ".join(issues_summary)
            prompt = f"""
            Act as a Supply Chain Expert. Analyze these anomalies: {issues_text}.
            For EACH item, provide a JSON object with:
            1. "stage": exact stage name from input
            2. "root_cause": why it happened (1 sentence)
            3. "recommendation": how to fix it (1 sentence)
            
            Format strictly as a JSON list: [{{...}}, {{...}}]
            """
            completion = client.chat.completions.create(
                model="google/gemini-2.0-flash-001",
                messages=[{"role": "user", "content": prompt}]
            )
            clean_text = completion.choices[0].message.content.replace('```json', '').replace('```', '').strip()
            ai_insights = json.loads(clean_text)
        except Exception as e:
            ai_insights = [{'stage': 'System', 'root_cause': 'AI Error', 'recommendation': str(e)}]

    return jsonify({
        'total_days': (chain[-1]['date'] - chain[0]['date']).days if len(chain) > 1 else 0,
        'breakdown': breakdown,
        'anomalies': anomalies,
        'ai_insights': ai_insights,
        'fastest': fastest,
        'slowest': slowest
    })

# --- FORECASTING API (Legacy) ---
@app.route('/api/forecast', methods=['POST'])
def api_forecast():
    entity_type = request.form.get('type')
    entity_name = request.form.get('name')
    df = None
    date_col = None
    qty_col = None

    if entity_type == 'dealer':
        data = data_manager.scan_for_entity('DD_Dealer_Name', entity_name)
        if 'dealer_dispatch' in data:
            df = data['dealer_dispatch']
            date_col = next((c for c in df.columns if 'dispatch_date' in c.lower()), 'DD_Dispatch_Date')
            qty_col = next((c for c in df.columns if 'dispatch_quantity' in c.lower()), 'DD_Dispatch_Quantity')
    else:
        # Now that DataManager can link Stock, we try stock first for Retailer
        data = data_manager.scan_for_entity('RS_Retailer_Name', entity_name)
        if 'retailer_stock' in data:
            df = data['retailer_stock']
            date_col = next((c for c in df.columns if 'stock_as_on_date' in c.lower()), 'RS_Stock_As_On_Date')
            qty_col = next((c for c in df.columns if 'received_quantity' in c.lower()), 'RS_Received_Quantity')

    if df is None or df.empty:
        return jsonify({'error': f'No data found for {entity_type}: {entity_name}'})

    result = forecasting_service.forecast_demand(df, date_col, qty_col)
    return jsonify(result)

@app.route('/api/complaints', methods=['POST'])
def api_complaints():
    return forecasting_service.analyze_complaint(request.form.get('text'))

# --- NEW: ADVANCED FORECASTING API (Entity-Based) ---
@app.route('/api/advanced/dealer', methods=['POST'])
def advanced_dealer_forecast():
    try:
        req = request.json
        dealer = req.get('dealer')
        data = data_manager.scan_for_entity('Dealer_Name', dealer)
        df_fac_disp = data.get('factory_dispatch', pd.DataFrame())
        df_deal_rec = data.get('dealer_receipt', pd.DataFrame())
        df_deal_disp = data.get('dealer_dispatch', pd.DataFrame())
        df_ret_rec = data.get('retailer_receipt', pd.DataFrame())
        
        if df_fac_disp.empty: return jsonify({'error': 'No data found for dealer'}), 404
        result = forecasting_service.analyze_dealer_advanced(dealer, df_fac_disp, df_deal_rec, df_deal_disp, df_ret_rec)
        return jsonify(result)
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/advanced/retailer', methods=['POST'])
def advanced_retailer_forecast():
    try:
        req = request.json
        retailer = req.get('retailer')
        data = data_manager.scan_for_entity('Retailer_Name', retailer)
        df_ret_stock = data.get('retailer_stock', pd.DataFrame())
        df_ret_rec = data.get('retailer_receipt', pd.DataFrame())
        
        df_mfg = pd.DataFrame()
        mfg_path = data_manager.file_paths.get('factory_manufacturing')
        if mfg_path and os.path.exists(mfg_path):
             try:
                 df_mfg = pd.read_csv(mfg_path, nrows=5000, on_bad_lines='skip')
             except: pass

        if df_ret_stock.empty and df_ret_rec.empty:
             return jsonify({'error': 'No data found for retailer'}), 404

        result = forecasting_service.analyze_retailer_advanced(retailer, df_ret_stock, df_ret_rec, df_mfg)
        return jsonify(result)
    except Exception as e:
        print(f"Retailer Advanced Error: {e}")
        return jsonify({'error': str(e)}), 500

# --- BATCH ANALYSIS ENDPOINT (Stage-Wise Forecasting) ---
@app.route('/api/batch/analyze', methods=['POST'])
def analyze_batch_flow():
    try:
        req = request.json
        batch_id = req.get('batch_id')
        data = data_manager.scan_for_entity('Batch_ID', batch_id)
        
        df_mfg = data.get('factory_manufacturing', pd.DataFrame())
        df_disp = data.get('factory_dispatch', pd.DataFrame())
        df_receipt = data.get('dealer_receipt', pd.DataFrame())
        df_deal_disp = data.get('dealer_dispatch', pd.DataFrame()) 
        df_ret_receipt = data.get('retailer_receipt', pd.DataFrame())
        df_ret_stock = data.get('retailer_stock', pd.DataFrame()) # Added Stage 6
        
        if df_mfg.empty:
             return jsonify({'error': 'Batch not found in Manufacturing records. Check exact ID match.'}), 404

        result = forecasting_service.analyze_batch_lifecycle(df_mfg, df_disp, df_receipt, df_deal_disp, df_ret_receipt, df_ret_stock, batch_id)
        return jsonify(result)
    except Exception as e:
        print(f"Analysis Error: {e}")
        return jsonify({'error': str(e)}), 500

# --- NEW: CHAT API ---
@app.route('/api/chat', methods=['POST'])
def chat_api():
    req = request.json
    return jsonify({"response": forecasting_service.chat_with_data(req.get('message'), req.get('context'))})

@app.route('/procurement')
def procurement_view():
    datasets = {k: k.replace('_', ' ').title() for k in DATASET_KEYS}
    return render_template('procurement.html', datasets=datasets)



@app.route('/api/procurement/analyze', methods=['POST'])
def analyze_procurement_api():
    try:
        req = request.json
        if 'batch_id' in req:
            result = procurement_service.analyze_procurement_request(None, None, req['batch_id'], 'batch')
        else:
            dataset = req.get('dataset')
            col = req.get('column')
            val = req.get('value')
            mode = 'entity'
            if col and 'batch' in col.lower(): mode = 'batch'
            if str(val).upper().startswith('BF'): mode = 'batch'
            result = procurement_service.analyze_procurement_request(dataset, col, val, mode)
        result['ai_explanation'] = procurement_service.generate_ai_explanation(result)
        return jsonify(result)
    except Exception as e:
        return jsonify({'error': str(e)}), 500

# --- NEW: PROCUREMENT CHAT API (Fixed: Added exactly once) ---
@app.route('/api/procurement/chat', methods=['POST'])
def procurement_chat():
    try:
        data = request.json
        query = data.get('query')
        context = data.get('context')
        response = procurement_service.chat_with_procurement(query, context)
        return jsonify({'response': response})
    except Exception as e:
        print(f"Procurement Chat Error: {e}")
        return jsonify({'error': str(e)}), 500


# --- TRACCAR WEBHOOK ---
@app.route('/api/traccar/webhook', methods=['POST'])
def traccar_hook():
    try:
        d = request.json
        if d:
            dev = d.get('device', {})
            pos = d.get('position', {})
            tracking_service.update_device(dev.get('id'), dev.get('name'), dev.get('status'))
            telemetry = {
                'device_id': dev.get('id'), 'lat': pos.get('latitude'), 'lon': pos.get('longitude'),
                'speed': pos.get('speed'), 'temp': 0, 'humidity': 0, 'battery': pos.get('attributes', {}).get('batteryLevel')
            }
            tracking_service.log_telemetry(telemetry)
        return jsonify({'status': 'ok'})
    except: return jsonify({'error': 'fail'}), 500

if __name__ == '__main__':
    if not os.path.exists('anomalies.db'): anomaly_service.init_db()
    if not os.path.exists('tracking_data.db'): tracking_service.init_db()
    socketio.run(app, debug=True, port=5000, allow_unsafe_werkzeug=True)