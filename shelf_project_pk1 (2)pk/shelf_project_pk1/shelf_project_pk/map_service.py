from geopy.geocoders import Nominatim
import json
import os
import re
import time
import pandas as pd

class MapService:
    def __init__(self):
        self.geolocator = Nominatim(user_agent="supply_chain_app_v1")
        self.cache_file = 'geocache.json'
        self.cache = self._load_cache()

    def _load_cache(self):
        if os.path.exists(self.cache_file):
            try:
                with open(self.cache_file, 'r') as f:
                    return json.load(f)
            except:
                return {}
        return {}

    def _save_cache(self):
        with open(self.cache_file, 'w') as f:
            json.dump(self.cache, f)

    def get_coordinates(self, city_name):
        """Get lat/lon for a city, using cache first."""
        if not city_name: return None
        
        clean_city = str(city_name).strip().title()
        key = re.sub(r'[^a-zA-Z]', '', clean_city.lower())
        
        if key in self.cache: return self.cache[key]

        try:
            # Add country context for better accuracy
            location = self.geolocator.geocode(f"{clean_city}, India", timeout=2)
            if location:
                data = {'lat': location.latitude, 'lon': location.longitude}
                self.cache[key] = data
                self._save_cache()
                return data
        except:
            pass
        
        # Default fallback (Center of India)
        return {'lat': 20.5937, 'lon': 78.9629}

    def generate_movements(self, data_manager_results, start_stage_key=None):
        """
        Generates map movements for a SINGLE transit step (Current -> Next).
        Visualizes relationships correctly using specific linking columns (Batch ID + Dealer Name).
        """
        locations = {}
        movements = []
        journey_summary = {
            'total_transit_days': 0,
            'current_location': 'Unknown',
            'start_date': None,
            'end_date': None,
            'stage_count': 0
        }
        
        # 1. Define Logical Supply Chain Sequence
        sequence = [
            'factory_manufacturing', 
            'factory_dispatch', 
            'dealer_receipt', 
            'dealer_dispatch', 
            'retailer_receipt', 
            'retailer_stock'
        ]

        # 2. Identify Target Stages (Current & Next ONLY)
        target_stages = []
        
        if start_stage_key and start_stage_key in sequence:
            idx = sequence.index(start_stage_key)
            target_stages.append(start_stage_key)
            # Add next stage if available
            if idx + 1 < len(sequence):
                target_stages.append(sequence[idx + 1])
        else:
            # Fallback: If no valid start stage, show first step
            target_stages = [sequence[0], sequence[1]]

        # 3. Collect Points Grouped by Stage
        points_by_stage = {key: [] for key in target_stages}
        all_dates = []

        for stage, df in data_manager_results.items():
            if stage not in target_stages: continue

            # Robust Column Finder
            cols = df.columns
            city_col = next((c for c in cols if 'city' in c.lower() or 'location' in c.lower()), None)
            date_col = next((c for c in cols if 'date' in c.lower()), None)
            qty_col = next((c for c in cols if 'quantity' in c.lower() or 'qty' in c.lower()), None)
            
            # Find Dealer Name Column (Critical for Factory Dispatch -> Dealer Receipt matching)
            dealer_col = next((c for c in cols if 'dealer' in c.lower() and 'name' in c.lower()), None)
            
            # Specific Fixes
            if stage == 'dealer_receipt' and not city_col:
                 city_col = next((c for c in cols if 'dr_city' in c.lower()), None)

            if city_col:
                for _, row in df.iterrows():
                    city = row[city_col]
                    date_val = row[date_col] if date_col else 'N/A'
                    qty_val = row[qty_col] if qty_col else 'N/A'
                    dealer_val = row[dealer_col] if dealer_col else None
                    batch_id = row.get('Batch_ID', 'Unknown')
                    
                    coords = self.get_coordinates(city)
                    if coords:
                        point_data = {
                            'stage_key': stage,
                            'stage_label': stage.replace('_', ' ').title(),
                            'city': city,
                            'lat': coords['lat'],
                            'lon': coords['lon'],
                            'raw_date': str(date_val),
                            'parsed_date': pd.to_datetime(date_val, dayfirst=True, errors='coerce'),
                            'quantity': str(qty_val),
                            'batch_id': batch_id,
                            'dealer_name': str(dealer_val).strip() if dealer_val else None,
                            'unique_id': f"{stage}_{city}_{batch_id}_{qty_val}",
                            # Visual Step Number: 1 for Start, 2 for End
                            'step_number': target_stages.index(stage) + 1
                        }
                        
                        locations[point_data['unique_id']] = point_data
                        points_by_stage[stage].append(point_data)
                        
                        if pd.notna(point_data['parsed_date']):
                            all_dates.append(point_data['parsed_date'])

        # 4. Generate Movements (Connect Group A to Group B)
        # We only connect target_stages[0] -> target_stages[1]
        if len(target_stages) == 2:
            start_stage = target_stages[0]
            end_stage = target_stages[1]
            
            sources = points_by_stage[start_stage]
            destinations = points_by_stage[end_stage]
            
            if sources and destinations:
                for source in sources:
                    for dest in destinations:
                        # Verify Batch ID Match (Basic requirement)
                        if str(source['batch_id']).strip() != str(dest['batch_id']).strip():
                            continue

                        # --- SPECIFIC MATCHING LOGIC ---
                        
                        # Logic 1: Factory Dispatch -> Dealer Receipt
                        # Must match on Dealer Name to avoid connecting to wrong dealers
                        if start_stage == 'factory_dispatch' and end_stage == 'dealer_receipt':
                            s_dealer = source.get('dealer_name')
                            d_dealer = dest.get('dealer_name')
                            
                            # If we have dealer names, enforce match
                            if s_dealer and d_dealer and s_dealer != d_dealer:
                                continue
                        
                        # Logic 2: Factory Mfg -> Factory Dispatch
                        # One factory connects to all dispatches for this batch (1-to-N)
                        # No extra filtering needed beyond Batch ID
                        
                        # Logic 3: Dealer Receipt -> Dealer Dispatch
                        # Usually 1-to-1 or 1-to-many within same dealer. 
                        # Dealer Name match is good here too.
                        elif 'dealer' in start_stage and 'dealer' in end_stage:
                             s_dealer = source.get('dealer_name')
                             d_dealer = dest.get('dealer_name')
                             if s_dealer and d_dealer and s_dealer != d_dealer:
                                continue

                        add_movement(movements, source, dest)

        # 5. Summary Metrics
        journey_summary['stage_count'] = len(locations)
        if all_dates:
            all_dates.sort()
            journey_summary['start_date'] = all_dates[0].strftime('%Y-%m-%d')
            journey_summary['end_date'] = all_dates[-1].strftime('%Y-%m-%d')
            journey_summary['total_transit_days'] = (all_dates[-1] - all_dates[0]).days
        
        # Set current location text
        if len(target_stages) > 1 and points_by_stage[target_stages[1]]:
             last_pts = points_by_stage[target_stages[1]]
             journey_summary['current_location'] = f"{len(last_pts)} Locations ({target_stages[1].replace('_',' ').title()})"
        elif points_by_stage[target_stages[0]]:
             first_pts = points_by_stage[target_stages[0]]
             journey_summary['current_location'] = f"{first_pts[0]['city']} ({target_stages[0].replace('_',' ').title()})"

        return movements, locations, journey_summary

def add_movement(movements_list, start, end):
    """Helper to calculate transit and add movement dict"""
    transit = 0
    if pd.notna(start['parsed_date']) and pd.notna(end['parsed_date']):
        transit = (end['parsed_date'] - start['parsed_date']).days

    movements_list.append({
        'from_city': start['city'], 
        'to_city': end['city'],
        'from_lat': start['lat'], 
        'from_lon': start['lon'],
        'to_lat': end['lat'], 
        'to_lon': end['lon'],
        'from_stage': start['stage_label'], 
        'to_stage': end['stage_label'],
        'from_stage_key': start['stage_key'], # For coloring
        'transit_days': transit
    })