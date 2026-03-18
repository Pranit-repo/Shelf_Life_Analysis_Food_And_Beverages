import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import json
import math

class ProcurementService:
    def __init__(self, data_manager, client=None):
        self.dm = data_manager
        self.client = client

    def _get_df(self, key):
        if hasattr(self.dm, 'datasets') and key in self.dm.datasets:
            return self.dm.datasets[key]
        path = self.dm.file_paths.get(key)
        if path:
            try: return pd.read_csv(path, on_bad_lines='skip')
            except: return pd.DataFrame()
        return pd.DataFrame()

    def _safe_float(self, val):
        try: return float(val)
        except: return 0.0

    def analyze_procurement_request(self, dataset, filter_col, filter_val, mode='entity'):
        sim_params = None
        if isinstance(filter_val, str) and '||' in filter_val:
            parts = filter_val.split('||')
            filter_val = parts[0]
            try:
                sim_params = json.loads(parts[1])
            except:
                pass

        if str(filter_val).upper().startswith('BF') or (filter_col and 'batch' in filter_col.lower()):
            mode = 'batch'
        return self._analyze_batch_sequential_steps(filter_val, sim_params)

    def _analyze_batch_sequential_steps(self, batch_id, sim_params=None):
        # --- 0. DATA LOADING ---
        keys = [
            'factory_manufacturing', 'factory_dispatch', 
            'dealer_receipt', 'dealer_dispatch', 
            'retailer_receipt', 'retailer_stock'
        ]
        
        data = {}
        for k in keys:
            df = self._get_df(k)
            if not df.empty:
                b_col = next((c for c in df.columns if 'batch' in c.lower() and 'id' in c.lower()), None)
                if b_col:
                    data[k] = df[df[b_col].astype(str).str.strip() == str(batch_id).strip()].copy()
                else:
                    data[k] = pd.DataFrame()
            else:
                data[k] = pd.DataFrame()

        # Helper: Extract Key Events
        def get_key_events(df, date_col_hint, qty_col_hint, entity_col_hint):
            events = []
            if df.empty: return ["No data recorded."]
            d_col = next((c for c in df.columns if date_col_hint in c.lower()), None)
            q_col = next((c for c in df.columns if qty_col_hint in c.lower()), None)
            e_col = next((c for c in df.columns if entity_col_hint in c.lower()), None)
            if d_col:
                try: df.sort_values(d_col, inplace=True)
                except: pass
            for _, row in df.head(3).iterrows():
                date_str = str(row[d_col]) if d_col else "Unknown Date"
                qty_str = str(row[q_col]) if q_col else "Unknown Qty"
                ent_str = str(row[e_col]) if e_col else ""
                desc = f"{qty_str} units"
                if ent_str: desc += f" ({ent_str})"
                if d_col: desc += f" on {date_str}"
                events.append(desc)
            return events

        # --- STEP 1-3: BASIC METRICS ---
        mfg = data['factory_manufacturing']
        mfg_qty = mfg['FM_Manufacturing_Quantity'].apply(self._safe_float).sum() if not mfg.empty else 0
        prod_dates = pd.to_datetime(mfg['FM_Manufacturing_Date'], dayfirst=True, errors='coerce').dropna() if not mfg.empty and 'FM_Manufacturing_Date' in mfg.columns else pd.Series()
        prod_date_str = prod_dates.min().strftime('%d-%b-%Y') if not prod_dates.empty else "Unknown"
        # Assuming mfg date is the start
        mfg_date_val = prod_dates.min() if not prod_dates.empty else datetime.now()

        product_name = mfg['FM_Product_Name'].iloc[0] if not mfg.empty else "Unknown"
        category = mfg['FM_Product_Category'].iloc[0] if not mfg.empty else "Unknown"
        
        disp = data['factory_dispatch']
        disp_qty = disp['FD_Dispatch_Quantity'].apply(self._safe_float).sum() if not disp.empty else 0
        factory_remaining = max(0, mfg_qty - disp_qty)
        
        rec = data['dealer_receipt']
        rec_dates = pd.to_datetime(rec['DR_Receipt_Date'], dayfirst=True, errors='coerce').dropna() if not rec.empty and 'DR_Receipt_Date' in rec.columns else pd.Series()
        disp_dates = pd.to_datetime(disp['FD_Factory_Dispatch_Date'], dayfirst=True, errors='coerce').dropna() if not disp.empty and 'FD_Factory_Dispatch_Date' in disp.columns else pd.Series()
        
        avg_transit_days = 0
        if not rec_dates.empty and not disp_dates.empty:
            avg_transit_days = max(0, (rec_dates.mean() - disp_dates.mean()).days)

        # --- STEP 4: DEALER INVENTORY ---
        d_disp = data['dealer_dispatch']
        d_disp_dates = pd.to_datetime(d_disp['DD_Dispatch_Date'], dayfirst=True, errors='coerce').dropna() if not d_disp.empty and 'DD_Dispatch_Date' in d_disp.columns else pd.Series()
        
        avg_holding_days = 0
        if not d_disp_dates.empty and not rec_dates.empty:
            avg_holding_days = max(0, (d_disp_dates.mean() - rec_dates.mean()).days)
            
        slow_dealers = []
        if not d_disp.empty and 'DD_Dealer_Name' in d_disp.columns:
            vol = d_disp.groupby('DD_Dealer_Name')['DD_Dispatch_Quantity'].apply(lambda x: x.apply(self._safe_float).sum())
            if not vol.empty:
                mean_vol = vol.mean()
                slow_dealers = vol[vol < mean_vol * 0.5].index.tolist()

        # --- STEP 6: RETAILER DEMAND ---
        ret_rec = data['retailer_receipt']
        ret_stock = data['retailer_stock']
        total_ret_received = ret_rec['RR_Received_Quantity'].apply(self._safe_float).sum() if not ret_rec.empty else 0
        current_ret_stock = ret_stock['RS_Stock'].apply(self._safe_float).sum() if not ret_stock.empty else 0
        sales_15_days = max(0, total_ret_received - current_ret_stock)
        avg_daily_demand = sales_15_days / 15 if sales_15_days > 0 else 0
        next_15_demand = avg_daily_demand * 15 * 1.1

        # --- SHELF-LIFE CONSUMPTION METER (NEW) ---
        SHELF_LIFE_DAYS = 270 # Assumption for FMCG
        
        # Calculate Average Dates for Stages
        dates_summary = {
            'mfg': mfg_date_val,
            'f_disp': disp_dates.mean() if not disp_dates.empty else mfg_date_val,
            'd_rec': rec_dates.mean() if not rec_dates.empty else mfg_date_val,
            'd_disp': d_disp_dates.mean() if not d_disp_dates.empty else mfg_date_val
        }
        
        # Retailer Receipt Dates
        ret_rec_dates = pd.to_datetime(ret_rec['RR_Receipt_Date'], dayfirst=True, errors='coerce').dropna() if not ret_rec.empty and 'RR_Receipt_Date' in ret_rec.columns else pd.Series()
        dates_summary['r_rec'] = ret_rec_dates.mean() if not ret_rec_dates.empty else mfg_date_val

        # Calculate % Consumed at each stage
        consumption_meter = []
        stages_order = [('Factory', 'mfg'), ('Dispatch', 'f_disp'), ('Dealer Rec.', 'd_rec'), ('Dealer Push', 'd_disp'), ('Retail Shelf', 'r_rec')]
        
        for label, key in stages_order:
            dt = dates_summary[key]
            days_passed = max(0, (dt - mfg_date_val).days)
            pct_consumed = min(100, (days_passed / SHELF_LIFE_DAYS) * 100)
            consumption_meter.append({
                'stage': label,
                'days_elapsed': int(days_passed),
                'pct_consumed': float(f"{pct_consumed:.1f}")
            })

        # Calculate Remaining Sellable Days per City (Freshness at Arrival)
        city_freshness = []
        if not ret_rec.empty and 'RR_City' in ret_rec.columns and 'RR_Receipt_Date' in ret_rec.columns:
            # Clone and ensure date
            rr_fresh = ret_rec[['RR_City', 'RR_Receipt_Date']].copy()
            rr_fresh['date_obj'] = pd.to_datetime(rr_fresh['RR_Receipt_Date'], dayfirst=True, errors='coerce')
            rr_fresh = rr_fresh.dropna(subset=['date_obj'])
            
            # Calculate days elapsed since mfg for each receipt
            rr_fresh['age_at_arrival'] = (rr_fresh['date_obj'] - mfg_date_val).dt.days
            rr_fresh['remaining_days'] = SHELF_LIFE_DAYS - rr_fresh['age_at_arrival']
            
            # Group by City
            fresh_grp = rr_fresh.groupby('RR_City')['remaining_days'].mean().reset_index()
            fresh_grp = fresh_grp.sort_values('remaining_days', ascending=True) # Lowest remaining first (Higher risk)
            
            for _, row in fresh_grp.iterrows():
                rem_days = int(row['remaining_days'])
                status = "Critical" if rem_days < 30 else ("Warning" if rem_days < 90 else "Fresh")
                city_freshness.append({
                    'city': row['RR_City'],
                    'avg_remaining_days': rem_days,
                    'status': status
                })

        # --- STEP 7: TIMING & ALLOCATION LOCK ---
        total_lead_time = 0
        if not ret_rec_dates.empty and not prod_dates.empty:
            total_lead_time = (ret_rec_dates.max() - prod_dates.min()).days
        if total_lead_time == 0: total_lead_time = avg_transit_days + avg_holding_days + 5

        days_inventory_remaining = (current_ret_stock / avg_daily_demand) if avg_daily_demand > 0 else 0
        timing_gap = days_inventory_remaining - total_lead_time
        timing_risk = "HIGH" if timing_gap < 0 else ("MEDIUM" if timing_gap < 5 else "LOW")
        
        is_locked = False
        lock_date_str = "N/A"
        if not prod_dates.empty:
            start_prod = prod_dates.min()
            lock_date = start_prod + timedelta(days=int(total_lead_time * 0.2))
            lock_date_str = lock_date.strftime('%d-%b-%Y')
            if datetime.now() > lock_date:
                is_locked = True

        # --- STEP 8: RECOMMENDATION BASE ---
        procurement_needed = timing_risk in ["HIGH", "MEDIUM"]
        raw_rec_qty = max(0, int(next_15_demand - current_ret_stock + (next_15_demand * 0.2)))
        
        start_days_delta = max(0, int(days_inventory_remaining - total_lead_time))
        production_date_obj = datetime.now() + timedelta(days=start_days_delta)
        production_date_str = production_date_obj.strftime('%d-%b-%Y')
        if start_days_delta == 0: production_date_str += " (Immediate)"

        # === 1. DEALER DISTRIBUTION PLAN ===
        distribution_breakdown = []
        if raw_rec_qty > 0 and not d_disp.empty:
            required_cols = ['DD_Dealer_Name', 'DD_Retailer_Name', 'DD_Dispatch_Quantity', 'DD_City']
            available_cols = [c for c in required_cols if c in d_disp.columns]
            
            if len(available_cols) == 4:
                d_disp['qty_clean'] = d_disp['DD_Dispatch_Quantity'].apply(self._safe_float)
                grp = d_disp.groupby(['DD_City', 'DD_Dealer_Name', 'DD_Retailer_Name'])['qty_clean'].sum().reset_index()
                total_hist_vol = grp['qty_clean'].sum()
                
                if total_hist_vol > 0:
                    grp['share'] = grp['qty_clean'] / total_hist_vol
                    grp['allocated'] = (grp['share'] * raw_rec_qty)
                    grp['allocated_int'] = grp['allocated'].round(0).astype(int)
                    
                    grp = grp.sort_values('allocated_int', ascending=False)
                    for _, row in grp.iterrows():
                        if row['allocated_int'] > 0:
                            distribution_breakdown.append({
                                'city': row['DD_City'],
                                'dealer': row['DD_Dealer_Name'],
                                'retailer': row['DD_Retailer_Name'],
                                'share_pct': float(f"{row['share']*100:.2f}"),
                                'allocated_qty': int(row['allocated_int'])
                            })

        # === 2. CITY-WISE STRATEGIC ALLOCATION ===
        city_allocations = []
        city_stats = {}
        total_score_weight = 0
        
        if not ret_rec.empty and 'RR_City' in ret_rec.columns:
            rec_grp = ret_rec.groupby('RR_City')['RR_Received_Quantity'].apply(lambda x: x.apply(self._safe_float).sum())
            stock_grp = pd.Series(dtype=float)
            if not ret_stock.empty and 'RS_City' in ret_stock.columns:
                 stock_grp = ret_stock.groupby('RS_City')['RS_Stock'].apply(lambda x: x.apply(self._safe_float).sum())

            city_holding = {}
            if not d_disp.empty and 'DD_City' in d_disp.columns:
                for city, grp in d_disp.groupby('DD_City'):
                     ddates = pd.to_datetime(grp['DD_Dispatch_Date'], dayfirst=True, errors='coerce').dropna()
                     if not ddates.empty:
                         if not rec.empty and 'DR_City' in rec.columns:
                             rec_in_city = rec[rec['DR_City'] == city]
                             rdates = pd.to_datetime(rec_in_city['DR_Receipt_Date'], dayfirst=True, errors='coerce').dropna()
                             if not rdates.empty:
                                 hold_days = (ddates.mean() - rdates.mean()).days
                                 city_holding[city] = max(0, hold_days)

            all_cities = set(rec_grp.index)
            for city in all_cities:
                received = rec_grp.get(city, 0)
                curr_stock = stock_grp.get(city, 0)
                holding_days = city_holding.get(city, avg_holding_days)
                
                sales = max(0, received - curr_stock)
                velocity = sales / 15.0 
                sell_through = (sales / received) if received > 0 else 0
                
                if sell_through >= 0.75:
                    band = "High Velocity"
                    weight = 1.5 
                    timing_rule = "Immediate Start"
                    insight = "Fast mover; prioritize dispatch."
                elif sell_through <= 0.35:
                    band = "High Shelf-Life Risk"
                    weight = 0.4
                    timing_rule = "Delay / Safe Date Only"
                    insight = f"Stagnant; Holding avg {int(holding_days)} days."
                else:
                    band = "Medium Velocity"
                    weight = 1.0
                    timing_rule = "Standard Window"
                    insight = "Steady flow; maintain levels."
                
                score = velocity * weight
                total_score_weight += score
                
                city_stats[city] = {
                    'velocity': velocity,
                    'sell_through': sell_through,
                    'band': band,
                    'timing': timing_rule,
                    'insight': insight,
                    'score': score,
                    'holding': holding_days
                }

        if raw_rec_qty > 0 and total_score_weight > 0:
            for city, stats in city_stats.items():
                share_pct = stats['score'] / total_score_weight
                alloc_qty = int(raw_rec_qty * share_pct)
                city_allocations.append({
                    'city': city,
                    'velocity': float(f"{stats['velocity']:.1f}"),
                    'band': stats['band'],
                    'timing_rule': stats['timing'],
                    'allocation_qty': alloc_qty,
                    'share_pct': float(f"{share_pct*100:.1f}"),
                    'insight': stats['insight'],
                    'holding_days': int(stats['holding'])
                })
        
        city_allocations.sort(key=lambda x: x['allocation_qty'], reverse=True)

        # === 3. RETAILER-WISE INTELLIGENCE ===
        retailer_allocations = []
        retailer_stats = {}
        total_ret_score_weight = 0

        if not ret_rec.empty and 'RR_Retailer_Name' in ret_rec.columns:
            r_rec_grp = ret_rec.groupby('RR_Retailer_Name')['RR_Received_Quantity'].apply(lambda x: x.apply(self._safe_float).sum())
            r_stock_grp = pd.Series(dtype=float)
            if not ret_stock.empty and 'RS_Retailer_Name' in ret_stock.columns:
                 r_stock_grp = ret_stock.groupby('RS_Retailer_Name')['RS_Stock'].apply(lambda x: x.apply(self._safe_float).sum())

            all_retailers = set(r_rec_grp.index) | set(r_stock_grp.index)
            
            for ret in all_retailers:
                received = r_rec_grp.get(ret, 0)
                curr_stock = r_stock_grp.get(ret, 0)
                
                sales = max(0, received - curr_stock)
                velocity = sales / 15.0 
                sell_through = (sales / received) if received > 0 else 0
                
                if sell_through >= 0.70:
                    band = "High Perf / Low Risk"
                    weight = 1.5 
                elif sell_through <= 0.30:
                    band = "Low Perf / High Risk"
                    weight = 0.3
                else:
                    band = "Stable"
                    weight = 1.0

                score = velocity * weight
                total_ret_score_weight += score
                
                retailer_stats[ret] = {
                    'velocity': velocity,
                    'sell_through': sell_through,
                    'band': band,
                    'score': score
                }

        if raw_rec_qty > 0 and total_ret_score_weight > 0:
            for ret, stats in retailer_stats.items():
                share_pct = stats['score'] / total_ret_score_weight
                alloc_qty = int(raw_rec_qty * share_pct)
                
                if alloc_qty > 0:
                    retailer_allocations.append({
                        'retailer': ret,
                        'velocity': float(f"{stats['velocity']:.1f}"),
                        'band': stats['band'],
                        'allocation_qty': alloc_qty,
                        'share_pct': float(f"{share_pct*100:.1f}")
                    })
        
        retailer_allocations.sort(key=lambda x: x['allocation_qty'], reverse=True)

        # --- SIMULATION & CONSTRAINTS ---
        simulation_data = None
        MOQ = 500
        MAX_CAPACITY = 20000 
        
        if sim_params:
            try:
                demand_change_pct = float(sim_params.get('demand_change', 0))
                lead_time_change = int(sim_params.get('lead_time_change', 0))
                remove_slow = str(sim_params.get('remove_slow_dealers', 'false')).lower() == 'true'
                use_constraints = str(sim_params.get('use_constraints', 'false')).lower() == 'true'

                sim_avg_holding = avg_holding_days
                if remove_slow and len(slow_dealers) > 0:
                     sim_avg_holding = avg_holding_days * 0.85
                
                sim_next_15_demand = next_15_demand * (1 + demand_change_pct / 100)
                sim_avg_daily = avg_daily_demand * (1 + demand_change_pct / 100)
                
                sim_lead_time = (total_lead_time - avg_holding_days + sim_avg_holding) + lead_time_change
                if sim_lead_time < 1: sim_lead_time = 1
                
                sim_days_inv_remaining = (current_ret_stock / sim_avg_daily) if sim_avg_daily > 0 else 0
                sim_timing_gap = sim_days_inv_remaining - sim_lead_time
                sim_timing_risk = "HIGH" if sim_timing_gap < 0 else ("MEDIUM" if sim_timing_gap < 5 else "LOW")
                
                sim_rec_qty = max(0, int(sim_next_15_demand - current_ret_stock + (sim_next_15_demand * 0.2)))
                
                constraint_msg = "None"
                if use_constraints:
                    if sim_rec_qty > 0 and sim_rec_qty < MOQ:
                        sim_rec_qty = MOQ
                        constraint_msg = f"Rounded to MOQ ({MOQ})"
                    elif sim_rec_qty > MOQ:
                        sim_rec_qty = math.ceil(sim_rec_qty / 100) * 100
                    
                    if sim_rec_qty > MAX_CAPACITY:
                        sim_rec_qty = MAX_CAPACITY
                        constraint_msg = f"Capped at Capacity ({MAX_CAPACITY})"

                sim_start_delta = max(0, int(sim_days_inv_remaining - sim_lead_time))
                sim_prod_date_obj = datetime.now() + timedelta(days=sim_start_delta)
                sim_prod_date = sim_prod_date_obj.strftime('%d-%b-%Y')
                if sim_start_delta == 0: sim_prod_date += " (Immediate)"
                
                simulation_data = {
                    "inputs": sim_params,
                    "sim_next_15_demand": float(f"{sim_next_15_demand:.2f}"),
                    "sim_lead_time": int(sim_lead_time),
                    "sim_rec_qty": sim_rec_qty,
                    "sim_start_date": sim_prod_date,
                    "sim_risk": sim_timing_risk,
                    "sim_gap": float(f"{sim_timing_gap:.1f}"),
                    "sim_holding_improvement": float(f"{avg_holding_days - sim_avg_holding:.1f}"),
                    "constraint_msg": constraint_msg
                }
            except Exception as e: print(f"Simulation Error: {e}")

        # --- AI SYNTHESIS ---
        synthetic_qa = [
            {"q": "How is allocation split?", "a": "Based on sales velocity per city. High-velocity cities get 1.5x weighting."},
            {"q": "Any city restricted?", "a": f"{', '.join([c['city'] for c in city_allocations if 'Low' in c['band']][:2]) or 'None'} due to shelf-life risk."},
            {"q": "Is current production sufficient?", "a": f"{'No' if procurement_needed else 'Yes'}, based on {int(days_inventory_remaining)} days cover."},
            {"q": f"Why is the recommended quantity {raw_rec_qty}?", "a": f"To cover 15-day projected demand of {int(next_15_demand)} units."},
            {"q": "Which date should production start?", "a": f"{production_date_str} (in {start_days_delta} days)."},
        ]

        ai_output = {}
        if self.client and not simulation_data:
            try:
                context = {
                    "mfg": {"qty": mfg_qty, "date": prod_date_str},
                    "allocation_plan": [{"city": c['city'], "qty": c['allocation_qty'], "risk": c['band']} for c in city_allocations[:3]],
                    "calc": {"recommend_qty": raw_rec_qty, "start_date": production_date_str}
                }
                
                prompt = f"""
                Act as a Supply Chain Director. Analyze Batch {batch_id}. Context: {json.dumps(context)}
                Return JSON with:
                1. "ai_reasoning": Strategic reason for allocation split.
                2. "dealer_strategy": Strategy for dealers in high-risk cities.
                3. "qa_list": List of 6 objects (q, a) focusing on city performance.
                """
                res = self.client.chat.completions.create(model="google/gemini-2.0-flash-001", messages=[{"role": "user", "content": prompt}])
                ai_output = json.loads(res.choices[0].message.content.replace('```json','').replace('```','').strip())
            except Exception:
                ai_output = {"ai_reasoning": f"Allocation weighted by velocity.", "dealer_strategy": "Push stock to high velocity zones.", "qa_list": synthetic_qa}
        else:
             ai_output = {"ai_reasoning": f"System Calculation: Demand exceeds Stock.", "dealer_strategy": "Optimize allocation.", "qa_list": synthetic_qa}

        # --- FINAL OUTPUT ---
        raw_data_export = {}
        for k, v in data.items():
            df_export = v.copy()
            for col in df_export.columns:
                if 'date' in col.lower(): df_export[col] = df_export[col].astype(str)
            raw_data_export[k] = df_export.to_dict(orient='records')

        return {
            "batch_summary": {
                "batch_id": batch_id, "product_name": product_name, "category": category, 
                "manufactured_quantity": mfg_qty, "production_date": prod_date_str
            },
            "stage_insights": {
                "factory_dispatch": { "total_dispatched": disp_qty, "factory_remaining_stock": factory_remaining, "key_events": get_key_events(disp, "date", "quantity", "dealer") },
                "transit_analysis": { "avg_factory_to_dealer_transit_days": avg_transit_days, "transit_risk_comment": "High Risk" if avg_transit_days > 7 else "Normal", "key_events": get_key_events(rec, "date", "received", "dealer") },
                "dealer_inventory": { "avg_inventory_days": avg_holding_days, "slow_dealers": slow_dealers, "key_events": [] },
                "retailer_demand": { "last_15_day_sales": sales_15_days, "avg_daily_demand": float(f"{avg_daily_demand:.2f}"), "next_15_day_demand_projection": float(f"{next_15_demand:.2f}"), "key_events": get_key_events(ret_rec, "date", "quantity", "retailer") },
                "timing_alignment": { "total_lead_time_days": total_lead_time, "timing_gap_days": float(f"{timing_gap:.1f}"), "timing_risk": timing_risk },
                "allocation_status": { "is_locked": is_locked, "lock_date": lock_date_str, "lead_time": total_lead_time },
                "shelf_life_meter": { "consumption": consumption_meter, "city_freshness": city_freshness } # NEW FIELD
            },
            "final_procurement_recommendation": {
                "procurement_required": procurement_needed, "recommended_production_quantity": raw_rec_qty,
                "production_start_date": production_date_str, "dealer_strategy": ai_output.get('dealer_strategy', 'Review allocation.'),
                "ai_reasoning": ai_output.get('ai_reasoning', 'Based on calculated inventory cover.'), 
                "qa_list": ai_output.get('qa_list', synthetic_qa),
                "city_allocation_breakdown": city_allocations,
                "distribution_breakdown": distribution_breakdown,
                "retailer_allocations": retailer_allocations
            },
            "simulation_results": simulation_data,
            "stage_data": raw_data_export
        }

    def generate_ai_explanation(self, res):
        if 'final_procurement_recommendation' in res: return res['final_procurement_recommendation']['ai_reasoning']
        return "Analysis Complete"

    # --- NEW: NATURAL LANGUAGE CHAT ---
    def chat_with_procurement(self, query, full_analysis_data):
        if not self.client:
            return "⚠️ AI Configuration Error: API Key is missing. Please configure OPENROUTER_API_KEY in the .env file."
        
        try:
            # Create a concise context summary to focus the LLM
            context_summary = {
                "batch_id": full_analysis_data.get("batch_summary", {}).get("batch_id"),
                "product": full_analysis_data.get("batch_summary", {}).get("product_name"),
                "recommended_qty": full_analysis_data.get("final_procurement_recommendation", {}).get("recommended_production_quantity"),
                "reasoning": full_analysis_data.get("final_procurement_recommendation", {}).get("ai_reasoning"),
                "top_allocations": full_analysis_data.get("final_procurement_recommendation", {}).get("city_allocation_breakdown", [])[:5],
                "retailer_risks": [r for r in full_analysis_data.get("final_procurement_recommendation", {}).get("retailer_allocations", []) if "Risk" in r.get("band", "")][:5],
                "shelf_life": full_analysis_data.get("stage_insights", {}).get("shelf_life_meter", {}),
                "timing": full_analysis_data.get("stage_insights", {}).get("timing_alignment", {})
            }
            
            prompt = f"""
            You are a Supply Chain Procurement Assistant.
            Context: {json.dumps(context_summary, indent=2)}
            
            User Query: "{query}"
            
            Answer strictly based on the context. Be executive, concise, and data-driven.
            If asking about 'what-if' scenarios (e.g., delay), estimate impact based on 'timing' data.
            """
            
            res = self.client.chat.completions.create(
                model="google/gemini-2.0-flash-001",
                messages=[{"role": "user", "content": prompt}]
            )
            return res.choices[0].message.content
        except Exception as e:
            if "401" in str(e):
                return " AI Connection Error: Unauthorized (401). Please check your OpenRouter API Key."
            return f"Error generating response: {str(e)}"