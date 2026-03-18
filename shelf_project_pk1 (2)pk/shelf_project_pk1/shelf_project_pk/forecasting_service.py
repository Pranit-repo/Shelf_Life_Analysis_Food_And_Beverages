import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import os
from openai import OpenAI
import json
import warnings

# Try importing sklearn, fallback if missing
try:
    from sklearn.linear_model import LinearRegression
    HAS_SKLEARN = True
except ImportError:
    HAS_SKLEARN = False

# Try importing statsmodels for advanced forecasting
try:
    from statsmodels.tsa.holtwinters import ExponentialSmoothing
    from statsmodels.tools.sm_exceptions import ConvergenceWarning
    HAS_STATSMODELS = True
except ImportError:
    HAS_STATSMODELS = False

class ForecastingService:
    def __init__(self, client=None):
        self.client = client  # OpenRouter/OpenAI Client
        
        # Web-Based Knowledge Simulation for Shelf Life (Days)
        self.SHELF_LIFE_DB = {
            'biscuit': 180, 'chocolate': 270, 'soap': 730, 'toothpaste': 730,
            'oil': 365, 'milk': 2, 'cream': 14, 'shampoo': 1095, 'detergent': 1095
        }

    def _get_product_shelf_life(self, product_name, category):
        search_term = (str(product_name) + " " + str(category)).lower()
        best_match = None
        max_len = 0
        for key, days in self.SHELF_LIFE_DB.items():
            if key in search_term and len(key) > max_len:
                best_match = days
                max_len = len(key)
        return best_match if best_match else 180

    def _safe_date(self, df, col_name):
        if col_name in df.columns:
            return pd.to_datetime(df[col_name], dayfirst=True, errors='coerce')
        return None

    def _generate_forecast(self, df, date_col, qty_col, periods=3, freq='M'):
        if df.empty or date_col not in df.columns or qty_col not in df.columns:
            return [{"period": "Error", "qty": 0}]

        df = df.copy()
        df['date_norm'] = self._safe_date(df, date_col)
        df = df.dropna(subset=['date_norm', qty_col])
        if df.empty: return [{"period": "Insufficient Data", "qty": 0}]

        df[qty_col] = pd.to_numeric(df[qty_col], errors='coerce').fillna(0)

        # Resample based on frequency
        if freq == 'M': ts = df.set_index('date_norm').resample('M')[qty_col].sum().reset_index()
        elif freq == 'W': ts = df.set_index('date_norm').resample('W')[qty_col].sum().reset_index()
        else: ts = df.set_index('date_norm').resample('D')[qty_col].sum().reset_index()

        if len(ts) < 2:
            avg = ts[qty_col].mean() if not ts.empty else 0
            return [{"period": "Next Period", "qty": int(avg)}]

        # Advanced Forecasting with Holt-Winters
        if HAS_STATSMODELS and len(ts) >= 4:
            try:
                with warnings.catch_warnings():
                    warnings.simplefilter("ignore", category=ConvergenceWarning)
                    warnings.simplefilter("ignore", category=UserWarning) 
                    
                    model = ExponentialSmoothing(
                        ts[qty_col], 
                        seasonal_periods=min(len(ts)//2, 12), 
                        trend='add', 
                        seasonal='add', 
                        damped_trend=True
                    ).fit()
                    
                    forecast_values = model.forecast(periods)
                    predictions = []
                    last_date = ts['date_norm'].max()
                    for i, val in enumerate(forecast_values):
                        if freq == 'M': next_date = last_date + pd.DateOffset(months=i+1)
                        elif freq == 'W': next_date = last_date + pd.DateOffset(weeks=i+1)
                        else: next_date = last_date + pd.DateOffset(days=i+1)
                        predictions.append({ "period": next_date.strftime("%Y-%m-%d"), "qty": max(0, int(val)) })
                    return predictions
            except Exception as e:
                pass 

        # Linear Regression Fallback
        ts['idx'] = np.arange(len(ts))
        X = ts[['idx']].values
        y = ts[qty_col].values
        predictions = []
        if HAS_SKLEARN:
            try:
                model = LinearRegression()
                model.fit(X, y)
                last_idx = ts['idx'].max()
                last_date = ts['date_norm'].max()
                for i in range(1, periods + 1):
                    pred_y = model.predict([[last_idx + i]])[0]
                    if freq == 'M': next_date = last_date + pd.DateOffset(months=i)
                    elif freq == 'W': next_date = last_date + pd.DateOffset(weeks=i)
                    else: next_date = last_date + pd.DateOffset(days=i)
                    predictions.append({ "period": next_date.strftime("%Y-%m-%d"), "qty": max(0, int(pred_y)) })
            except:
                avg = int(y.mean())
                predictions = [{"period": "Estimated Next", "qty": avg}]
        else:
            avg = int(y.mean())
            predictions = [{"period": "Estimated Next", "qty": avg}]
        return predictions

    def _generate_product_insights(self, forecast_data, total_qty):
        """Generates 3 data-driven bullet points based on forecast"""
        if not forecast_data:
            return ["Insufficient data for predictions.", "Monitor manual stock levels.", "Check data connectivity."]
        
        # 1. Peak Analysis
        peak = max(forecast_data, key=lambda x: x['qty'])
        peak_note = f"Expected peak demand of {peak['qty']} units on week of {peak['period']}."
        
        # 2. Trend Analysis
        first = forecast_data[0]['qty']
        last = forecast_data[-1]['qty']
        trend = "Stable"
        if last > first * 1.1: trend = "Increasing"
        elif last < first * 0.9: trend = "Decreasing"
        trend_note = f"{trend} demand trend observed over next 3 months."
        
        # 3. Actionable Item
        action = "Maintain standard inventory levels."
        if trend == "Increasing": action = "Increase safety stock by 15% immediately."
        elif trend == "Decreasing": action = "Reduce replenishment frequency to avoid overstock."
        
        return [peak_note, trend_note, action]

    # =========================================================
    # ADVANCED DEALER FORECASTING (Entity-Based)
    # =========================================================
    def analyze_dealer_advanced(self, dealer_name, df_fac_disp, df_deal_rec, df_deal_disp, df_ret_rec):
        result = { "dealer": dealer_name, "raw_dispatch_rows": [], "forecast": {}, "city_performance": [], "ai_insights": [] }

        fd_dealer_col = next((c for c in df_fac_disp.columns if 'dealer' in c.lower()), None)
        if fd_dealer_col:
            dealer_fd = df_fac_disp[df_fac_disp[fd_dealer_col].astype(str).str.strip() == str(dealer_name).strip()].copy()
            cols = df_fac_disp.columns
            date_col = next((c for c in cols if 'date' in c.lower()), cols[0])
            qty_col = next((c for c in cols if 'qty' in c.lower() or 'quantity' in c.lower()), cols[1])
            cat_col = next((c for c in cols if 'category' in c.lower()), None)
            
            for _, row in dealer_fd.head(50).iterrows():
                result['raw_dispatch_rows'].append({
                    "date": str(row[date_col]), "qty": int(row[qty_col]) if str(row[qty_col]).isdigit() else 0,
                    "category": str(row[cat_col]) if cat_col else "N/A"
                })
                
            forecast_qty = self._generate_forecast(dealer_fd, date_col, qty_col, periods=1, freq='M')
            result['forecast']['predicted_qty'] = forecast_qty[0]['qty'] if forecast_qty else 0
            
            dealer_fd['date_norm'] = self._safe_date(dealer_fd, date_col)
            dealer_fd = dealer_fd.sort_values('date_norm')
            dealer_fd['diff'] = dealer_fd['date_norm'].diff().dt.days
            avg_cycle = dealer_fd['diff'].mean()
            last_date = dealer_fd['date_norm'].max()
            
            if not pd.isna(avg_cycle) and not pd.isna(last_date):
                next_date = last_date + timedelta(days=avg_cycle)
                result['forecast']['expected_date'] = next_date.strftime('%Y-%m-%d')
            else: result['forecast']['expected_date'] = "Insufficient History"

        dd_dealer_col = next((c for c in df_deal_disp.columns if 'dealer' in c.lower()), None)
        if dd_dealer_col:
            dealer_dd = df_deal_disp[df_deal_disp[dd_dealer_col].astype(str).str.strip() == str(dealer_name).strip()]
            if not dealer_dd.empty:
                loc_col = next((c for c in df_deal_disp.columns if 'location' in c.lower() or 'city' in c.lower()), None)
                qty_col_dd = next((c for c in df_deal_disp.columns if 'qty' in c.lower() or 'quantity' in c.lower()), None)
                if loc_col and qty_col_dd:
                    city_stats = dealer_dd.groupby(loc_col)[qty_col_dd].sum().reset_index().sort_values(qty_col_dd, ascending=False)
                    for _, row in city_stats.iterrows():
                        result['city_performance'].append({
                            "city": row[loc_col], "total_sales": int(row[qty_col_dd]),
                            "performance": "High Demand" if int(row[qty_col_dd]) > 500 else "Standard"
                        })

        if self.client:
            prompt = f"""Dealer Advanced Forecasting: {dealer_name}. Historical Orders: {len(result['raw_dispatch_rows'])}. Predicted Demand: {result['forecast'].get('predicted_qty', 0)}. Provide 3 insights (Accuracy, Behavior, Transit-Sales). Return JSON list of {{title, text}}."""
            ai_resp = self._get_ai_insight(prompt)
            if isinstance(ai_resp, dict): result['ai_insights'] = [{"title": k, "text": v} for k,v in ai_resp.items()]
            elif isinstance(ai_resp, list): result['ai_insights'] = ai_resp
        return result

    # =========================================================
    # ADVANCED RETAILER FORECASTING (Entity-Based)
    # =========================================================
    def analyze_retailer_advanced(self, retailer_name, df_ret_stock, df_ret_rec=None, df_mfg=None):
        result = { 
            "retailer": retailer_name, 
            "stock_history": [], 
            "forecast": [], # Stock forecast
            "demand_forecast": [], # Overall Sales forecast
            "top_products": [], # New: Detailed High Demand objects
            "low_products": [], # New: Detailed Low Demand objects
            "product_forecasts": {}, 
            "ai_qa": {},
            "market_summary": "Analyzing market data..."
        }
        
        # --- 1. Stock & Overall Demand ---
        ret_col = next((c for c in df_ret_stock.columns if 'retailer' in c.lower()), None)
        if ret_col:
            ret_data = df_ret_stock[df_ret_stock[ret_col].astype(str).str.strip() == str(retailer_name).strip()].copy()
            if not ret_data.empty:
                date_col = next((c for c in ret_data.columns if 'date' in c.lower()), None)
                stock_col = next((c for c in ret_data.columns if 'stock' in c.lower()), None) 
                rcv_col = next((c for c in ret_data.columns if 'received' in c.lower()), None)

                if date_col and stock_col:
                    ret_data['date_norm'] = self._safe_date(ret_data, date_col)
                    ret_data = ret_data.sort_values('date_norm')
                    ret_data[stock_col] = pd.to_numeric(ret_data[stock_col], errors='coerce').fillna(0)
                    
                    if rcv_col:
                        ret_data[rcv_col] = pd.to_numeric(ret_data[rcv_col], errors='coerce').fillna(0)
                        ret_data['prev_stock'] = ret_data[stock_col].shift(1).fillna(ret_data[stock_col])
                        ret_data['calc_sales'] = ret_data['prev_stock'] + ret_data[rcv_col] - ret_data[stock_col]
                        ret_data['calc_sales'] = ret_data['calc_sales'].apply(lambda x: max(0, x))
                    else:
                        ret_data['calc_sales'] = ret_data[stock_col].diff().apply(lambda x: max(0, -x) if pd.notnull(x) else 0)

                    for _, row in ret_data.tail(7).iterrows():
                        result['stock_history'].append({ "date": row[date_col], "stock": int(row[stock_col]), "estimated_sales": int(row['calc_sales']) })
                    result['forecast'] = self._generate_forecast(ret_data, date_col, stock_col, periods=3, freq='D')
                    result['demand_forecast'] = self._generate_forecast(ret_data, date_col, 'calc_sales', periods=3, freq='D')

        # --- 2. Product-Wise Demand (Next 3 Months) ---
        if df_ret_rec is not None and not df_ret_rec.empty and df_mfg is not None and not df_mfg.empty:
            rec_batch_col = next((c for c in df_ret_rec.columns if 'batch' in c.lower()), 'Batch_ID')
            rec_qty_col = next((c for c in df_ret_rec.columns if 'received' in c.lower() or 'qty' in c.lower()), None)
            rec_date_col = next((c for c in df_ret_rec.columns if 'date' in c.lower()), None)
            rec_ret_col = next((c for c in df_ret_rec.columns if 'retailer' in c.lower()), None)
            
            mfg_batch_col = next((c for c in df_mfg.columns if 'batch' in c.lower()), 'Batch_ID')
            # Intelligent Product Name Column Detection
            mfg_cols = df_mfg.columns
            mfg_prod_col = next((c for c in mfg_cols if 'product' in c.lower() and 'name' in c.lower()), None)
            if not mfg_prod_col:
                for c in mfg_cols:
                    lower_c = c.lower()
                    if df_mfg[c].dtype == object and 'date' not in lower_c and 'id' not in lower_c and 'cat' not in lower_c and 'loc' not in lower_c:
                        mfg_prod_col = c
                        break
            
            # Filter receipts for this retailer
            ret_receipts = df_ret_rec[df_ret_rec[rec_ret_col].astype(str).str.strip() == str(retailer_name).strip()].copy()
            
            if not ret_receipts.empty and rec_qty_col and rec_date_col and mfg_prod_col:
                 # FIX: Convert date column to datetime objects BEFORE iterating or checking
                 ret_receipts[rec_date_col] = self._safe_date(ret_receipts, rec_date_col)

                 ret_receipts['b_id_clean'] = ret_receipts[rec_batch_col].astype(str).str.strip()
                 df_mfg['b_id_clean'] = df_mfg[mfg_batch_col].astype(str).str.strip()
                 
                 merged = pd.merge(ret_receipts, df_mfg[['b_id_clean', mfg_prod_col]], on='b_id_clean', how='inner')
                 
                 if not merged.empty:
                     merged[rec_qty_col] = pd.to_numeric(merged[rec_qty_col], errors='coerce').fillna(0)
                     
                     prod_totals = merged.groupby(mfg_prod_col)[rec_qty_col].sum().sort_values(ascending=False)
                     
                     def build_detail(prod_name):
                         p_data = merged[merged[mfg_prod_col] == prod_name].copy().sort_values(rec_date_col)
                         
                         # History Logic
                         history = []
                         for _, r in p_data.iterrows():
                             date_val = r[rec_date_col]
                             date_str = "N/A"
                             if pd.notnull(date_val):
                                 if hasattr(date_val, 'strftime'):
                                     date_str = date_val.strftime('%Y-%m-%d')
                                 else:
                                     date_str = str(date_val)
                             history.append({ "date": date_str, "qty": int(r[rec_qty_col]), "batch": r['b_id_clean'] })
                         
                         # FORECAST: Next 3 Months (12 Weeks)
                         forecast = self._generate_forecast(p_data, rec_date_col, rec_qty_col, periods=12, freq='W')
                         total_forecast_qty = sum(item['qty'] for item in forecast)
                         
                         # BULLET POINTS
                         insights = self._generate_product_insights(forecast, total_forecast_qty)

                         return {
                             "product": prod_name,
                             "total_qty": int(prod_totals[prod_name]), # Historical
                             "total_forecast_qty": total_forecast_qty, # Future
                             "history": history,
                             "forecast": forecast,
                             "insights": insights
                         }

                     result['top_products'] = [build_detail(p) for p in prod_totals.head(5).index]
                     result['low_products'] = [build_detail(p) for p in prod_totals.tail(5).index]

                     if not prod_totals.empty:
                        result['market_summary'] = f"Top mover '{prod_totals.index[0]}' constitutes {int(prod_totals.iloc[0]/prod_totals.sum()*100)}% of inflow."

        if self.client:
            avg_sales = np.mean([x['estimated_sales'] for x in result['stock_history']]) if result['stock_history'] else 0
            prompt = f"""Retailer: {retailer_name}. Avg Daily Sales: {avg_sales:.1f}. Provide 5 Q&A pairs (Sales Trend, Stockout Risk, Demand Spike, Reorder Timing, Seasonality). Return JSON {{q1:..., a1:..., ...}}"""
            result['ai_qa'] = self._get_ai_insight(prompt)
        return result

    # =========================================================
    # 6-STAGE BATCH LIFECYCLE (MFG -> ... -> RETAILER STOCK -> AI)
    # =========================================================
    def analyze_batch_lifecycle(self, df_mfg, df_disp, df_receipt, df_deal_disp, df_ret_receipt, df_ret_stock, batch_id):
        result = {
            "batch_id": batch_id,
            "mfg_details": {}, "dispatch_list": [], "receipt_list": [], "dealer_dispatch_list": [],
            "retailer_receipt_list": [], "dealer_profiles": {}, "transit_metrics": {},
            "ai_qa_transit": {}, "ai_qa_storage": {}, "ai_qa_retail_transit": {} 
        }

        # 1. MFG
        mfg_batch_col = df_mfg.columns[0]
        mfg_row = df_mfg[df_mfg[mfg_batch_col].astype(str).str.strip() == str(batch_id).strip()]
        mfg_date = None
        if not mfg_row.empty:
            mfg_date_col = next((c for c in df_mfg.columns if 'date' in c.lower()), None)
            prod_name_col = next((c for c in df_mfg.columns if 'name' in c.lower() or 'product' in c.lower()), None)
            qty_col = next((c for c in df_mfg.columns if 'quantity' in c.lower()), None)
            cat_col = next((c for c in df_mfg.columns if 'category' in c.lower()), None)
            loc_col = next((c for c in df_mfg.columns if 'location' in c.lower()), None)
            mfg_date = self._safe_date(mfg_row, mfg_date_col).iloc[0] if mfg_date_col else None
            result['mfg_details'] = {
                "product": mfg_row.iloc[0][prod_name_col] if prod_name_col else "Unknown",
                "category": mfg_row.iloc[0][cat_col] if cat_col else "Unknown",
                "date": mfg_date.strftime('%Y-%m-%d') if mfg_date else "N/A",
                "location": mfg_row.iloc[0][loc_col] if loc_col else "Unknown",
                "produced_qty": int(mfg_row.iloc[0][qty_col]) if qty_col else 0
            }

        # 2. DISPATCH
        disp_batch_col = df_disp.columns[0]
        disp_rows = df_disp[df_disp[disp_batch_col].astype(str).str.strip() == str(batch_id).strip()]
        dealer_dispatch_map, dealer_qty_map, dealer_source_map = {}, {}, {}

        if not disp_rows.empty:
            dealer_col = next((c for c in df_disp.columns if 'dealer' in c.lower()), None)
            disp_date_col = next((c for c in df_disp.columns if 'date' in c.lower()), None)
            disp_qty_col = next((c for c in df_disp.columns if 'quantity' in c.lower()), None)
            loc_col = next((c for c in df_disp.columns if 'city' in c.lower() or 'location' in c.lower()), None)

            for _, row in disp_rows.iterrows():
                d_date = self._safe_date(pd.DataFrame([row]), disp_date_col).iloc[0] if disp_date_col else None
                dealer_name = row[dealer_col] if dealer_col else "Unknown"
                qty = int(row[disp_qty_col]) if disp_qty_col else 0
                location = row[loc_col] if loc_col else "Unknown"
                if dealer_name != "Unknown":
                    dealer_dispatch_map[dealer_name] = d_date
                    dealer_qty_map[dealer_name] = qty
                    dealer_source_map[dealer_name] = location
                days_held = (d_date - mfg_date).days if (d_date and mfg_date) else 0
                result['dispatch_list'].append({
                    "dealer": dealer_name, "location": location, "qty": qty,
                    "date": d_date.strftime('%Y-%m-%d') if d_date else "N/A", "days_held": max(0, days_held)
                })

        # 3. DEALER RECEIPT
        rec_batch_col = df_receipt.columns[0]
        rec_rows = df_receipt[df_receipt[rec_batch_col].astype(str).str.strip() == str(batch_id).strip()]
        transit_times = []
        dealer_receipt_map = {}

        if not rec_rows.empty:
            rec_dealer_col = next((c for c in df_receipt.columns if 'dealer' in c.lower()), None)
            rec_date_col = next((c for c in df_receipt.columns if 'date' in c.lower()), None)
            rec_qty_col = next((c for c in df_receipt.columns if 'quantity' in c.lower()), None)
            rec_loc_col = next((c for c in df_receipt.columns if 'city' in c.lower() or 'location' in c.lower()), None)

            for _, row in rec_rows.iterrows():
                r_dealer = row[rec_dealer_col] if rec_dealer_col else "Unknown"
                r_date = self._safe_date(pd.DataFrame([row]), rec_date_col).iloc[0] if rec_date_col else None
                if r_dealer != "Unknown" and r_date: dealer_receipt_map[r_dealer] = r_date
                transit_days, status = "N/A", "Unknown"
                if r_dealer in dealer_dispatch_map:
                    dispatch_date = dealer_dispatch_map[r_dealer]
                    if r_date and dispatch_date:
                        diff = (r_date - dispatch_date).days
                        transit_days = max(0, diff)
                        transit_times.append(transit_days)
                        status = "Delayed" if transit_days > 7 else ("Fast" if transit_days <= 3 else "Normal")
                result['receipt_list'].append({
                    "dealer": r_dealer, "location": row[rec_loc_col] if rec_loc_col else "Unknown",
                    "qty": int(row[rec_qty_col]) if rec_qty_col else 0, "date": r_date.strftime('%Y-%m-%d') if r_date else "N/A",
                    "transit_days": transit_days, "status": status
                })

        # 4. DEALER DISPATCH
        dd_batch_col = df_deal_disp.columns[0]
        dd_rows = df_deal_disp[df_deal_disp[dd_batch_col].astype(str).str.strip() == str(batch_id).strip()]
        retailer_dispatch_map, dealer_outbound_map, dealer_retailer_sales = {}, {}, {}

        if not dd_rows.empty:
            dd_dealer_col = next((c for c in df_deal_disp.columns if 'dealer' in c.lower()), None)
            dd_retailer_col = next((c for c in df_deal_disp.columns if 'retailer' in c.lower()), None)
            dd_date_col = next((c for c in df_deal_disp.columns if 'date' in c.lower()), None)
            dd_qty_col = next((c for c in df_deal_disp.columns if 'quantity' in c.lower()), None)
            dd_loc_col = next((c for c in df_deal_disp.columns if 'city' in c.lower() or 'location' in c.lower()), None)

            for _, row in dd_rows.iterrows():
                dd_date = self._safe_date(pd.DataFrame([row]), dd_date_col).iloc[0] if dd_date_col else None
                dealer_name = row[dd_dealer_col] if dd_dealer_col else "Unknown"
                retailer_name = row[dd_retailer_col] if dd_retailer_col else "Unknown"
                qty = int(row[dd_qty_col]) if dd_qty_col else 0
                if retailer_name != "Unknown": retailer_dispatch_map[retailer_name] = dd_date
                if dealer_name != "Unknown":
                    if dd_date:
                        if dealer_name not in dealer_outbound_map or dd_date > dealer_outbound_map[dealer_name]:
                            dealer_outbound_map[dealer_name] = dd_date
                    if dealer_name not in dealer_retailer_sales: dealer_retailer_sales[dealer_name] = []
                    dealer_retailer_sales[dealer_name].append({ "retailer": retailer_name, "qty": qty, "date": dd_date })
                result['dealer_dispatch_list'].append({
                    "dealer": dealer_name, "retailer": retailer_name, "location": row[dd_loc_col] if dd_loc_col else "Unknown",
                    "qty": qty, "date": dd_date.strftime('%Y-%m-%d') if dd_date else "N/A"
                })

        # 5. RETAILER RECEIPT
        rr_batch_col = df_ret_receipt.columns[0]
        rr_rows = df_ret_receipt[df_ret_receipt[rr_batch_col].astype(str).str.strip() == str(batch_id).strip()]
        retailer_transit_times = []
        retailer_receipt_details = {}

        if not rr_rows.empty:
            rr_retailer_col = next((c for c in df_ret_receipt.columns if 'retailer' in c.lower()), None)
            rr_date_col = next((c for c in df_ret_receipt.columns if 'date' in c.lower()), None)
            rr_qty_col = next((c for c in df_ret_receipt.columns if 'quantity' in c.lower()), None)
            rr_loc_col = next((c for c in df_ret_receipt.columns if 'city' in c.lower() or 'location' in c.lower()), None)

            for _, row in rr_rows.iterrows():
                rr_retailer = row[rr_retailer_col] if rr_retailer_col else "Unknown"
                rr_date = self._safe_date(pd.DataFrame([row]), rr_date_col).iloc[0] if rr_date_col else None
                city = row[rr_loc_col] if rr_loc_col else "Unknown"
                transit_days, status = 0, "Unknown"
                if rr_retailer in retailer_dispatch_map:
                    dispatch_date = retailer_dispatch_map[rr_retailer]
                    if rr_date and dispatch_date:
                        diff = (rr_date - dispatch_date).days
                        transit_days = max(0, diff)
                        retailer_transit_times.append(transit_days)
                        status = "Delayed" if transit_days > 5 else ("Fast" if transit_days <= 2 else "Normal")
                retailer_receipt_details[rr_retailer] = { "transit_days": transit_days, "city": city, "date": rr_date }
                result['retailer_receipt_list'].append({
                    "retailer": rr_retailer, "location": city, "qty": int(row[rr_qty_col]) if rr_qty_col else 0,
                    "date": rr_date.strftime('%Y-%m-%d') if rr_date else "N/A", "transit_days": transit_days, "status": status
                })

        # 6. DEALER PROFILE (SHELF LIFE FOCUSED)
        prod_name = result['mfg_details'].get('product', '')
        prod_cat = result['mfg_details'].get('category', '')
        real_total_shelf_life = self._get_product_shelf_life(prod_name, prod_cat)

        def get_season(date_obj):
            if not date_obj: return "Standard"
            m = date_obj.month
            if m in [10, 11]: return "Festive"
            if m in [12, 1]: return "Winter"
            if m in [4, 5]: return "Summer"
            return "Standard"

        for dealer, qty in dealer_qty_map.items():
            if dealer not in dealer_outbound_map: continue 
            take_date = dealer_dispatch_map.get(dealer)
            out_date = dealer_outbound_map.get(dealer)
            source_loc = dealer_source_map.get(dealer, "Factory")
            
            city_sales_analysis = []
            sales_data = dealer_retailer_sales.get(dealer, [])
            if sales_data:
                city_groups = {}
                for sale in sales_data:
                    ret = sale['retailer']
                    if ret in retailer_receipt_details:
                        details = retailer_receipt_details[ret]
                        city, transit = details['city'], details['transit_days']
                        if city not in city_groups: city_groups[city] = {'qty': 0, 'transit_sum': 0, 'count': 0}
                        city_groups[city]['qty'] += sale['qty']
                        city_groups[city]['transit_sum'] += transit
                        city_groups[city]['count'] += 1
                for city, data in city_groups.items():
                    avg_t = data['transit_sum'] / data['count'] if data['count'] else 0
                    insight = "Neutral"
                    if avg_t <= 2: insight = "Low Transit → High Sales (Optimal)" if data['qty'] > 100 else "Low Transit → Low Sales (Demand Issue)"
                    else: insight = "High Transit → High Sales (Strong Demand)" if data['qty'] > 100 else "High Transit → Low Sales (Logistics Issue)"
                    city_sales_analysis.append({ "city": city, "sales": data['qty'], "avg_transit": round(avg_t, 1), "insight": insight })

            last_retail_date = mfg_date 
            if sales_data:
                for sale in sales_data:
                    ret = sale['retailer']
                    if ret in retailer_receipt_details:
                        r_date = retailer_receipt_details[ret]['date']
                        if r_date and (not last_retail_date or r_date > last_retail_date): last_retail_date = r_date
            
            days_elapsed = (last_retail_date - mfg_date).days if (last_retail_date and mfg_date) else 0
            remaining_days = real_total_shelf_life - days_elapsed
            shelf_status = "Fresh"
            if remaining_days < 0: shelf_status = "EXPIRED"
            elif remaining_days < (real_total_shelf_life * 0.2): shelf_status = "Critical"
            elif remaining_days < (real_total_shelf_life * 0.5): shelf_status = "Moderate"

            dlr_rec_date = dealer_receipt_map.get(dealer)
            transit_f_to_d = (dlr_rec_date - take_date).days if (dlr_rec_date and take_date) else 0
            dealer_holding_days = (out_date - dlr_rec_date).days if (out_date and dlr_rec_date) else 0
            last_mile_days = round(np.mean([c['avg_transit'] for c in city_sales_analysis]), 1) if city_sales_analysis else 0

            result['dealer_profiles'][dealer] = {
                "source_location": source_loc, "taken_qty": qty,
                "taken_date": take_date.strftime('%Y-%m-%d') if take_date else "N/A",
                "season_context": get_season(take_date), "city_performance": city_sales_analysis,
                "days_elapsed": days_elapsed, "remaining_shelf_life": max(0, remaining_days),
                "total_shelf_life": real_total_shelf_life, "shelf_status": shelf_status,
                "transit_days_used": { "factory_to_dealer": max(0, transit_f_to_d), "dealer_holding": max(0, dealer_holding_days), "last_mile_avg": last_mile_days }
            }

        # AI Q&A
        avg_transit = round(np.mean(transit_times), 1) if transit_times else 0
        avg_retail_transit = round(np.mean(retailer_transit_times), 1) if retailer_transit_times else 0
        result['transit_metrics'] = {"avg_transit_days": avg_transit, "avg_retail_transit_days": avg_retail_transit}

        if self.client:
            prompt = f"""
            Supply Chain Analysis. Batch: {batch_id}.
            Product: {result['mfg_details'].get('product')} ({result['mfg_details'].get('category')}).
            Factory->Dealer Transit: {avg_transit} days.
            Dealer->Retailer Transit: {avg_retail_transit} days.
            
            Provide THREE JSON Objects.
            OBJECT 1 (Factory->Dealer Transit): 5 Q&A pairs using keys q1, a1, q2, a2... up to q5, a5. (Topics: Temp, Vehicle, Spoilage, Shelf Life Impact, Critical Action).
            OBJECT 2 (Dealer Storage): 5 Q&A pairs using keys q1, a1, q2, a2... up to q5, a5. (Topics: Duration, Conditions, Wastage, FIFO, Spoilage Signs).
            OBJECT 3 (Dealer->Retailer Transit): 5 Q&A pairs using keys q1, a1, q2, a2... up to q5, a5. (Topics: Quality, Last Mile, Vehicle, Risk, Shelf Life).
            
            Return JSON: {{ "transit": {{...}}, "storage": {{...}}, "retail_transit": {{...}} }}
            """
            ai_resp = self._get_ai_insight(prompt)
            result['ai_qa_transit'] = ai_resp.get('transit', {})
            result['ai_qa_storage'] = ai_resp.get('storage', {})
            result['ai_qa_retail_transit'] = ai_resp.get('retail_transit', {})

        return result

    def chat_with_data(self, message, context_data=None):
        if not self.client: return "AI System Offline."
        system_prompt = "You are a Supply Chain Assistant. Answer ONLY custom queries about shelf life, batch specifics, complaints, logistics."
        user_content = f"Query: {message}\nBatch Context: {json.dumps(context_data, default=str)[:3000]}"
        try:
            res = self.client.chat.completions.create(model="google/gemini-2.0-flash-001", messages=[{"role": "system", "content": system_prompt}, {"role": "user", "content": user_content}])
            return res.choices[0].message.content
        except Exception as e: return f"Error: {str(e)}"

    def _get_ai_insight(self, prompt):
        try:
            res = self.client.chat.completions.create(model="google/gemini-2.0-flash-001", messages=[{"role":"user", "content": prompt + " Return ONLY valid JSON."}])
            return json.loads(res.choices[0].message.content.replace('```json', '').replace('```', '').strip())
        except: return {"transit": {}, "storage": {}, "retail_transit": {}}

    # Legacy
    def analyze_factory_dealer_flow(self, *args, **kwargs): return {} 
    def forecast_demand(self, df, date_col, qty_col, product_col=None, entity_type='dealer'): return {"forecast": self._generate_forecast(df, date_col, qty_col)}
    def analyze_complaint(self, text): return self.chat_with_data(text)