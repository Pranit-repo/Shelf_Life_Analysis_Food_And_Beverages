import pandas as pd
import os
import json
from column_mapper import ColumnMapper
import io

class DataManager:
    """
    Manages access to massive datasets using file paths and chunking.
    Robustly handles encoding errors and messy CSV headers.
    
    [UPDATED] Includes 'Contextual Scanning' to link Retailer Receipt to Stock
    without a common Batch ID.
    """
    
    def __init__(self):
        # Stores file paths: {'factory_manufacturing': 'D:/data/factory.csv', ...}
        self.file_paths = {} 
        self.dataset_previews = {}
        self.mapper = ColumnMapper()

    def register_dataset(self, key, file_path):
        """Register a dataset path and analyze its columns immediately."""
        if not os.path.exists(file_path):
            print(f"[ERROR] File not found: {file_path}")
            return False

        # Normalize key to lowercase to prevent mismatch
        key = key.lower()
        self.file_paths[key] = file_path
        
        # Try multiple encodings to handle Excel/Legacy formats
        encodings_to_try = ['utf-8', 'utf-8-sig', 'latin1', 'cp1252']
        
        for encoding in encodings_to_try:
            try:
                # Read just the header and first few rows
                df_preview = pd.read_csv(
                    file_path, 
                    nrows=100, 
                    encoding=encoding, 
                    on_bad_lines='skip',
                    engine='python' # More robust engine
                )
                
                # Store preview metadata
                self.dataset_previews[key] = {
                    'columns': list(df_preview.columns),
                    'preview': df_preview.head(5).to_dict(orient='records'),
                    'encoding': encoding
                }
                return True
            except Exception as e:
                continue
                
        print(f"[FAIL] Could not read {key} with any standard encoding.")
        return False

    def get_columns(self, key):
        """Get columns for a specific dataset."""
        return self.dataset_previews.get(key, {}).get('columns', [])

    def get_unique_values_paged(self, key, column, page=1, per_page=50):
        """Efficiently get unique values from a large CSV without loading it all."""
        if key not in self.file_paths: return []
        
        # In a real massive file scenario, we'd cache this or use a database.
        # For this demo, we scan the file (which is okay for <1GB files).
        try:
            seen = set()
            path = self.file_paths[key]
            enc = self.dataset_previews[key]['encoding']
            
            for chunk in pd.read_csv(path, usecols=[column], chunksize=50000, encoding=enc, on_bad_lines='skip'):
                seen.update(chunk[column].dropna().astype(str).unique())
                if len(seen) > 1000: break # Limit for performance
            
            return sorted(list(seen))[:per_page]
        except:
            return []

    def scan_for_entity(self, entity_column, entity_value):
        """
        Scans ALL registered datasets for a specific entity (e.g., Batch ID).
        
        [CRITICAL UPDATE]: Implements 'Contextual Scanning'.
        If 'Retailer Stock' is missing the Batch ID, it uses the Retailer Name + Quantity
        found in the 'Retailer Receipt' stage to bridge the gap.
        """
        results = {}
        # Normalize the search column (e.g., 'Batch_ID' -> 'batch_id')
        normalized_entity_col = entity_column.lower().replace(' ', '_')
        
        # Context Storage: To bridge the gap between Receipt and Stock
        link_context = {} 

        # Iterate through datasets in order
        # Note: We rely on standard insertion order, but explicit handling is better.
        # The key names should match app.py DATASET_KEYS logic
        
        for key, file_path in self.file_paths.items():
            enc = self.dataset_previews[key]['encoding']
            preview_cols = self.dataset_previews[key]['columns']
            
            # 1. Try to find the Batch ID column in this file
            target_col = self.mapper.get_best_match(preview_cols, normalized_entity_col)
            
            # --- CONTEXTUAL BRIDGE LOGIC START ---
            is_stock_file = 'stock' in key and 'retailer' in key
            
            if not target_col and is_stock_file and link_context:
                # If we are in Retailer Stock, have no Batch ID, but have context from Receipt
                # We perform a "Composite Scan"
                try:
                    # Find the Name and Qty columns in this Stock file
                    name_col = self.mapper.get_best_match(preview_cols, 'retailer_name')
                    qty_col = self.mapper.get_best_match(preview_cols, 'received_quantity')
                    
                    if name_col and qty_col:
                        stock_rows = []
                        ctx_name = str(link_context['name']).strip()
                        ctx_qty = str(link_context['qty']).strip()

                        for chunk in pd.read_csv(file_path, chunksize=100000, encoding=enc, on_bad_lines='skip'):
                            # String comparison for robustness
                            mask = (chunk[name_col].astype(str).str.strip() == ctx_name) & \
                                   (chunk[qty_col].astype(str).str.strip() == ctx_qty)
                            
                            matches = chunk[mask]
                            if not matches.empty:
                                stock_rows.append(matches)
                        
                        if stock_rows:
                            results[key] = pd.concat(stock_rows)
                            # Create a fake Batch ID column for consistency downstream
                            results[key]['Batch_ID'] = entity_value 
                except Exception as e:
                    print(f"Context Scan Error in {key}: {e}")
                
                # Continue to next file, we are done with stock
                continue
            # --- CONTEXTUAL BRIDGE LOGIC END ---

            # 2. If standard column mapping failed, try hardcoded fallbacks
            if not target_col:
                 if 'batch' in normalized_entity_col:
                     for col in preview_cols:
                         if 'batch' in col.lower() and 'id' in col.lower():
                             target_col = col
                             break
            
            if not target_col:
                continue

            # 3. Standard Scan (Batch ID exists)
            try:
                found_rows = []
                for chunk in pd.read_csv(file_path, chunksize=100000, encoding=enc, on_bad_lines='skip'):
                    chunk[target_col] = chunk[target_col].astype(str)
                    matches = chunk[chunk[target_col].str.strip() == str(entity_value).strip()]
                    
                    if not matches.empty:
                        # Normalize Batch_ID column name
                        if target_col != 'Batch_ID' and normalized_entity_col == 'batch_id':
                             matches = matches.rename(columns={target_col: 'Batch_ID'})
                        found_rows.append(matches)
                
                if found_rows:
                    final_df = pd.concat(found_rows)
                    results[key] = final_df
                    
                    # --- CAPTURE CONTEXT ---
                    # If this is the Retailer Receipt stage, grab Name & Qty
                    if 'receipt' in key and 'retailer' in key and not final_df.empty:
                        try:
                            # Use mapper to find specific columns in the Receipt file
                            r_name_col = self.mapper.get_best_match(final_df.columns, 'retailer_name')
                            r_qty_col = self.mapper.get_best_match(final_df.columns, 'received_quantity')
                            
                            if r_name_col and r_qty_col:
                                # Take the first match to establish context
                                link_context = {
                                    'name': final_df.iloc[0][r_name_col],
                                    'qty': final_df.iloc[0][r_qty_col]
                                }
                        except:
                            pass # Fail silently, just won't link stock
                            
            except Exception as e:
                print(f"Error scanning {key}: {e}")

        return results