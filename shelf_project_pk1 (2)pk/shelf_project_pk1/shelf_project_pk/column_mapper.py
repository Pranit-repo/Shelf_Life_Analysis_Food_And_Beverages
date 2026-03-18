import re
from fuzzywuzzy import fuzz
import pandas as pd

class ColumnMapper:
    """
    Maps raw CSV column headers to standardized logical names.
    Includes fuzzy matching to handle messy or inconsistent data headers.
    """
    
    # Dictionary of acceptable variations for each logical column
    COLUMN_VARIANTS = {
        'batch_id': [
            'batch', 'batch_id', 'batchno', 'batch_no', 'lot', 'lot_no', 'lot_number', 
            'lotid', 'lot_id', 'batchcode', 'batch_code', 'production_batch', 'prod_batch',
            'batchnumber', 'Batch Number', 'LotNumber', 'bf_batch', 'bf_batch_id',
            'shipment_id', 'consignment_no', 'consignment_id', 'dispatch_batch'
        ],
        'date': [
            'date', 'datetime', 'timestamp', 'time', 'created_at', 'updated_at', 
            'event_date', 'transaction_date', 'trans_date'
        ],
        'manufacturing_date': [
            'mfg_date', 'manufacturing_date', 'prod_date', 'production_date', 
            'manf_date', 'fm_manufacturing_date', 'date_of_manufacture'
        ],
        'dispatch_date': [
            'dispatch_date', 'ship_date', 'shipping_date', 'shipped_on', 
            'dd_dispatch_date', 'fd_factory_dispatch_date', 'dispatch_time'
        ],
        'receipt_date': [
            'receipt_date', 'received_date', 'arrival_date', 'dr_receipt_date', 
            'rr_receipt_date', 'date_received', 'receiving_date'
        ],
        'stock_date': [
            'stock_date', 'stock_as_on', 'stock_as_on_date', 'inventory_date', 
            'rs_stock_as_on_date', 'as_on_date', 'position_date'
        ],
        'retailer_name': [
            'retailer', 'retailer_name', 'shop_name', 'store_name', 'rr_retailer_name', 
            'rs_retailer_name', 'dd_retailer_name', 'customer_name'
        ],
        'received_quantity': [
            'received_qty', 'received_quantity', 'qty_received', 'rr_received_quantity', 
            'rs_received_quantity', 'dr_received_quantity', 'inward_qty'
        ],
        'city': [
            'city', 'location', 'town', 'place', 'dd_city', 'rr_city', 'dr_city', 
            'fd_city', 'destination', 'source_city'
        ]
    }

    def get_best_match(self, columns, logical_name):
        """
        Finds the column in 'columns' that best matches the 'logical_name'.
        Returns the actual column name string, or None if no match found.
        """
        # 1. Direct Normalization Match
        # Check if the logical name variants exist directly in the columns (case-insensitive)
        variants = self.COLUMN_VARIANTS.get(logical_name, [])
        columns_lower = {c.lower().strip(): c for c in columns}
        
        # Add the logical name itself as a search variant
        search_terms = [logical_name] + variants
        
        for variant in search_terms:
            variant_clean = variant.lower().strip()
            if variant_clean in columns_lower:
                return columns_lower[variant_clean]
        
        # 2. Substring Match (e.g. "FM_Manufacturing_Date" contains "Manufacturing_Date")
        for col in columns:
            col_lower = col.lower()
            for variant in search_terms:
                if variant in col_lower:
                    return col
        
        # 3. Fuzzy Match (Last Resort)
        best_col = None
        best_score = 0
        
        for col in columns:
            col_lower = col.lower()
            for variant in search_terms:
                score = fuzz.partial_ratio(variant, col_lower)
                if score > 85 and score > best_score:
                    best_score = score
                    best_col = col
        
        return best_col

    def map_dataframe_columns(self, df):
        """
        Returns a dictionary mapping actual DataFrame columns to logical fields.
        Format: {'Actual_Col_Name': 'logical_field', ...}
        """
        mapping = {}
        for logical_field in self.COLUMN_VARIANTS.keys():
            match = self.get_best_match(df.columns, logical_field)
            if match:
                mapping[match] = logical_field
        return mapping