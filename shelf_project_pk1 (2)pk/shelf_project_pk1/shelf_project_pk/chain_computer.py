import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Tuple
from collections import defaultdict
import warnings

class ChainComputer:
    """
    Compute supply chain transit times and shelf life across stages.
    [UPDATED] Now supports 'Retailer Stock' as a valid end-stage for Shelf Life calculation.
    """
    
    def __init__(self, stages_data: List[Dict], column_mappings: List[Dict]):
        self.stages_data = stages_data  # List of {stage_id, df, filename}
        self.column_mappings = column_mappings  # List of mappings per stage
        self.chains = []
        self.warnings = []
    
    def safe_date_parse(self, date_series: pd.Series) -> pd.Series:
        """Safely parse dates with multiple format support"""
        return pd.to_datetime(date_series, dayfirst=True, errors='coerce')
    
    def get_stage_date_column(self, stage_idx: int) -> Optional[str]:
        """Get the best date column for a stage"""
        stage_mapping = self.column_mappings[stage_idx]
        
        # Priority order for date columns [UPDATED] to include stock dates
        date_priority = [
            'manufacturing_date', 
            'dispatch_date', 
            'receipt_date', 
            'stock_date', 
            'stock_as_on_date', # Specific for Retailer Stock
            'event_date',
            'date'
        ]
        
        for date_field in date_priority:
            for orig_col, logical_field in stage_mapping.items():
                if logical_field == date_field:
                    return orig_col
        
        # Fallback: look for any column with 'date' in the name
        stage_df = self.stages_data[stage_idx]['df']
        for col in stage_df.columns:
            if 'date' in col.lower():
                return col
                
        return None

    def compute_entity_chain(self, entity_column: str, entity_value: str) -> Dict:
        """
        Trace a specific entity (Batch ID) through all stages.
        """
        entity_chains = []
        
        # We start looking from the first stage (usually Factory Manufacturing)
        start_stage_idx = 0
        start_df = self.stages_data[start_stage_idx]['df']
        
        # Find rows in the first stage
        start_rows = start_df[start_df[entity_column].astype(str).str.strip() == str(entity_value).strip()]
        
        if start_rows.empty:
            return {'error': 'Entity not found in start stage', 'count': 0}

        # Iterate through each starting item (e.g., a specific batch could be split, but usually unique at start)
        for _, start_row in start_rows.iterrows():
            chain = {
                'entity_value': entity_value,
                'stages': [],
                'total_transit': 0,
                'total_shelf_life': 0, # [UPDATED] New Metric
                'is_complete': False
            }
            
            # 1. Add First Stage
            date_col = self.get_stage_date_column(start_stage_idx)
            start_date = self.safe_date_parse(pd.Series([start_row[date_col]])).iloc[0] if date_col else None
            
            chain['stages'].append({
                'stage_name': self.stages_data[start_stage_idx]['stage_id'],
                'date': start_date,
                'details': start_row.to_dict()
            })
            
            current_date = start_date
            
            # 2. Trace Subsequent Stages
            for i in range(1, len(self.stages_data)):
                stage_info = self.stages_data[i]
                stage_df = stage_info['df']
                stage_id = stage_info['stage_id']
                
                # Check if this stage has the entity (Batch ID)
                # Note: DataManager has already injected Batch_ID into Stock dataframe if needed
                if entity_column not in stage_df.columns:
                    continue
                
                # Find matching row
                # In a real graph, we'd need more complex linking (e.g. Dealer Name), 
                # but app.py filters datasets before passing here, so we assume direct filtering.
                match = stage_df[stage_df[entity_column].astype(str).str.strip() == str(entity_value).strip()]
                
                if not match.empty:
                    row = match.iloc[0] # Take first match for linear chain
                    
                    date_col_next = self.get_stage_date_column(i)
                    next_date = self.safe_date_parse(pd.Series([row[date_col_next]])).iloc[0] if date_col_next else None
                    
                    if current_date and next_date:
                        duration = (next_date - current_date).days
                        
                        # [UPDATED] Logic to distinguish Transit vs Shelf Life
                        if 'stock' in stage_id.lower() and 'receipt' in self.stages_data[i-1]['stage_id'].lower():
                            chain['total_shelf_life'] += duration
                            stage_type = 'shelf_life'
                        else:
                            chain['total_transit'] += duration
                            stage_type = 'transit'
                    else:
                        duration = 0
                        stage_type = 'unknown'

                    chain['stages'].append({
                        'stage_name': stage_id,
                        'date': next_date,
                        'duration_from_prev': duration,
                        'type': stage_type,
                        'details': row.to_dict()
                    })
                    
                    current_date = next_date
            
            entity_chains.append(chain)
        
        # Summarize
        if not entity_chains:
             return {'count': 0, 'metrics': {}}

        # Calculate Averages
        transits = [c['total_transit'] for c in entity_chains]
        shelf_lives = [c['total_shelf_life'] for c in entity_chains]
        
        metrics = {
            'avg_transit': np.mean(transits) if transits else 0,
            'max_transit': np.max(transits) if transits else 0,
            'avg_shelf_life': np.mean(shelf_lives) if shelf_lives else 0, # [UPDATED]
            'count': len(entity_chains)
        }
        
        return {
            'entity': entity_value,
            'chains': entity_chains,
            'metrics': metrics
        }