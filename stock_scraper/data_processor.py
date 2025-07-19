"""
Stock Data Web Scraper - Data Processing Module
Handles data cleaning, preprocessing, and transformation
"""

import pandas as pd
import numpy as np
import logging
import re
from typing import Optional, Dict, List
from datetime import datetime

logger = logging.getLogger(__name__)

class DataProcessor:
    """Data processing and cleaning class"""
    
    def __init__(self):
        self.numeric_columns = [
            'current_price', 'change', 'change_percent', 'previous_close',
            'open', 'day_low', 'day_high', 'week_52_low', 'week_52_high',
            'pe_ratio', 'eps'
        ]
        
    def process_data(self, input_file: str) -> Optional[pd.DataFrame]:
        """Process raw stock data"""
        try:
            logger.info(f"Processing data from {input_file}")
            
            # Load raw data
            df = pd.read_csv(input_file)
            
            if df.empty:
                logger.warning("Input file is empty")
                return None
            
            logger.info(f"Loaded {len(df)} records from {input_file}")
            
            # Clean and process data
            df_processed = self.clean_data(df)
            df_processed = self.standardize_formats(df_processed)
            df_processed = self.handle_missing_values(df_processed)
            df_processed = self.add_derived_metrics(df_processed)
            
            logger.info(f"Processing completed. Final dataset has {len(df_processed)} records")
            
            return df_processed
            
        except Exception as e:
            logger.error(f"Error processing data: {e}")
            return None
    
    def clean_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean raw data"""
        df_clean = df.copy()
        
        # Remove rows with errors
        if 'error' in df_clean.columns:
            df_clean = df_clean[df_clean['error'].isna()]
        
        # Clean numeric columns
        for col in self.numeric_columns:
            if col in df_clean.columns:
                df_clean[col] = df_clean[col].apply(self.clean_numeric_value)
        
        # Clean volume column
        if 'volume' in df_clean.columns:
            df_clean['volume'] = df_clean['volume'].apply(self.clean_volume_value)
        
        # Clean market cap column
        if 'market_cap' in df_clean.columns:
            df_clean['market_cap'] = df_clean['market_cap'].apply(self.clean_market_cap_value)
        
        # Clean company names
        if 'company_name' in df_clean.columns:
            df_clean['company_name'] = df_clean['company_name'].apply(self.clean_company_name)
        
        logger.info(f"Data cleaning completed. {len(df_clean)} records remaining")
        
        return df_clean
    
    def standardize_formats(self, df: pd.DataFrame) -> pd.DataFrame:
        """Standardize data formats"""
        df_std = df.copy()
        
        # Standardize timestamp format
        if 'timestamp' in df_std.columns:
            df_std['timestamp'] = pd.to_datetime(df_std['timestamp'])
        
        # Ensure numeric columns are float type
        for col in self.numeric_columns:
            if col in df_std.columns:
                df_std[col] = pd.to_numeric(df_std[col], errors='coerce')
        
        # Convert volume to integer
        if 'volume' in df_std.columns:
            df_std['volume'] = pd.to_numeric(df_std['volume'], errors='coerce').astype('Int64')
        
        # Add processing timestamp
        df_std['processed_at'] = datetime.now()
        
        logger.info("Data format standardization completed")
        
        return df_std
    
    def handle_missing_values(self, df: pd.DataFrame) -> pd.DataFrame:
        """Handle missing values in the dataset"""
        df_filled = df.copy()
        
        # Fill missing values with appropriate strategies
        numeric_cols = df_filled.select_dtypes(include=[np.number]).columns
        
        for col in numeric_cols:
            if col in ['current_price', 'previous_close', 'open']:
                # Use forward fill for price columns
                df_filled[col] = df_filled[col].fillna(method='ffill')
            elif col in ['change', 'change_percent']:
                # Fill with 0 for change columns
                df_filled[col] = df_filled[col].fillna(0)
            else:
                # Use median for other numeric columns
                df_filled[col] = df_filled[col].fillna(df_filled[col].median())
        
        # Fill missing company names
        if 'company_name' in df_filled.columns:
            df_filled['company_name'] = df_filled['company_name'].fillna(df_filled['symbol'])
        
        logger.info("Missing value handling completed")
        
        return df_filled
    
    def add_derived_metrics(self, df: pd.DataFrame) -> pd.DataFrame:
        """Add derived metrics and features"""
        df_derived = df.copy()
        
        # Calculate price momentum
        if 'current_price' in df_derived.columns and 'previous_close' in df_derived.columns:
            df_derived['price_momentum'] = (
                (df_derived['current_price'] - df_derived['previous_close']) / 
                df_derived['previous_close'] * 100
            )
        
        # Calculate volatility indicator
        if 'day_high' in df_derived.columns and 'day_low' in df_derived.columns:
            df_derived['daily_volatility'] = (
                (df_derived['day_high'] - df_derived['day_low']) / 
                df_derived['current_price'] * 100
            )
        
        # Add market cap category
        if 'market_cap' in df_derived.columns:
            df_derived['market_cap_category'] = df_derived['market_cap'].apply(
                self.categorize_market_cap
            )
        
        # Add performance category
        if 'change_percent' in df_derived.columns:
            df_derived['performance_category'] = df_derived['change_percent'].apply(
                self.categorize_performance
            )
        
        logger.info("Derived metrics calculation completed")
        
        return df_derived
    
    def clean_numeric_value(self, value) -> Optional[float]:
        """Clean numeric values"""
        if pd.isna(value):
            return None
        
        if isinstance(value, (int, float)):
            return float(value)
        
        if isinstance(value, str):
            # Remove currency symbols, commas, and parentheses
            cleaned = re.sub(r'[^\d.-]', '', value)
            try:
                return float(cleaned) if cleaned else None
            except ValueError:
                return None
        
        return None
    
    def clean_volume_value(self, value) -> Optional[int]:
        """Clean volume values"""
        if pd.isna(value):
            return None
        
        if isinstance(value, (int, float)):
            return int(value)
        
        if isinstance(value, str):
            # Handle K, M, B suffixes
            multipliers = {'K': 1000, 'M': 1000000, 'B': 1000000000}
            
            value_upper = value.upper().replace(',', '')
            
            for suffix, multiplier in multipliers.items():
                if suffix in value_upper:
                    try:
                        number = float(value_upper.replace(suffix, ''))
                        return int(number * multiplier)
                    except ValueError:
                        return None
            
            # No suffix
            try:
                return int(float(re.sub(r'[^\d.]', '', value)))
            except ValueError:
                return None
        
        return None
    
    def clean_market_cap_value(self, value) -> Optional[str]:
        """Clean market cap values"""
        if pd.isna(value):
            return None
        
        if isinstance(value, str):
            # Keep original format for market cap
            return value.strip()
        
        return str(value)
    
    def clean_company_name(self, value) -> Optional[str]:
        """Clean company names"""
        if pd.isna(value):
            return None
        
        if isinstance(value, str):
            # Remove extra whitespace and common suffixes
            name = value.strip()
            # Remove stock symbol in parentheses
            name = re.sub(r'\([^)]*\)', '', name).strip()
            return name
        
        return str(value)
    
    def categorize_market_cap(self, market_cap) -> str:
        """Categorize market cap into size categories"""
        if pd.isna(market_cap):
            return 'Unknown'
        
        if isinstance(market_cap, str):
            if 'T' in market_cap:
                return 'Mega Cap'
            elif 'B' in market_cap:
                return 'Large Cap'
            elif 'M' in market_cap:
                return 'Mid Cap'
            else:
                return 'Small Cap'
        
        return 'Unknown'
    
    def categorize_performance(self, change_percent) -> str:
        """Categorize performance based on change percentage"""
        if pd.isna(change_percent):
            return 'Unknown'
        
        if change_percent > 5:
            return 'Strong Positive'
        elif change_percent > 2:
            return 'Positive'
        elif change_percent > -2:
            return 'Neutral'
        elif change_percent > -5:
            return 'Negative'
        else:
            return 'Strong Negative'
    
    def preview_data(self, filename: str, rows: int = 10):
        """Preview data file"""
        try:
            df = pd.read_csv(filename)
            print(f"\n--- Data Preview: {filename} ---")
            print(f"Shape: {df.shape}")
            print(f"Columns: {list(df.columns)}")
            print(f"\nFirst {rows} rows:")
            print(df.head(rows))
            print(f"\nData types:")
            print(df.dtypes)
            
        except Exception as e:
            print(f"Error previewing data: {e}")
    
    def get_data_quality_report(self, df: pd.DataFrame) -> Dict:
        """Generate data quality report"""
        report = {
            'total_records': len(df),
            'total_columns': len(df.columns),
            'missing_values': df.isnull().sum().to_dict(),
            'duplicate_records': df.duplicated().sum(),
            'data_types': df.dtypes.to_dict()
        }
        
        # Add numeric statistics
        numeric_cols = df.select_dtypes(include=[np.number]).columns
        if len(numeric_cols) > 0:
            report['numeric_summary'] = df[numeric_cols].describe().to_dict()
        
        return report
