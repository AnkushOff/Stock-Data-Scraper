#!/usr/bin/env python3
"""
Stock Data Web Scraper - Main Application
A comprehensive web scraping tool for stock market data analysis
"""

import os
import sys
import argparse
import logging
from datetime import datetime
from typing import List, Optional

# Import our custom modules
from stock_scraper.scraper import StockScraper
from stock_scraper.data_processor import DataProcessor
from stock_scraper.analyzer import StockAnalyzer
from stock_scraper.scheduler import SchedulerManager
from stock_scraper.config import Config

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('stock_scraper.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class StockScraperApp:
    """Main application class for Stock Data Web Scraper"""
    
    def __init__(self):
        self.config = Config()
        self.scraper = StockScraper()
        self.processor = DataProcessor()
        self.analyzer = StockAnalyzer()
        self.scheduler = SchedulerManager()
        
    def interactive_mode(self):
        """Interactive menu-driven interface"""
        print("\n" + "="*50)
        print("    STOCK DATA WEB SCRAPER")
        print("="*50)
        
        while True:
            print("\n--- Main Menu ---")
            print("1. Collect Stock Data")
            print("2. Process and Clean Data")
            print("3. Analyze Data")
            print("4. Generate Visualizations")
            print("5. Schedule Data Collection")
            print("6. View Collected Data")
            print("7. Exit")
            
            choice = input("\nEnter your choice (1-7): ").strip()
            
            try:
                if choice == '1':
                    self.collect_data_interactive()
                elif choice == '2':
                    self.process_data_interactive()
                elif choice == '3':
                    self.analyze_data_interactive()
                elif choice == '4':
                    self.generate_visualizations_interactive()
                elif choice == '5':
                    self.setup_scheduler_interactive()
                elif choice == '6':
                    self.view_data_interactive()
                elif choice == '7':
                    print("\nThank you for using Stock Data Web Scraper!")
                    break
                else:
                    print("Invalid choice. Please try again.")
                    
            except Exception as e:
                logger.error(f"Error in interactive mode: {e}")
                print(f"\nError: {e}")
                
    def collect_data_interactive(self):
        """Interactive data collection"""
        print("\n--- Data Collection ---")
        
        # Get stock symbols from user
        symbols_input = input("Enter stock symbols (comma-separated, e.g., AAPL,GOOGL,MSFT): ").strip()
        if not symbols_input:
            print("No symbols provided. Using default symbols.")
            symbols = self.config.DEFAULT_SYMBOLS
        else:
            symbols = [s.strip().upper() for s in symbols_input.split(',')]
            
        print(f"\nCollecting data for: {', '.join(symbols)}")
        
        # Collect data
        try:
            raw_data = self.scraper.scrape_multiple_stocks(symbols)
            
            if raw_data is not None:
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                filename = f"raw_stock_data_{timestamp}.csv"
                raw_data.to_csv(filename, index=False)
                print(f"\nData collection completed! Saved to: {filename}")
                print(f"Collected data for {len(raw_data)} stocks.")
            else:
                print("\nNo data collected. Please check your internet connection and try again.")
                
        except Exception as e:
            logger.error(f"Error during data collection: {e}")
            print(f"\nError during data collection: {e}")

    def process_data_interactive(self):
        """Interactive data processing"""
        print("\n--- Data Processing ---")
        
        # Get input file
        filename = input("Enter the raw data file name: ").strip()
        
        if not filename:
            print("No filename provided.")
            return
        
        if not os.path.exists(filename):
            print(f"File '{filename}' not found.")
            return
        
        try:
            # Process data
            processed_data = self.processor.process_data(filename)
            
            if processed_data is not None:
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                output_filename = f"processed_stock_data_{timestamp}.csv"
                processed_data.to_csv(output_filename, index=False)
                print(f"\nData processing completed! Saved to: {output_filename}")
                print(f"Processed {len(processed_data)} records.")
            else:
                print("Data processing failed. Please check the file and try again.")
                
        except Exception as e:
            print(f"Error during data processing: {e}")

    def analyze_data_interactive(self):
        """Interactive data analysis"""
        self.analyzer.analyze_data_interactive()

    def generate_visualizations_interactive(self):
        """Interactive visualization generation"""
        self.analyzer.generate_visualizations_interactive()

    def setup_scheduler_interactive(self):
        """Interactive scheduler setup"""
        self.scheduler.setup_scheduler_interactive()

    def view_data_interactive(self):
        """Interactive data viewing"""
        print("\n--- View Collected Data ---")
        
        # List available files
        csv_files = [f for f in os.listdir('.') if f.endswith('.csv')]
        
        if not csv_files:
            print("No CSV files found in current directory.")
            return
        
        print("Available data files:")
        for i, file in enumerate(csv_files, 1):
            print(f"{i}. {file}")
        
        try:
            choice = int(input(f"\nSelect file to view (1-{len(csv_files)}): ")) - 1
            if 0 <= choice < len(csv_files):
                selected_file = csv_files[choice]
                self.processor.preview_data(selected_file)
            else:
                print("Invalid selection.")
        except ValueError:
            print("Please enter a valid number.")

    def run_cli(self, args):
        """Run command-line interface"""
        if args.collect:
            symbols = args.symbols if args.symbols else self.config.DEFAULT_SYMBOLS
            raw_data = self.scraper.scrape_multiple_stocks(symbols)
            
            if raw_data is not None:
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                filename = f"raw_stock_data_{timestamp}.csv"
                raw_data.to_csv(filename, index=False)
                print(f"Data collected and saved to: {filename}")

def main():
    """Main function"""
    parser = argparse.ArgumentParser(description='Stock Data Web Scraper')
    parser.add_argument('--collect', action='store_true', help='Collect stock data')
    parser.add_argument('--process', action='store_true', help='Process raw data')
    parser.add_argument('--analyze', action='store_true', help='Analyze processed data')
    parser.add_argument('--visualize', action='store_true', help='Generate visualizations')
    parser.add_argument('--schedule', action='store_true', help='Setup scheduled collection')
    parser.add_argument('--symbols', nargs='+', help='Stock symbols to collect')
    parser.add_argument('--file', help='Input file for processing/analysis')
    parser.add_argument('--config', choices=['daily', 'market_hours', 'hourly'], help='Scheduler configuration')
    
    args = parser.parse_args()
    
    app = StockScraperApp()
    
    # If no arguments provided, run interactive mode
    if len(sys.argv) == 1:
        app.interactive_mode()
    else:
        app.run_cli(args)

if __name__ == "__main__":
    main()
