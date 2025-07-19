"""
Stock Data Web Scraper - Main Scraping Module
Handles data extraction from multiple financial websites
"""

import requests
import time
import random
import logging
import pandas as pd
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import TimeoutException, WebDriverException
from typing import List, Dict, Optional, Union
import re
from datetime import datetime
import warnings
warnings.filterwarnings('ignore')

logger = logging.getLogger(__name__)

class StockScraper:
    """Main scraping class for stock data collection"""
    
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        })
        self.driver = None
        self.setup_selenium()
        
    def setup_selenium(self):
        """Setup Selenium WebDriver with options"""
        try:
            chrome_options = Options()
            chrome_options.add_argument('--headless')  # Run in background
            chrome_options.add_argument('--no-sandbox')
            chrome_options.add_argument('--disable-dev-shm-usage')
            chrome_options.add_argument('--disable-gpu')
            chrome_options.add_argument('--window-size=1920,1080')
            chrome_options.add_argument('--disable-blink-features=AutomationControlled')
            chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
            chrome_options.add_experimental_option('useAutomationExtension', False)
            
            self.driver = webdriver.Chrome(options=chrome_options)
            self.driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
            logger.info("Selenium WebDriver initialized successfully")
            
        except Exception as e:
            logger.warning(f"Selenium setup failed: {e}. Will use requests only.")
            self.driver = None
            
    def scrape_yahoo_finance(self, symbol: str) -> Dict:
        """Scrape stock data from Yahoo Finance"""
        try:
            url = f"https://finance.yahoo.com/quote/{symbol}"
            response = self.session.get(url, timeout=10)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'html.parser')
            
            data = {'symbol': symbol, 'source': 'yahoo'}
            
            # Extract current price
            price_element = soup.find('fin-streamer', {'data-field': 'regularMarketPrice'})
            if price_element:
                data['current_price'] = self.clean_price(price_element.get_text())
            
            # Extract change and percentage
            change_element = soup.find('fin-streamer', {'data-field': 'regularMarketChange'})
            if change_element:
                data['change'] = self.clean_price(change_element.get_text())
                
            change_percent_element = soup.find('fin-streamer', {'data-field': 'regularMarketChangePercent'})
            if change_percent_element:
                data['change_percent'] = self.clean_percentage(change_percent_element.get_text())
            
            # Extract additional data from summary table
            summary_data = self.extract_yahoo_summary_data(soup)
            data.update(summary_data)
            
            # Extract company name
            name_element = soup.find('h1', {'data-reactid': True})
            if name_element:
                data['company_name'] = name_element.get_text().split('(')[0].strip()
            
            data['timestamp'] = datetime.now().isoformat()
            logger.info(f"Successfully scraped {symbol} from Yahoo Finance")
            
            return data
            
        except Exception as e:
            logger.error(f"Error scraping {symbol} from Yahoo Finance: {e}")
            return {'symbol': symbol, 'source': 'yahoo', 'error': str(e)}
    
    def scrape_multiple_stocks(self, symbols: List[str]) -> Optional[pd.DataFrame]:
        """Scrape data for multiple stocks"""
        logger.info(f"Scraping data for {len(symbols)} stocks: {symbols}")
        
        all_stock_data = []
        
        for i, symbol in enumerate(symbols):
            try:
                logger.info(f"Scraping {symbol} ({i+1}/{len(symbols)})")
                stock_data = self.scrape_stock_data(symbol)
                
                if 'error' not in stock_data:
                    all_stock_data.append(stock_data)
                else:
                    logger.warning(f"Failed to scrape {symbol}: {stock_data.get('error', 'Unknown error')}")
                
                # Add delay between stocks
                if i < len(symbols) - 1:
                    time.sleep(random.uniform(2, 5))
                    
            except Exception as e:
                logger.error(f"Error processing {symbol}: {e}")
                continue
        
        if all_stock_data:
            df = pd.DataFrame(all_stock_data)
            logger.info(f"Successfully scraped {len(df)} stocks")
            return df
        else:
            logger.error("No stock data scraped successfully")
            return None
    
    def scrape_stock_data(self, symbol: str) -> Dict:
        """Scrape stock data from multiple sources"""
        logger.info(f"Scraping data for {symbol}")
        
        # Try Yahoo Finance first
        try:
            data = self.scrape_yahoo_finance(symbol)
            if 'error' not in data:
                return data
        except Exception as e:
            logger.error(f"Error scraping {symbol} from Yahoo Finance: {e}")
            
        # Fallback to basic data structure
        return {
            'symbol': symbol,
            'current_price': None,
            'change': None,
            'change_percent': None,
            'timestamp': datetime.now().isoformat(),
            'error': 'Could not fetch data'
        }
    
    def clean_price(self, price_str: str) -> Optional[float]:
        """Clean and convert price string to float"""
        if not price_str:
            return None
            
        try:
            # Remove currency symbols and commas
            cleaned = re.sub(r'[^0-9.-]', '', price_str)
            return float(cleaned) if cleaned else None
        except:
            return None
    
    def clean_percentage(self, percent_str: str) -> Optional[float]:
        """Clean and convert percentage string to float"""
        if not percent_str:
            return None
            
        try:
            # Remove % sign and parentheses
            cleaned = re.sub(r'[^0-9.-]', '', percent_str)
            return float(cleaned) if cleaned else None
        except:
            return None
    
    def extract_yahoo_summary_data(self, soup: BeautifulSoup) -> Dict:
        """Extract additional data from Yahoo Finance summary table"""
        data = {}
        
        try:
            # Find all data rows
            rows = soup.find_all('tr')
            
            for row in rows:
                cells = row.find_all('td')
                if len(cells) >= 2:
                    key = cells[0].get_text().strip()
                    value = cells[1].get_text().strip()
                    
                    # Map keys to our data structure
                    if 'Previous Close' in key:
                        data['previous_close'] = self.clean_price(value)
                    elif 'Open' in key:
                        data['open'] = self.clean_price(value)
                    elif 'Volume' in key:
                        data['volume'] = value
                    elif 'Market Cap' in key:
                        data['market_cap'] = value
                    elif 'P/E Ratio' in key:
                        data['pe_ratio'] = self.clean_price(value)
                        
        except Exception as e:
            logger.warning(f"Error extracting Yahoo summary data: {e}")
            
        return data
    
    def __del__(self):
        """Clean up resources"""
        if self.driver:
            try:
                self.driver.quit()
            except:
                pass
