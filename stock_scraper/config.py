"""
Stock Data Web Scraper - Configuration Module
Handles application configuration and settings
"""

import os
from typing import List, Dict, Any

class Config:
    """Configuration class for Stock Data Web Scraper"""
    
    def __init__(self):
        # Default stock symbols
        self.DEFAULT_SYMBOLS = [
            'AAPL', 'GOOGL', 'MSFT', 'AMZN', 'TSLA',
            'META', 'NFLX', 'NVDA', 'AMD', 'INTC'
        ]
        
        # Scraping settings
        self.REQUEST_TIMEOUT = 10
        self.RETRY_ATTEMPTS = 3
        self.DELAY_BETWEEN_REQUESTS = 2
        self.SELENIUM_TIMEOUT = 10
        
        # Data processing settings
        self.MAX_MISSING_PERCENTAGE = 50
        self.OUTLIER_THRESHOLD = 3  # Standard deviations
        
        # File settings
        self.DATA_DIRECTORY = 'data'
        self.LOG_DIRECTORY = 'logs'
        self.OUTPUT_DIRECTORY = 'output'
        
        # Email settings (for notifications)
        self.SMTP_SERVER = os.getenv('SMTP_SERVER', 'smtp.gmail.com')
        self.SMTP_PORT = int(os.getenv('SMTP_PORT', '587'))
        self.EMAIL_ADDRESS = os.getenv('EMAIL_ADDRESS')
        self.EMAIL_PASSWORD = os.getenv('EMAIL_PASSWORD')
        
        # Create directories if they don't exist
        self.create_directories()
    
    def create_directories(self):
        """Create necessary directories"""
        directories = [
            self.DATA_DIRECTORY,
            self.LOG_DIRECTORY,
            self.OUTPUT_DIRECTORY
        ]
        
        for directory in directories:
            if not os.path.exists(directory):
                os.makedirs(directory)
    
    def get_user_agent(self) -> str:
        """Get user agent string"""
        return 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    
    def get_headers(self) -> Dict[str, str]:
        """Get HTTP headers"""
        return {
            'User-Agent': self.get_user_agent(),
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1'
        }
