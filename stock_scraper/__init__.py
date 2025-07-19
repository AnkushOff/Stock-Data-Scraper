"""
Stock Data Web Scraper Package
A comprehensive web scraping tool for stock market data analysis
"""

__version__ = "1.0.0"
__author__ = "Stock Scraper Team"
__email__ = "contact@stockscraper.com"

from .scraper import StockScraper
from .data_processor import DataProcessor
from .analyzer import StockAnalyzer
from .scheduler import SchedulerManager
from .config import Config

__all__ = [
    'StockScraper',
    'DataProcessor',
    'StockAnalyzer',
    'SchedulerManager',
    'Config'
]
