"""
Stock Data Web Scraper - Scheduler Module
Handles scheduling and automation of data collection
"""

import schedule
import time
import logging
from datetime import datetime, timedelta
from typing import Callable, List, Optional
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import threading
import json
import os

logger = logging.getLogger(__name__)

class SchedulerManager:
    """Handles scheduling and automation of data collection"""
    
    def __init__(self):
        self.is_running = False
        self.scheduler_thread = None
        self.jobs = []
        self.config = self.load_config()
        
    def load_config(self) -> dict:
        """Load scheduler configuration"""
        config_file = 'scheduler_config.json'
        default_config = {
            'email_notifications': False,
            'smtp_server': 'smtp.gmail.com',
            'smtp_port': 587,
            'email_address': '',
            'email_password': '',
            'default_symbols': ['AAPL', 'GOOGL', 'MSFT', 'AMZN', 'TSLA']
        }
        
        if os.path.exists(config_file):
            try:
                with open(config_file, 'r') as f:
                    return json.load(f)
            except Exception as e:
                logger.error(f"Error loading config: {e}")
                return default_config
        else:
            # Create default config file
            with open(config_file, 'w') as f:
                json.dump(default_config, f, indent=2)
            return default_config
    
    def save_config(self):
        """Save current configuration"""
        try:
            with open('scheduler_config.json', 'w') as f:
                json.dump(self.config, f, indent=2)
        except Exception as e:
            logger.error(f"Error saving config: {e}")
    
    def schedule_daily_collection(self, time_str: str, symbols: List[str] = None):
        """Schedule daily data collection"""
        if symbols is None:
            symbols = self.config['default_symbols']
        
        def job():
            self.collect_and_process_data(symbols)
        
        schedule.every().day.at(time_str).do(job)
        self.jobs.append(f"Daily collection at {time_str}")
        logger.info(f"Scheduled daily collection at {time_str}")
    
    def schedule_market_hours_collection(self, symbols: List[str] = None):
        """Schedule collection during market hours"""
        if symbols is None:
            symbols = self.config['default_symbols']
        
        def job():
            self.collect_and_process_data(symbols)
        
        # Schedule for market open, midday, and close (EST)
        schedule.every().monday.at("09:30").do(job)
        schedule.every().tuesday.at("09:30").do(job)
        schedule.every().wednesday.at("09:30").do(job)
        schedule.every().thursday.at("09:30").do(job)
        schedule.every().friday.at("09:30").do(job)
        
        schedule.every().monday.at("12:00").do(job)
        schedule.every().tuesday.at("12:00").do(job)
        schedule.every().wednesday.at("12:00").do(job)
        schedule.every().thursday.at("12:00").do(job)
        schedule.every().friday.at("12:00").do(job)
        
        schedule.every().monday.at("15:30").do(job)
        schedule.every().tuesday.at("15:30").do(job)
        schedule.every().wednesday.at("15:30").do(job)
        schedule.every().thursday.at("15:30").do(job)
        schedule.every().friday.at("15:30").do(job)
        
        self.jobs.append("Market hours collection (9:30 AM, 12:00 PM, 3:30 PM EST)")
        logger.info("Scheduled market hours collection")
    
    def schedule_hourly_collection(self, symbols: List[str] = None):
        """Schedule hourly data collection"""
        if symbols is None:
            symbols = self.config['default_symbols']
        
        def job():
            self.collect_and_process_data(symbols)
        
        schedule.every().hour.do(job)
        self.jobs.append("Hourly collection")
        logger.info("Scheduled hourly collection")
    
    def collect_and_process_data(self, symbols: List[str]):
        """Collect and process data"""
        try:
            from .scraper import StockScraper
            from .data_processor import DataProcessor
            from .analyzer import StockAnalyzer
            
            logger.info(f"Starting scheduled data collection for {symbols}")
            
            # Initialize components
            scraper = StockScraper()
            processor = DataProcessor()
            analyzer = StockAnalyzer()
            
            # Collect data
            raw_data = scraper.scrape_multiple_stocks(symbols)
            
            if raw_data is not None:
                # Save raw data
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                raw_filename = f"scheduled_raw_data_{timestamp}.csv"
                raw_data.to_csv(raw_filename, index=False)
                
                # Process data
                processed_data = processor.process_data(raw_filename)
                
                if processed_data is not None:
                    # Save processed data
                    processed_filename = f"scheduled_processed_data_{timestamp}.csv"
                    processed_data.to_csv(processed_filename, index=False)
                    
                    # Analyze data
                    analysis_results = analyzer.analyze_data(processed_filename)
                    
                    # Generate visualizations
                    analyzer.generate_visualizations(processed_filename)
                    
                    # Send notification if enabled
                    if self.config['email_notifications']:
                        self.send_notification(symbols, analysis_results)
                    
                    logger.info(f"Scheduled data collection completed successfully")
                else:
                    logger.error("Data processing failed")
            else:
                logger.error("Data collection failed")
                
        except Exception as e:
            logger.error(f"Error in scheduled data collection: {e}")
            if self.config['email_notifications']:
                self.send_error_notification(str(e))
    
    def send_notification(self, symbols: List[str], analysis_results: dict):
        """Send email notification with analysis results"""
        try:
            if not self.config['email_address'] or not self.config['email_password']:
                logger.warning("Email credentials not configured")
                return
            
            # Create email content
            subject = f"Stock Data Analysis - {datetime.now().strftime('%Y-%m-%d %H:%M')}"
            
            body = f"""
            Stock Data Analysis Report
            ==========================
            
            Symbols Analyzed: {', '.join(symbols)}
            Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
            
            """
            
            # Add performance summary
            if 'performance_analysis' in analysis_results:
                perf = analysis_results['performance_analysis']
                body += f"""
                Performance Summary:
                - Average Change: {perf.get('avg_change_percent', 0):.2f}%
                - Positive Stocks: {perf.get('positive_stocks', 0)}
                - Negative Stocks: {perf.get('negative_stocks', 0)}
                
                """
                
                if 'top_gainers' in perf:
                    body += "Top Gainers:\n"
                    for gainer in perf['top_gainers'][:3]:
                        body += f"  {gainer['symbol']}: {gainer['change_percent']:.2f}%\n"
            
            # Send email
            msg = MIMEMultipart()
            msg['From'] = self.config['email_address']
            msg['To'] = self.config['email_address']
            msg['Subject'] = subject
            
            msg.attach(MIMEText(body, 'plain'))
            
            server = smtplib.SMTP(self.config['smtp_server'], self.config['smtp_port'])
            server.starttls()
            server.login(self.config['email_address'], self.config['email_password'])
            server.send_message(msg)
            server.quit()
            
            logger.info("Email notification sent successfully")
            
        except Exception as e:
            logger.error(f"Error sending email notification: {e}")
    
    def send_error_notification(self, error_message: str):
        """Send error notification email"""
        try:
            if not self.config['email_address'] or not self.config['email_password']:
                return
            
            subject = f"Stock Scraper Error - {datetime.now().strftime('%Y-%m-%d %H:%M')}"
            body = f"""
            Stock Data Scraper Error
            =======================
            
            An error occurred during scheduled data collection:
            
            Error: {error_message}
            Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
            
            Please check the logs for more details.
            """
            
            msg = MIMEMultipart()
            msg['From'] = self.config['email_address']
            msg['To'] = self.config['email_address']
            msg['Subject'] = subject
            
            msg.attach(MIMEText(body, 'plain'))
            
            server = smtplib.SMTP(self.config['smtp_server'], self.config['smtp_port'])
            server.starttls()
            server.login(self.config['email_address'], self.config['email_password'])
            server.send_message(msg)
            server.quit()
            
            logger.info("Error notification sent")
            
        except Exception as e:
            logger.error(f"Error sending error notification: {e}")
    
    def start_scheduler(self):
        """Start the scheduler in a separate thread"""
        if self.is_running:
            logger.warning("Scheduler is already running")
            return
        
        self.is_running = True
        
        def run_scheduler():
            while self.is_running:
                schedule.run_pending()
                time.sleep(60)  # Check every minute
        
        self.scheduler_thread = threading.Thread(target=run_scheduler)
        self.scheduler_thread.daemon = True
        self.scheduler_thread.start()
        
        logger.info("Scheduler started")
    
    def stop_scheduler(self):
        """Stop the scheduler"""
        self.is_running = False
        if self.scheduler_thread:
            self.scheduler_thread.join()
        
        logger.info("Scheduler stopped")
    
    def clear_jobs(self):
        """Clear all scheduled jobs"""
        schedule.clear()
        self.jobs = []
        logger.info("All scheduled jobs cleared")
    
    def get_job_status(self) -> dict:
        """Get current job status"""
        return {
            'is_running': self.is_running,
            'jobs': self.jobs,
            'next_run': str(schedule.next_run()) if schedule.jobs else None
        }
    
    def setup_scheduler_interactive(self):
        """Interactive scheduler setup"""
        print("\n--- Schedule Data Collection ---")
        print("1. Daily collection")
        print("2. Market hours collection")
        print("3. Hourly collection")
        print("4. Custom schedule")
        print("5. View current schedule")
        print("6. Clear all jobs")
        print("7. Start/Stop scheduler")
        print("8. Configure email notifications")
        
        choice = input("\nSelect an option (1-8): ").strip()
        
        if choice == '1':
            self.setup_daily_collection()
        elif choice == '2':
            self.setup_market_hours_collection()
        elif choice == '3':
            self.setup_hourly_collection()
        elif choice == '4':
            self.setup_custom_schedule()
        elif choice == '5':
            self.view_current_schedule()
        elif choice == '6':
            self.clear_jobs()
            print("All jobs cleared.")
        elif choice == '7':
            self.toggle_scheduler()
        elif choice == '8':
            self.configure_email_notifications()
        else:
            print("Invalid choice")
    
    def setup_daily_collection(self):
        """Setup daily collection interactively"""
        time_str = input("Enter time for daily collection (HH:MM, e.g., 09:30): ").strip()
        
        if not time_str:
            print("No time provided.")
            return
        
        symbols_input = input("Enter symbols (comma-separated) or press Enter for default: ").strip()
        symbols = None
        if symbols_input:
            symbols = [s.strip().upper() for s in symbols_input.split(',')]
        
        self.schedule_daily_collection(time_str, symbols)
        print(f"Daily collection scheduled at {time_str}")
    
    def setup_market_hours_collection(self):
        """Setup market hours collection interactively"""
        symbols_input = input("Enter symbols (comma-separated) or press Enter for default: ").strip()
        symbols = None
        if symbols_input:
            symbols = [s.strip().upper() for s in symbols_input.split(',')]
        
        self.schedule_market_hours_collection(symbols)
        print("Market hours collection scheduled (9:30 AM, 12:00 PM, 3:30 PM EST)")
    
    def setup_hourly_collection(self):
        """Setup hourly collection interactively"""
        symbols_input = input("Enter symbols (comma-separated) or press Enter for default: ").strip()
        symbols = None
        if symbols_input:
            symbols = [s.strip().upper() for s in symbols_input.split(',')]
        
        self.schedule_hourly_collection(symbols)
        print("Hourly collection scheduled")
    
    def setup_custom_schedule(self):
        """Setup custom schedule interactively"""
        print("Custom schedule setup is available for extension.")
        print("You can add specific scheduling logic here.")
    
    def view_current_schedule(self):
        """View current schedule"""
        status = self.get_job_status()
        print(f"\nScheduler Status: {'Running' if status['is_running'] else 'Stopped'}")
        print(f"Active Jobs: {len(status['jobs'])}")
        
        if status['jobs']:
            print("\nScheduled Jobs:")
            for job in status['jobs']:
                print(f"  - {job}")
        
        if status['next_run']:
            print(f"\nNext Run: {status['next_run']}")
    
    def toggle_scheduler(self):
        """Toggle scheduler on/off"""
        if self.is_running:
            self.stop_scheduler()
            print("Scheduler stopped.")
        else:
            self.start_scheduler()
            print("Scheduler started.")
    
    def configure_email_notifications(self):
        """Configure email notifications"""
        print("\n--- Email Notification Configuration ---")
        
        enable = input("Enable email notifications? (y/n): ").strip().lower()
        self.config['email_notifications'] = enable == 'y'
        
        if self.config['email_notifications']:
            email = input("Enter email address: ").strip()
            password = input("Enter email password: ").strip()
            
            self.config['email_address'] = email
            self.config['email_password'] = password
            
            print("Email notifications configured.")
        else:
            print("Email notifications disabled.")
        
        self.save_config()
