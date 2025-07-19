"""
Stock Data Web Scraper - Analysis Module
Handles statistical analysis and data visualization
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler
from sklearn.decomposition import PCA
import warnings
import logging
from typing import Dict, List, Optional, Tuple
from datetime import datetime
import os

warnings.filterwarnings('ignore')
logger = logging.getLogger(__name__)

class StockAnalyzer:
    """Statistical analysis and visualization class for stock data"""
    
    def __init__(self):
        self.setup_matplotlib_style()
        
    def setup_matplotlib_style(self):
        """Setup matplotlib styling"""
        plt.style.use('seaborn-v0_8')
        plt.rcParams['figure.figsize'] = (12, 8)
        plt.rcParams['font.size'] = 10
        plt.rcParams['axes.titlesize'] = 14
        plt.rcParams['axes.labelsize'] = 12
        plt.rcParams['xtick.labelsize'] = 10
        plt.rcParams['ytick.labelsize'] = 10
        plt.rcParams['legend.fontsize'] = 10
        
    def analyze_data(self, input_file: str) -> Optional[Dict]:
        """Perform comprehensive analysis on stock data"""
        try:
            logger.info(f"Starting analysis of {input_file}")
            
            # Load processed data
            df = pd.read_csv(input_file)
            
            if df.empty:
                logger.warning("Input file is empty")
                return None
            
            logger.info(f"Loaded {len(df)} records for analysis")
            
            # Perform various analyses
            analysis_results = {}
            
            # Basic statistics
            analysis_results['descriptive_stats'] = self.calculate_descriptive_statistics(df)
            
            # Performance analysis
            analysis_results['performance_analysis'] = self.analyze_performance(df)
            
            # Correlation analysis
            analysis_results['correlation_analysis'] = self.analyze_correlations(df)
            
            # Clustering analysis
            analysis_results['clustering_analysis'] = self.perform_clustering(df)
            
            # Market analysis
            analysis_results['market_analysis'] = self.analyze_market_segments(df)
            
            # Risk analysis
            analysis_results['risk_analysis'] = self.analyze_risk_metrics(df)
            
            # Save analysis results
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            results_file = f"analysis_results_{timestamp}.csv"
            
            # Create summary dataframe
            summary_df = self.create_analysis_summary(df, analysis_results)
            summary_df.to_csv(results_file, index=False)
            
            logger.info(f"Analysis completed and saved to {results_file}")
            
            return analysis_results
            
        except Exception as e:
            logger.error(f"Error during analysis: {e}")
            return None
    
    def calculate_descriptive_statistics(self, df: pd.DataFrame) -> Dict:
        """Calculate descriptive statistics"""
        logger.info("Calculating descriptive statistics")
        
        numeric_cols = df.select_dtypes(include=[np.number]).columns
        stats = {}
        
        for col in numeric_cols:
            if col in df.columns and df[col].notna().sum() > 0:
                stats[col] = {
                    'mean': df[col].mean(),
                    'median': df[col].median(),
                    'std': df[col].std(),
                    'min': df[col].min(),
                    'max': df[col].max(),
                    'q25': df[col].quantile(0.25),
                    'q75': df[col].quantile(0.75),
                    'skewness': df[col].skew(),
                    'kurtosis': df[col].kurtosis()
                }
        
        return stats
    
    def analyze_performance(self, df: pd.DataFrame) -> Dict:
        """Analyze stock performance"""
        logger.info("Analyzing stock performance")
        
        performance = {}
        
        if 'change_percent' in df.columns:
            # Top performers
            performance['top_gainers'] = df.nlargest(5, 'change_percent')[['symbol', 'change_percent', 'current_price']].to_dict('records')
            performance['top_losers'] = df.nsmallest(5, 'change_percent')[['symbol', 'change_percent', 'current_price']].to_dict('records')
            
            # Performance distribution
            performance['positive_stocks'] = len(df[df['change_percent'] > 0])
            performance['negative_stocks'] = len(df[df['change_percent'] < 0])
            performance['neutral_stocks'] = len(df[df['change_percent'] == 0])
            
            # Average performance
            performance['avg_change_percent'] = df['change_percent'].mean()
            performance['median_change_percent'] = df['change_percent'].median()
        
        if 'volume' in df.columns:
            # Volume analysis
            performance['high_volume_stocks'] = df.nlargest(5, 'volume')[['symbol', 'volume', 'current_price']].to_dict('records')
            performance['avg_volume'] = df['volume'].mean()
        
        return performance
    
    def analyze_correlations(self, df: pd.DataFrame) -> Dict:
        """Analyze correlations between variables"""
        logger.info("Analyzing correlations")
        
        numeric_cols = df.select_dtypes(include=[np.number]).columns
        correlation_matrix = df[numeric_cols].corr()
        
        correlations = {}
        correlations['correlation_matrix'] = correlation_matrix.to_dict()
        
        # Find strong correlations
        strong_correlations = []
        for i in range(len(correlation_matrix.columns)):
            for j in range(i+1, len(correlation_matrix.columns)):
                corr_value = correlation_matrix.iloc[i, j]
                if abs(corr_value) > 0.7:  # Strong correlation threshold
                    strong_correlations.append({
                        'variable1': correlation_matrix.columns[i],
                        'variable2': correlation_matrix.columns[j],
                        'correlation': corr_value
                    })
        
        correlations['strong_correlations'] = strong_correlations
        
        return correlations
    
    def perform_clustering(self, df: pd.DataFrame) -> Dict:
        """Perform clustering analysis"""
        logger.info("Performing clustering analysis")
        
        # Select numeric columns for clustering
        numeric_cols = ['current_price', 'change_percent', 'volume']
        available_cols = [col for col in numeric_cols if col in df.columns]
        
        if len(available_cols) < 2:
            logger.warning("Insufficient numeric columns for clustering")
            return {'error': 'Insufficient data for clustering'}
        
        # Prepare data for clustering
        cluster_data = df[available_cols].dropna()
        
        if len(cluster_data) < 5:
            logger.warning("Insufficient data points for clustering")
            return {'error': 'Insufficient data points for clustering'}
        
        # Standardize data
        scaler = StandardScaler()
        scaled_data = scaler.fit_transform(cluster_data)
        
        # Perform K-means clustering
        kmeans = KMeans(n_clusters=3, random_state=42, n_init=10)
        cluster_labels = kmeans.fit_predict(scaled_data)
        
        # Add cluster labels to original data
        cluster_df = cluster_data.copy()
        cluster_df['cluster'] = cluster_labels
        cluster_df['symbol'] = df.loc[cluster_data.index, 'symbol']
        
        # Analyze clusters
        cluster_analysis = {}
        for cluster_id in range(3):
            cluster_stocks = cluster_df[cluster_df['cluster'] == cluster_id]
            cluster_analysis[f'cluster_{cluster_id}'] = {
                'count': len(cluster_stocks),
                'stocks': cluster_stocks['symbol'].tolist(),
                'characteristics': {
                    'avg_price': cluster_stocks['current_price'].mean() if 'current_price' in cluster_stocks.columns else None,
                    'avg_change': cluster_stocks['change_percent'].mean() if 'change_percent' in cluster_stocks.columns else None,
                    'avg_volume': cluster_stocks['volume'].mean() if 'volume' in cluster_stocks.columns else None
                }
            }
        
        return cluster_analysis
    
    def analyze_market_segments(self, df: pd.DataFrame) -> Dict:
        """Analyze market segments"""
        logger.info("Analyzing market segments")
        
        segments = {}
        
        # Market cap analysis
        if 'market_cap_category' in df.columns:
            segments['market_cap_distribution'] = df['market_cap_category'].value_counts().to_dict()
        
        # Performance category analysis
        if 'performance_category' in df.columns:
            segments['performance_distribution'] = df['performance_category'].value_counts().to_dict()
        
        # Price range analysis
        if 'current_price' in df.columns:
            price_ranges = pd.cut(df['current_price'], bins=5, labels=['Very Low', 'Low', 'Medium', 'High', 'Very High'])
            segments['price_range_distribution'] = price_ranges.value_counts().to_dict()
        
        return segments
    
    def analyze_risk_metrics(self, df: pd.DataFrame) -> Dict:
        """Analyze risk metrics"""
        logger.info("Analyzing risk metrics")
        
        risk_metrics = {}
        
        if 'change_percent' in df.columns:
            # Volatility analysis
            risk_metrics['volatility'] = df['change_percent'].std()
            risk_metrics['var_95'] = df['change_percent'].quantile(0.05)  # Value at Risk (95%)
            risk_metrics['max_drawdown'] = df['change_percent'].min()
            
            # Risk categorization
            high_risk_stocks = df[df['change_percent'].abs() > df['change_percent'].std() * 2]
            risk_metrics['high_risk_stocks'] = high_risk_stocks[['symbol', 'change_percent']].to_dict('records')
        
        return risk_metrics
    
    def create_analysis_summary(self, df: pd.DataFrame, analysis_results: Dict) -> pd.DataFrame:
        """Create summary dataframe from analysis results"""
        summary_data = []
        
        for index, row in df.iterrows():
            summary_row = {
                'symbol': row.get('symbol', ''),
                'current_price': row.get('current_price', 0),
                'change_percent': row.get('change_percent', 0),
                'volume': row.get('volume', 0),
                'market_cap_category': row.get('market_cap_category', ''),
                'performance_category': row.get('performance_category', ''),
                'timestamp': row.get('timestamp', '')
            }
            summary_data.append(summary_row)
        
        return pd.DataFrame(summary_data)
    
    def generate_visualizations(self, input_file: str) -> bool:
        """Generate comprehensive visualizations"""
        try:
            logger.info(f"Generating visualizations for {input_file}")
            
            df = pd.read_csv(input_file)
            
            if df.empty:
                logger.warning("Input file is empty")
                return False
            
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            
            # Create visualizations
            self.create_price_distribution_chart(df, timestamp)
            self.create_performance_chart(df, timestamp)
            self.create_correlation_heatmap(df, timestamp)
            self.create_volume_analysis_chart(df, timestamp)
            self.create_market_cap_analysis(df, timestamp)
            
            logger.info("All visualizations generated successfully")
            return True
            
        except Exception as e:
            logger.error(f"Error generating visualizations: {e}")
            return False
    
    def create_price_distribution_chart(self, df: pd.DataFrame, timestamp: str):
        """Create price distribution visualization"""
        if 'current_price' not in df.columns:
            return
        
        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(15, 6))
        
        # Histogram
        ax1.hist(df['current_price'].dropna(), bins=20, alpha=0.7, edgecolor='black')
        ax1.set_title('Stock Price Distribution')
        ax1.set_xlabel('Current Price ($)')
        ax1.set_ylabel('Frequency')
        ax1.grid(True, alpha=0.3)
        
        # Box plot
        ax2.boxplot(df['current_price'].dropna())
        ax2.set_title('Stock Price Box Plot')
        ax2.set_ylabel('Current Price ($)')
        ax2.grid(True, alpha=0.3)
        
        plt.tight_layout()
        plt.savefig(f'price_distribution_{timestamp}.png', dpi=300, bbox_inches='tight')
        plt.close()
        
        logger.info(f"Price distribution chart saved as price_distribution_{timestamp}.png")
    
    def create_performance_chart(self, df: pd.DataFrame, timestamp: str):
        """Create performance analysis chart"""
        if 'change_percent' not in df.columns:
            return
        
        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(15, 6))
        
        # Performance histogram
        ax1.hist(df['change_percent'].dropna(), bins=20, alpha=0.7, edgecolor='black', 
                color='green' if df['change_percent'].mean() > 0 else 'red')
        ax1.axvline(x=0, color='black', linestyle='--', alpha=0.5)
        ax1.set_title('Stock Performance Distribution')
        ax1.set_xlabel('Change Percentage (%)')
        ax1.set_ylabel('Frequency')
        ax1.grid(True, alpha=0.3)
        
        # Top performers bar chart
        top_performers = df.nlargest(10, 'change_percent')
        ax2.barh(top_performers['symbol'], top_performers['change_percent'], 
                color='green' if top_performers['change_percent'].iloc[0] > 0 else 'red')
        ax2.set_title('Top 10 Performers')
        ax2.set_xlabel('Change Percentage (%)')
        ax2.grid(True, alpha=0.3)
        
        plt.tight_layout()
        plt.savefig(f'performance_analysis_{timestamp}.png', dpi=300, bbox_inches='tight')
        plt.close()
        
        logger.info(f"Performance chart saved as performance_analysis_{timestamp}.png")
    
    def create_correlation_heatmap(self, df: pd.DataFrame, timestamp: str):
        """Create correlation heatmap"""
        numeric_cols = df.select_dtypes(include=[np.number]).columns
        
        if len(numeric_cols) < 2:
            return
        
        correlation_matrix = df[numeric_cols].corr()
        
        plt.figure(figsize=(12, 10))
        sns.heatmap(correlation_matrix, annot=True, cmap='coolwarm', center=0,
                   square=True, fmt='.2f', cbar_kws={'shrink': 0.8})
        plt.title('Stock Data Correlation Matrix')
        plt.tight_layout()
        plt.savefig(f'correlation_heatmap_{timestamp}.png', dpi=300, bbox_inches='tight')
        plt.close()
        
        logger.info(f"Correlation heatmap saved as correlation_heatmap_{timestamp}.png")
    
    def create_volume_analysis_chart(self, df: pd.DataFrame, timestamp: str):
        """Create volume analysis chart"""
        if 'volume' not in df.columns or 'current_price' not in df.columns:
            return
        
        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(15, 6))
        
        # Volume vs Price scatter
        ax1.scatter(df['volume'], df['current_price'], alpha=0.6)
        ax1.set_xlabel('Volume')
        ax1.set_ylabel('Current Price ($)')
        ax1.set_title('Volume vs Price Relationship')
        ax1.grid(True, alpha=0.3)
        
        # Volume distribution
        ax2.hist(df['volume'].dropna(), bins=20, alpha=0.7, edgecolor='black')
        ax2.set_xlabel('Volume')
        ax2.set_ylabel('Frequency')
        ax2.set_title('Trading Volume Distribution')
        ax2.grid(True, alpha=0.3)
        
        plt.tight_layout()
        plt.savefig(f'volume_analysis_{timestamp}.png', dpi=300, bbox_inches='tight')
        plt.close()
        
        logger.info(f"Volume analysis chart saved as volume_analysis_{timestamp}.png")
    
    def create_market_cap_analysis(self, df: pd.DataFrame, timestamp: str):
        """Create market cap analysis chart"""
        if 'market_cap_category' not in df.columns:
            return
        
        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(15, 6))
        
        # Market cap distribution pie chart
        market_cap_counts = df['market_cap_category'].value_counts()
        ax1.pie(market_cap_counts.values, labels=market_cap_counts.index, autopct='%1.1f%%')
        ax1.set_title('Market Cap Distribution')
        
        # Market cap vs performance
        if 'change_percent' in df.columns:
            market_cap_performance = df.groupby('market_cap_category')['change_percent'].mean()
            ax2.bar(market_cap_performance.index, market_cap_performance.values)
            ax2.set_title('Average Performance by Market Cap')
            ax2.set_xlabel('Market Cap Category')
            ax2.set_ylabel('Average Change (%)')
            ax2.tick_params(axis='x', rotation=45)
        
        plt.tight_layout()
        plt.savefig(f'market_cap_analysis_{timestamp}.png', dpi=300, bbox_inches='tight')
        plt.close()
        
        logger.info(f"Market cap analysis chart saved as market_cap_analysis_{timestamp}.png")
    
    def generate_analysis_report(self, analysis_results: Dict) -> str:
        """Generate text analysis report"""
        report = []
        report.append("STOCK DATA ANALYSIS REPORT")
        report.append("=" * 50)
        report.append(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        report.append("")
        
        # Performance Analysis
        if 'performance_analysis' in analysis_results:
            perf = analysis_results['performance_analysis']
            report.append("PERFORMANCE ANALYSIS")
            report.append("-" * 20)
            
            if 'top_gainers' in perf:
                report.append("Top Gainers:")
                for gainer in perf['top_gainers']:
                    report.append(f"  {gainer['symbol']}: {gainer['change_percent']:.2f}%")
            
            if 'top_losers' in perf:
                report.append("\nTop Losers:")
                for loser in perf['top_losers']:
                    report.append(f"  {loser['symbol']}: {loser['change_percent']:.2f}%")
            
            report.append("")
        
        # Market Analysis
        if 'market_analysis' in analysis_results:
            market = analysis_results['market_analysis']
            report.append("MARKET ANALYSIS")
            report.append("-" * 15)
            
            if 'market_cap_distribution' in market:
                report.append("Market Cap Distribution:")
                for cap_type, count in market['market_cap_distribution'].items():
                    report.append(f"  {cap_type}: {count} stocks")
            
            report.append("")
        
        return "\n".join(report)
    
    def print_analysis_summary(self, analysis_results: Dict):
        """Print analysis summary to console"""
        print("\n" + "="*60)
        print("             STOCK DATA ANALYSIS SUMMARY")
        print("="*60)
        
        # Performance Summary
        if 'performance_analysis' in analysis_results:
            perf = analysis_results['performance_analysis']
            print("\n📊 PERFORMANCE ANALYSIS")
            print("-" * 25)
            
            if 'avg_change_percent' in perf:
                avg_change = perf['avg_change_percent']
                print(f"Average Change: {avg_change:.2f}%")
                print(f"Market Sentiment: {'Positive' if avg_change > 0 else 'Negative'}")
            
            if 'positive_stocks' in perf:
                print(f"Positive Stocks: {perf['positive_stocks']}")
                print(f"Negative Stocks: {perf['negative_stocks']}")
        
        # Risk Analysis
        if 'risk_analysis' in analysis_results:
            risk = analysis_results['risk_analysis']
            print("\n⚠️  RISK ANALYSIS")
            print("-" * 15)
            
            if 'volatility' in risk:
                print(f"Market Volatility: {risk['volatility']:.2f}%")
            
            if 'high_risk_stocks' in risk:
                print(f"High Risk Stocks: {len(risk['high_risk_stocks'])}")
        
        print("\n" + "="*60)
    
    def analyze_data_interactive(self):
        """Interactive data analysis"""
        print("\n--- Data Analysis ---")
        
        # Get input file
        filename = input("Enter the processed data file name: ").strip()
        
        if not filename:
            print("No filename provided.")
            return
        
        if not os.path.exists(filename):
            print(f"File '{filename}' not found.")
            return
        
        try:
            # Perform analysis
            analysis_results = self.analyze_data(filename)
            
            if analysis_results:
                # Print summary
                self.print_analysis_summary(analysis_results)
                
                # Ask if user wants detailed report
                detailed = input("\nGenerate detailed report? (y/n): ").strip().lower()
                if detailed == 'y':
                    report = self.generate_analysis_report(analysis_results)
                    print("\n" + report)
                
            else:
                print("Analysis failed. Please check the file and try again.")
                
        except Exception as e:
            print(f"Error during analysis: {e}")
    
    def generate_visualizations_interactive(self):
        """Interactive visualization generation"""
        print("\n--- Generate Visualizations ---")
        
        # Get input file
        filename = input("Enter the processed data file name: ").strip()
        
        if not filename:
            print("No filename provided.")
            return
        
        if not os.path.exists(filename):
            print(f"File '{filename}' not found.")
            return
        
        try:
            success = self.generate_visualizations(filename)
            
            if success:
                print("\n✅ Visualizations generated successfully!")
                print("Generated files:")
                print("- price_distribution_[timestamp].png")
                print("- performance_analysis_[timestamp].png")
                print("- correlation_heatmap_[timestamp].png")
                print("- volume_analysis_[timestamp].png")
                print("- market_cap_analysis_[timestamp].png")
            else:
                print("❌ Visualization generation failed.")
                
        except Exception as e:
            print(f"Error generating visualizations: {e}")
