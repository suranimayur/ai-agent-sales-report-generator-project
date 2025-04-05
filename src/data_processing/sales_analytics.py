"""Sales Analytics Processing Module using PySpark."""
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import DoubleType, IntegerType
import yaml
import os
from datetime import datetime
from typing import Dict, Any

class SalesAnalytics:
    """Handles sales data analytics using PySpark."""
    
    def __init__(self):
        # Load configuration
        with open('config/config.yaml', 'r') as f:
            self.config = yaml.safe_load(f)
        
        # Initialize Spark session with optimized settings
        self.spark = SparkSession.builder \
            .appName("SalesAnalytics") \
            .config("spark.sql.shuffle.partitions", "8") \
            .config("spark.default.parallelism", "8") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .config("spark.sql.adaptive.advisoryPartitionSizeInBytes", "128MB") \
            .getOrCreate()
        
        # Set log level to WARN to reduce noise
        self.spark.sparkContext.setLogLevel("WARN")
        
        # Create necessary directories
        os.makedirs(self.config['data_paths']['curated'], exist_ok=True)
        os.makedirs(self.config['data_paths']['processed'], exist_ok=True)
        os.makedirs(self.config['data_paths']['reports'], exist_ok=True)

    def read_sales_data(self):
        """Read sales data from CSV file"""
        raw_data_path = os.path.join(self.config['data_paths']['raw'], 'product_sales_data.csv')
        df = self.spark.read.csv(raw_data_path, header=True, inferSchema=True)
        return df

    def process_sales_metrics(self, df):
        """Process various sales metrics with optimized window operations"""
        # Repartition data for better performance
        df = df.repartition(8, "Order_Date")
        
        # Define window specifications with proper partitioning
        date_window = Window.partitionBy("Order_Date").orderBy("Order_Date")
        category_window = Window.partitionBy("Product_Category").orderBy("Order_Date")
        product_window = Window.partitionBy("Product_ID").orderBy("Order_Date")
        customer_window = Window.partitionBy("Customer_ID").orderBy("Order_Date")
        
        # Daily sales metrics
        daily_sales = df.groupBy("Order_Date").agg(
            F.sum("Final_Price").alias("daily_sales"),
            F.sum("Profit").alias("daily_profit"),
            F.countDistinct("Order_ID").alias("daily_orders"),
            F.countDistinct("Customer_ID").alias("daily_customers"),
            F.avg("Discount_Percentage").alias("avg_discount")
        ).orderBy("Order_Date")
        
        # Product category performance
        category_performance = df.groupBy("Product_Category").agg(
            F.sum("Final_Price").alias("category_sales"),
            F.sum("Profit").alias("category_profit"),
            F.count("Order_ID").alias("orders_count"),
            F.avg("Discount_Percentage").alias("avg_discount")
        ).orderBy(F.col("category_sales").desc())
        
        # Customer regional analysis
        regional_analysis = df.groupBy("Customer_Region").agg(
            F.sum("Final_Price").alias("regional_sales"),
            F.countDistinct("Customer_ID").alias("customer_count"),
            F.avg("Final_Price").alias("avg_order_value")
        ).orderBy(F.col("regional_sales").desc())
        
        # Payment method analysis
        payment_analysis = df.groupBy("Payment_Method").agg(
            F.count("Order_ID").alias("transaction_count"),
            F.sum("Final_Price").alias("total_amount"),
            F.avg("Final_Price").alias("avg_transaction_amount")
        ).orderBy(F.col("total_amount").desc())
        
        # Top performing products
        top_products = df.groupBy("Product_ID", "Product_Name").agg(
            F.sum("Final_Price").alias("total_sales"),
            F.sum("Profit").alias("total_profit"),
            F.count("Order_ID").alias("order_count"),
            F.avg("Discount_Percentage").alias("avg_discount")
        ).orderBy(F.col("total_sales").desc()).limit(100)
        
        # Order status analysis
        status_analysis = df.groupBy("Order_Status").agg(
            F.count("Order_ID").alias("order_count"),
            F.sum("Final_Price").alias("total_amount"),
            F.avg("Final_Price").alias("avg_order_amount")
        ).orderBy(F.col("order_count").desc())
        
        # Shipping analysis
        shipping_analysis = df.groupBy("Shipping_Method").agg(
            F.count("Order_ID").alias("shipment_count"),
            F.avg("Shipping_Cost").alias("avg_shipping_cost"),
            F.sum("Final_Price").alias("total_sales")
        ).orderBy(F.col("shipment_count").desc())
        
        return {
            "daily_sales": daily_sales,
            "category_performance": category_performance,
            "regional_analysis": regional_analysis,
            "payment_analysis": payment_analysis,
            "top_products": top_products,
            "status_analysis": status_analysis,
            "shipping_analysis": shipping_analysis
        }

    def save_reports(self, reports):
        """Save reports to curated data folder"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        for report_name, df in reports.items():
            # Save as CSV
            csv_path = os.path.join(
                self.config['data_paths']['curated'],
                f"{report_name}_{timestamp}"
            )
            df.coalesce(1).write.csv(csv_path, header=True, mode="overwrite")
            
            # Save as Parquet
            parquet_path = os.path.join(
                self.config['data_paths']['curated'],
                f"{report_name}_{timestamp}.parquet"
            )
            df.write.parquet(parquet_path, mode="overwrite")
            
            print(f"Saved {report_name} report to {csv_path}")

    def generate_summary_metrics(self, df):
        """Generate summary metrics from sales data"""
        summary = df.agg(
            F.sum("Final_Price").alias("total_sales"),
            F.sum("Profit").alias("total_profit"),
            F.countDistinct("Order_ID").alias("total_orders"),
            F.countDistinct("Customer_ID").alias("unique_customers"),
            F.avg("Discount_Percentage").alias("avg_discount"),
            F.avg("Final_Price").alias("avg_order_value")
        ).collect()[0]
        
        print("\nSummary Metrics:")
        print(f"Total Sales: {summary['total_sales']:,.2f}")
        print(f"Total Profit: {summary['total_profit']:,.2f}")
        print(f"Total Orders: {summary['total_orders']:,.2f}")
        print(f"Unique Customers: {summary['unique_customers']:,.2f}")
        print(f"Average Discount %: {summary['avg_discount']:.2f}")
        print(f"Average Order Value: {summary['avg_order_value']:,.2f}")

    def run_analytics(self):
        """Run the complete analytics pipeline"""
        print("Starting sales analytics processing...")
        
        try:
            # Read data
            df = self.read_sales_data()
            print(f"Data loaded successfully. Total records: {df.count()}")
            
            # Process metrics
            reports = self.process_sales_metrics(df)
            
            # Save reports
            self.save_reports(reports)
            
            # Generate summary metrics
            self.generate_summary_metrics(df)
            
            print("\nAnalytics processing completed successfully!")
            
        except Exception as e:
            print(f"Error during analytics processing: {str(e)}")
            raise
        finally:
            # Stop Spark session
            self.spark.stop()

if __name__ == "__main__":
    analytics = SalesAnalytics()
    analytics.run_analytics() 