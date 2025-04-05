import pandas as pd
import numpy as np
from faker import Faker
from datetime import datetime, timedelta
import random
import os

# Initialize Faker
fake = Faker()

# Define product categories and subcategories
PRODUCT_CATEGORIES = {
    'Electronics': ['Smartphones', 'Laptops', 'Tablets', 'Accessories'],
    'Clothing': ['Men', 'Women', 'Kids', 'Accessories'],
    'Home & Kitchen': ['Furniture', 'Appliances', 'Cookware', 'Decor'],
    'Sports': ['Fitness', 'Outdoor', 'Team Sports', 'Equipment'],
    'Beauty': ['Skincare', 'Makeup', 'Haircare', 'Fragrances']
}

# Define regions and their corresponding cities
REGIONS = {
    'North America': ['New York', 'Los Angeles', 'Chicago', 'Toronto', 'Vancouver'],
    'Europe': ['London', 'Paris', 'Berlin', 'Madrid', 'Rome'],
    'Asia': ['Tokyo', 'Shanghai', 'Singapore', 'Seoul', 'Mumbai'],
    'Oceania': ['Sydney', 'Melbourne', 'Auckland', 'Brisbane', 'Perth']
}

# Generate product data
def generate_products(num_products=1000):
    products = []
    for i in range(num_products):
        category = random.choice(list(PRODUCT_CATEGORIES.keys()))
        subcategory = random.choice(PRODUCT_CATEGORIES[category])
        base_price = random.uniform(10, 1000)
        products.append({
            'Product_ID': f'PROD-{i:06d}',
            'Product_Name': fake.catch_phrase(),
            'Product_Category': category,
            'Product_Subcategory': subcategory,
            'Unit_Price': round(base_price, 2),
            'Cost_Price': round(base_price * random.uniform(0.4, 0.7), 2),
            'Stock_Quantity': random.randint(10, 1000)
        })
    return pd.DataFrame(products)

# Generate customer data
def generate_customers(num_customers=10000):
    customers = []
    for i in range(num_customers):
        region = random.choice(list(REGIONS.keys()))
        city = random.choice(REGIONS[region])
        customers.append({
            'Customer_ID': f'CUST-{i:06d}',
            'Customer_Name': fake.name(),
            'Customer_Email': fake.email(),
            'Customer_Phone': fake.phone_number(),
            'Customer_Region': region,
            'Customer_City': city
        })
    return pd.DataFrame(customers)

# Generate sales data
def generate_sales_data(num_records=1000000):
    # Generate base data
    products = generate_products()
    customers = generate_customers()
    
    # Generate sales records
    sales_data = []
    start_date = datetime(2023, 1, 1)
    end_date = datetime(2024, 12, 31)
    
    for i in range(num_records):
        # Select random product and customer
        product = products.sample(1).iloc[0]
        customer = customers.sample(1).iloc[0]
        
        # Generate order date and time
        order_date = fake.date_time_between(start_date=start_date, end_date=end_date)
        
        # Generate order details
        quantity = random.randint(1, 10)
        unit_price = product['Unit_Price']
        cost_price = product['Cost_Price']
        total_price = quantity * unit_price
        total_cost = quantity * cost_price
        
        # Generate discount based on quantity and customer region
        base_discount = random.uniform(0, 20)
        quantity_discount = min(quantity - 1, 10)  # Additional 1% per item over 1
        region_discount = 5 if customer['Customer_Region'] == 'Europe' else 0
        discount_percentage = round(base_discount + quantity_discount + region_discount, 2)
        discount_amount = round(total_price * (discount_percentage / 100), 2)
        final_price = round(total_price - discount_amount, 2)
        
        # Calculate profit
        profit = round(final_price - total_cost, 2)
        
        # Generate shipping details
        shipping_method = random.choice(['Standard', 'Express', 'Next Day'])
        shipping_cost = round(random.uniform(5, 20) if shipping_method == 'Standard' else 
                            random.uniform(15, 30) if shipping_method == 'Express' else 
                            random.uniform(25, 40), 2)
        
        # Generate order status with realistic probabilities
        status_weights = {'Completed': 0.7, 'Pending': 0.1, 'Cancelled': 0.1, 'Delivered': 0.1}
        order_status = random.choices(list(status_weights.keys()), weights=list(status_weights.values()))[0]
        
        # Generate payment method with realistic probabilities
        payment_weights = {'Credit Card': 0.5, 'Debit Card': 0.2, 'PayPal': 0.2, 'Bank Transfer': 0.05, 'Cash': 0.05}
        payment_method = random.choices(list(payment_weights.keys()), weights=list(payment_weights.values()))[0]
        
        sales_data.append({
            'Order_ID': f'ORD-{i:08d}',
            'Order_Date': order_date,
            'Customer_ID': customer['Customer_ID'],
            'Customer_Name': customer['Customer_Name'],
            'Customer_Email': customer['Customer_Email'],
            'Customer_Region': customer['Customer_Region'],
            'Customer_City': customer['Customer_City'],
            'Product_ID': product['Product_ID'],
            'Product_Name': product['Product_Name'],
            'Product_Category': product['Product_Category'],
            'Product_Subcategory': product['Product_Subcategory'],
            'Quantity': quantity,
            'Unit_Price': unit_price,
            'Total_Price': total_price,
            'Discount_Percentage': discount_percentage,
            'Discount_Amount': discount_amount,
            'Final_Price': final_price,
            'Cost_Price': cost_price,
            'Total_Cost': total_cost,
            'Profit': profit,
            'Payment_Method': payment_method,
            'Shipping_Method': shipping_method,
            'Shipping_Cost': shipping_cost,
            'Order_Status': order_status
        })
        
        # Print progress every 100,000 records
        if (i + 1) % 100000 == 0:
            print(f"Generated {i + 1} records...")
    
    return pd.DataFrame(sales_data)

def main():
    print("Starting data generation...")
    
    # Create data directory if it doesn't exist
    os.makedirs('data', exist_ok=True)
    
    # Generate and save sales data
    sales_data = generate_sales_data()
    output_file = "data/product_sales_data.csv"
    sales_data.to_csv(output_file, index=False)
    
    print(f"\nGenerated {len(sales_data)} records with {len(sales_data.columns)} columns")
    print(f"Data saved to {output_file}")
    
    # Print sample of the data
    print("\nSample of the generated data:")
    print(sales_data.head())
    
    # Print data summary
    print("\nData Summary:")
    print(f"Total Sales: ${sales_data['Final_Price'].sum():,.2f}")
    print(f"Average Order Value: ${sales_data['Final_Price'].mean():,.2f}")
    print(f"Total Profit: ${sales_data['Profit'].sum():,.2f}")
    print(f"Number of Unique Customers: {sales_data['Customer_ID'].nunique()}")
    print(f"Number of Unique Products: {sales_data['Product_ID'].nunique()}")

if __name__ == "__main__":
    main() 