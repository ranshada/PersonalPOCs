import pandas as pd
import numpy as np
from datetime import datetime, timedelta

# Create a comprehensive sample dataset - E-commerce Sales Data
np.random.seed(42)

# Generate sample data
n_records = 1000
dates = pd.date_range('2023-01-01', '2024-12-31', freq='D')
products = ['Laptop', 'Phone', 'Tablet', 'Headphones', 'Monitor', 'Keyboard', 'Mouse', 'Speaker']
categories = ['Electronics', 'Electronics', 'Electronics', 'Audio', 'Electronics', 'Accessories', 'Accessories', 'Audio']
regions = ['North', 'South', 'East', 'West', 'Central']
customers = ['Premium', 'Standard', 'Basic']

data = {
    'Order_ID': [f'ORD_{i:05d}' for i in range(1, n_records + 1)],
    'Date': np.random.choice(dates, n_records),
    'Product': np.random.choice(products, n_records),
    'Category': [categories[products.index(p)] for p in np.random.choice(products, n_records)],
    'Quantity': np.random.randint(1, 10, n_records),
    'Unit_Price': np.random.randint(50, 2000, n_records),
    'Customer_Type': np.random.choice(customers, n_records),
    'Region': np.random.choice(regions, n_records),
    'Sales_Rep': [f'Rep_{np.random.randint(1, 21):02d}' for _ in range(n_records)],
    'Discount': np.random.choice([0, 5, 10, 15, 20], n_records, p=[0.4, 0.2, 0.2, 0.1, 0.1])
}

# Create DataFrame
df = pd.DataFrame(data)
df['Total_Amount'] = df['Quantity'] * df['Unit_Price'] * (1 - df['Discount']/100)
df['Month'] = df['Date'].dt.month
df['Year'] = df['Date'].dt.year

print("Sample E-commerce Dataset Created!")
print(f"Shape: {df.shape}")
print("\nFirst 5 rows:")
print(df.head())

print("\nDataset Info:")
print(df.info())

# ============================================================================
# FILTERING EXERCISES
# ============================================================================

print("\n" + "="*60)
print("FILTERING EXERCISES")
print("="*60)

print("\n--- EXERCISE 1: Basic Filtering ---")
print("Task: Find all orders with Total_Amount > 5000")
print("Your code here:")
print("# Solution:")
print("high_value_orders = df[df['Total_Amount'] > 5000]")
print("print(f'High value orders: {len(high_value_orders)}')")

print("\n--- EXERCISE 2: Multiple Conditions ---")
print("Task: Find Premium customers in North region with Quantity > 5")
print("Your code here:")
print("# Solution:")
print("premium_north_bulk = df[(df['Customer_Type'] == 'Premium') & (df['Region'] == 'North') & (df['Quantity'] > 5)]")

print("\n--- EXERCISE 3: String Operations ---")
print("Task: Find all products containing 'Phone' in their name")
print("Your code here:")
print("# Solution:")
print("phone_products = df[df['Product'].str.contains('Phone')]")

print("\n--- EXERCISE 4: Date Filtering ---")
print("Task: Find all orders from 2024")
print("Your code here:")
print("# Solution:")
print("orders_2024 = df[df['Year'] == 2024]")

print("\n--- EXERCISE 5: Complex Filtering ---")
print("Task: Find Electronics orders with discount >= 10% OR Total_Amount > 8000")
print("Your code here:")
print("# Solution:")
print("complex_filter = df[((df['Category'] == 'Electronics') & (df['Discount'] >= 10)) | (df['Total_Amount'] > 8000)]")

# ============================================================================
# SORTING EXERCISES
# ============================================================================

print("\n" + "="*60)
print("SORTING EXERCISES")
print("="*60)

print("\n--- EXERCISE 6: Basic Sorting ---")
print("Task: Sort by Total_Amount in descending order")
print("Your code here:")
print("# Solution:")
print("sorted_by_amount = df.sort_values('Total_Amount', ascending=False)")

print("\n--- EXERCISE 7: Multiple Column Sorting ---")
print("Task: Sort by Region (ascending) then by Total_Amount (descending)")
print("Your code here:")
print("# Solution:")
print("multi_sort = df.sort_values(['Region', 'Total_Amount'], ascending=[True, False])")

print("\n--- EXERCISE 8: Date Sorting ---")
print("Task: Sort by Date to get chronological order")
print("Your code here:")
print("# Solution:")
print("chronological = df.sort_values('Date')")

# ============================================================================
# GROUPING EXERCISES
# ============================================================================

print("\n" + "="*60)
print("GROUPING EXERCISES")
print("="*60)

print("\n--- EXERCISE 9: Basic Grouping ---")
print("Task: Calculate average Total_Amount by Region")
print("Your code here:")
print("# Solution:")
print("avg_by_region = df.groupby('Region')['Total_Amount'].mean()")

print("\n--- EXERCISE 10: Multiple Aggregations ---")
print("Task: For each Product, calculate sum, mean, and count of Total_Amount")
print("Your code here:")
print("# Solution:")
print("product_stats = df.groupby('Product')['Total_Amount'].agg(['sum', 'mean', 'count'])")

print("\n--- EXERCISE 11: Multiple Column Grouping ---")
print("Task: Group by Customer_Type and Region, calculate total sales")
print("Your code here:")
print("# Solution:")
print("customer_region_sales = df.groupby(['Customer_Type', 'Region'])['Total_Amount'].sum()")

print("\n--- EXERCISE 12: Complex Aggregation ---")
print("Task: For each Category, calculate multiple metrics")
print("Your code here:")
print("# Solution:")
print("""category_analysis = df.groupby('Category').agg({
    'Total_Amount': ['sum', 'mean', 'max'],
    'Quantity': 'sum',
    'Order_ID': 'count'
})""")

# ============================================================================
# ADVANCED EXERCISES
# ============================================================================

print("\n" + "="*60)
print("ADVANCED COMBINATION EXERCISES")
print("="*60)

print("\n--- EXERCISE 13: Filter + Sort + Group ---")
print("Task: Find 2024 orders, sort by date, then group by month to get monthly sales")
print("Your code here:")
print("# Solution:")
print("""orders_2024 = df[df['Year'] == 2024]
monthly_sales_2024 = orders_2024.sort_values('Date').groupby('Month')['Total_Amount'].sum()""")

print("\n--- EXERCISE 14: Top N Analysis ---")
print("Task: Find top 3 sales reps by total sales amount")
print("Your code here:")
print("# Solution:")
print("top_reps = df.groupby('Sales_Rep')['Total_Amount'].sum().sort_values(ascending=False).head(3)")

print("\n--- EXERCISE 15: Percentage Analysis ---")
print("Task: Calculate what percentage of total sales each region contributes")
print("Your code here:")
print("# Solution:")
print("""region_sales = df.groupby('Region')['Total_Amount'].sum()
region_percentage = (region_sales / region_sales.sum()) * 100""")

# ============================================================================
# DATA ENGINEERING SCENARIOS
# ============================================================================

print("\n" + "="*60)
print("DATA ENGINEERING SCENARIO EXERCISES")
print("="*60)

print("\n--- SCENARIO 1: Data Quality Check ---")
print("Task: Find and count orders with potential data quality issues")
print("- Orders with Unit_Price = 0")
print("- Orders with Quantity > 20 (potential outliers)")
print("- Orders with missing Customer_Type")

print("\n--- SCENARIO 2: ETL Pipeline Simulation ---")
print("Task: Create a data pipeline that:")
print("1. Filters valid orders (Unit_Price > 0, Quantity > 0)")
print("2. Adds a 'Season' column based on month")
print("3. Creates summary tables by Region and Season")

print("\n--- SCENARIO 3: Business Intelligence Report ---")
print("Task: Create a comprehensive sales report showing:")
print("1. Top 5 products by revenue")
print("2. Customer type performance comparison")
print("3. Monthly trend analysis")
print("4. Regional performance ranking")

print("\n--- SCENARIO 4: Performance Optimization ---")
print("Task: Optimize queries for large datasets")
print("1. Use categorical data types for string columns")
print("2. Create efficient filtering strategies")
print("3. Use vectorized operations instead of loops")

# ============================================================================
# PRACTICE RECOMMENDATIONS
# ============================================================================

print("\n" + "="*60)
print("BECOMING AN EXPERT - PRACTICE PLAN")
print("="*60)

print("""
WEEK 1-2: Master the Basics
- Practice all 15 exercises above daily
- Try different datasets (customer data, financial data, web logs)
- Focus on clean, readable code

WEEK 3-4: Advanced Techniques
- Learn query optimization techniques
- Practice with larger datasets (100k+ rows)
- Combine operations efficiently
- Learn about memory usage and performance

WEEK 5-6: Real-World Application
- Work with messy, real-world datasets
- Handle missing data during filtering
- Create reusable functions for common operations
- Practice explaining your analysis to others

DATA ENGINEERING FOCUS AREAS:
1. Data Quality: Always validate your filters
2. Performance: Think about scalability
3. Documentation: Comment your complex queries
4. Testing: Verify your results make business sense
5. Automation: Write reusable functions and pipelines

RECOMMENDED PRACTICE DATASETS:
- Kaggle datasets (sales, customer, product data)
- Public APIs (weather, stock prices, social media)
- Generate synthetic data for specific scenarios
- Company internal data (if available)
""")

print("\n" + "="*50)
print("READY TO START PRACTICING!")
print("Copy this code and run it to begin your exercises!")
print("="*50)