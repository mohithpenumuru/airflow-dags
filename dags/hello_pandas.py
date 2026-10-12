from datetime import datetime
import pandas as pd
from airflow.decorators import dag, task

@dag(
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=['example', 'pandas', 'k8s']
)
def hello_k8s_pandas():

    @task
    def extract_raw_data():
        """
        Simulates extracting raw transaction data from a source.
        Returns a list of dictionaries (JSON serializable).
        """
        raw_data = [
            {"store_id": 1, "product": "Laptop", "amount": 1200, "category": "Electronics"},
            {"store_id": 1, "product": "Mouse", "amount": 25, "category": "Electronics"},
            {"store_id": 2, "product": "Desk Chair", "amount": 150, "category": "Furniture"},
            {"store_id": 2, "product": "Monitor", "amount": 300, "category": "Electronics"},
            {"store_id": 3, "product": "Notebook", "amount": 5, "category": "Stationery"},
        ]
        return raw_data

    @task
    def transform_filter_high_value(data_list) -> list:
        """
        Uses Pandas to filter transactions where the amount is greater than $50.
        """
        df = pd.DataFrame(data_list)
        
        # Filter for high-value transactions
        filtered_df = df[df['amount'] > 50]
        
        # Return as a list of dictionaries for XCom serialization
        return filtered_df.to_dict(orient='records')

    @task
    def transform_aggregate_by_category(data_list) -> dict:
        """
        Uses Pandas to calculate total spend and transaction count by category.
        """
        df = pd.DataFrame(data_list)
        
        # Group by category and aggregate total sales and count
        summary_df = df.groupby('category').agg(
            total_sales=('amount', 'sum'),
            transaction_count=('amount', 'count')
        ).reset_index()
        
        # Return summary as a dictionary
        return summary_df.to_dict(orient='list')

    @task
    def load_summary(high_value_items, aggregated_data):
        """
        Loads and prints the final processed reports.
        """
        print("--- High Value Items (> $50) ---")
        for item in high_value_items:
            print(f"Product: {item['product']} | Amount: ${item['amount']} ({item['category']})")
            
        print("\n--- Sales Summary by Category ---")
        categories = aggregated_data['category']
        sales = aggregated_data['total_sales']
        counts = aggregated_data['transaction_count']
        
        for cat, sale, count in zip(categories, sales, counts):
            print(f"Category: {cat} | Total Sales: ${sale} | Count: {count}")

    # Orchestrate the execution pipeline
    raw_data = extract_raw_data()
    
    high_value = transform_filter_high_value(raw_data)
    summary = transform_aggregate_by_category(raw_data)
    
    load_summary(high_value, summary)

# Instantiate the DAG
hello_k8s_pandas()
