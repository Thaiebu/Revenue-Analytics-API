import sqlite3
import pandas as pd
from datetime import datetime
import logging

logging.basicConfig(filename='data_loader.log', level=logging.INFO)

def load_data(csv_path, mode='append'):
    conn = sqlite3.connect('sales_data.db')
    cursor = conn.cursor()
    
    if mode == 'overwrite':
        cursor.execute('DELETE FROM Orders')
        cursor.execute('DELETE FROM Products')
        cursor.execute('DELETE FROM Customers')
        conn.commit()
        logging.info(f"{datetime.now()}: Overwrite mode. Cleared existing data.")
    
    chunk_size = 10000
    for chunk in pd.read_csv(csv_path, chunksize=chunk_size):
        # Process Customers
        customers = chunk[['Customer ID', 'Customer Name', 'Customer Email', 'Customer Address']].dropna(subset=['Customer ID'])
        customers = customers.drop_duplicates(subset=['Customer ID'])
        customers = customers.where(pd.notnull(customers), None)
        cursor.executemany('''
            INSERT OR IGNORE INTO Customers (CustomerID, CustomerName, CustomerEmail, CustomerAddress)
            VALUES (?, ?, ?, ?)
        ''', customers.to_records(index=False).tolist())
        
        # Process Products
        products = chunk[['Product ID', 'Product Name', 'Category']].dropna(subset=['Product ID'])
        products = products.drop_duplicates(subset=['Product ID'])
        products = products.where(pd.notnull(products), None)
        cursor.executemany('''
            INSERT OR IGNORE INTO Products (ProductID, ProductName, Category)
            VALUES (?, ?, ?)
        ''', products.to_records(index=False).tolist())
        
        # Process Orders
        orders = chunk[['Order ID', 'Product ID', 'Customer ID', 'Date of Sale', 'Quantity Sold',
                        'Unit Price', 'Discount', 'Shipping Cost', 'Payment Method', 'Region']].dropna(subset=['Order ID', 'Product ID', 'Customer ID'])
        orders['Date of Sale'] = pd.to_datetime(orders['Date of Sale'], errors='coerce').dt.strftime('%Y-%m-%d')
        orders = orders.dropna(subset=['Date of Sale'])
        orders = orders.where(pd.notnull(orders), None)
        cursor.executemany('''
            INSERT OR IGNORE INTO Orders 
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', orders.to_records(index=False).tolist())
        
        conn.commit()
    
    conn.close()
    logging.info(f"{datetime.now()}: Data loaded from {csv_path} in {mode} mode.")
    return True