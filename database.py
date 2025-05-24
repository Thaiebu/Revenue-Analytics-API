import sqlite3

def create_tables(conn):
    cursor = conn.cursor()
    
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS Customers (
            CustomerID TEXT PRIMARY KEY,
            CustomerName TEXT,
            CustomerEmail TEXT,
            CustomerAddress TEXT
        )
    ''')
    
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS Products (
            ProductID TEXT PRIMARY KEY,
            ProductName TEXT,
            Category TEXT
        )
    ''')
    
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS Orders (
            OrderID TEXT PRIMARY KEY,
            ProductID TEXT,
            CustomerID TEXT,
            DateOfSale TEXT,
            QuantitySold INTEGER,
            UnitPrice REAL,
            Discount REAL,
            ShippingCost REAL,
            PaymentMethod TEXT,
            Region TEXT,
            FOREIGN KEY (ProductID) REFERENCES Products(ProductID),
            FOREIGN KEY (CustomerID) REFERENCES Customers(CustomerID)
        )
    ''')
    
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_orders_date ON Orders (DateOfSale)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_orders_product ON Orders (ProductID)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_orders_customer ON Orders (CustomerID)')
    
    conn.commit()

def get_db_connection():
    conn = sqlite3.connect('sales_data.db')
    conn.row_factory = sqlite3.Row
    return conn

if __name__ == "__main__":
    conn = get_db_connection()
    create_tables(conn)
    conn.close()
    print("Database and tables created successfully.")