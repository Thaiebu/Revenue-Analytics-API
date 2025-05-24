import pytest
import sqlite3
import tempfile
import os
from fastapi.testclient import TestClient

# Import from app module
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.main import app
from app.database import create_tables

@pytest.fixture
def test_client():
    """Create test client"""
    return TestClient(app)

@pytest.fixture
def test_db():
    """Create a temporary test database with sample data"""
    # Create temporary database file
    db_fd, db_path = tempfile.mkstemp(suffix='.db')
    
    # Set environment variable for test database
    os.environ['DATABASE_PATH'] = db_path
    
    try:
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        
        # Create tables
        create_tables(conn)
        
        # Insert sample data
        insert_sample_data(conn)
        
        yield db_path
    finally:
        conn.close()
        os.close(db_fd)
        if os.path.exists(db_path):
            os.unlink(db_path)
        # Reset environment variable
        if 'DATABASE_PATH' in os.environ:
            del os.environ['DATABASE_PATH']

def insert_sample_data(conn):
    """Insert sample test data"""
    cursor = conn.cursor()
    
    # Insert customers
    customers = [
        ('C001', 'John Doe', 'john@example.com', '123 Main St'),
        ('C002', 'Jane Smith', 'jane@example.com', '456 Oak Ave'),
        ('C003', 'Bob Johnson', 'bob@example.com', '789 Pine Rd')
    ]
    cursor.executemany('INSERT INTO Customers VALUES (?, ?, ?, ?)', customers)
    
    # Insert products
    products = [
        ('P001', 'Laptop', 'Electronics'),
        ('P002', 'Smartphone', 'Electronics'),
        ('P003', 'Desk Chair', 'Furniture'),
        ('P004', 'Coffee Mug', 'Kitchen')
    ]
    cursor.executemany('INSERT INTO Products VALUES (?, ?, ?)', products)
    
    # Insert orders
    orders = [
        ('O001', 'P001', 'C001', '2024-01-15', 1, 1000.00, 0.1, 50.00, 'Credit Card', 'North'),
        ('O002', 'P002', 'C002', '2024-01-20', 2, 800.00, 0.05, 25.00, 'Debit Card', 'South'),
        ('O003', 'P003', 'C003', '2024-02-10', 1, 200.00, 0.0, 15.00, 'Cash', 'East'),
        ('O004', 'P001', 'C001', '2024-02-15', 1, 1000.00, 0.15, 50.00, 'Credit Card', 'North'),
        ('O005', 'P004', 'C002', '2024-03-01', 3, 25.00, 0.0, 10.00, 'Credit Card', 'West'),
        ('O006', 'P002', 'C003', '2024-03-15', 1, 800.00, 0.1, 25.00, 'Debit Card', 'South'),
        ('O007', 'P003', 'C001', '2024-04-01', 2, 200.00, 0.05, 15.00, 'Cash', 'East'),
        ('O008', 'P001', 'C002', '2024-05-01', 1, 1000.00, 0.2, 50.00, 'Credit Card', 'North')
    ]
    cursor.executemany('INSERT INTO Orders VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)', orders)
    
    conn.commit()

# ===== 8. tests/test_revenue_api.py (Updated) =====
import pytest
from unittest.mock import patch
from datetime import date

def test_total_revenue_success(test_client, test_db):
    """Test successful total revenue calculation"""
    response = test_client.get("/revenue/total?start_date=2024-01-01&end_date=2024-12-31")
    
    assert response.status_code == 200
    data = response.json()
    assert "total_revenue" in data
    assert "start_date" in data
    assert "end_date" in data
    assert isinstance(data["total_revenue"], (int, float))

def test_total_revenue_no_data(test_client, test_db):
    """Test total revenue with no data in date range"""
    response = test_client.get("/revenue/total?start_date=2025-01-01&end_date=2025-12-31")
    
    assert response.status_code == 200
    data = response.json()
    assert data["total_revenue"] == 0.0

def test_revenue_by_product_success(test_client, test_db):
    """Test revenue by product endpoint"""
    response = test_client.get("/revenue/by-product?start_date=2024-01-01&end_date=2024-12-31")
    
    assert response.status_code == 200
    data = response.json()
    assert "products" in data
    assert "total_products" in data
    assert isinstance(data["products"], list)

def test_revenue_by_category_success(test_client, test_db):
    """Test revenue by category endpoint"""
    response = test_client.get("/revenue/by-category?start_date=2024-01-01&end_date=2024-12-31")
    
    assert response.status_code == 200
    data = response.json()
    assert "categories" in data
    assert isinstance(data["categories"], list)

def test_revenue_by_region_success(test_client, test_db):
    """Test revenue by region endpoint"""
    response = test_client.get("/revenue/by-region?start_date=2024-01-01&end_date=2024-12-31")
    
    assert response.status_code == 200
    data = response.json()
    assert "regions" in data
    assert isinstance(data["regions"], list)

def test_revenue_trends_monthly(test_client, test_db):
    """Test revenue trends with monthly period"""
    response = test_client.get("/revenue/trends?start_date=2024-01-01&end_date=2024-12-31&period=monthly")
    
    assert response.status_code == 200
    data = response.json()
    assert "trends" in data
    assert data["period_type"] == "monthly"

def test_revenue_summary_success(test_client, test_db):
    """Test revenue summary endpoint"""
    response = test_client.get("/revenue/summary?start_date=2024-01-01&end_date=2024-12-31")
    
    assert response.status_code == 200
    data = response.json()
    assert "summary" in data
    summary = data["summary"]
    assert "total_revenue" in summary

def test_health_endpoint(test_client):
    """Test health check endpoint"""
    response = test_client.get("/health")
    
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "healthy"

def test_invalid_date_format(test_client):
    """Test invalid date format handling"""
    response = test_client.get("/revenue/total?start_date=invalid-date&end_date=2024-12-31")
    assert response.status_code == 422

def test_missing_required_parameters(test_client):
    """Test missing required parameters"""
    response = test_client.get("/revenue/total")
    assert response.status_code == 422
