from fastapi import FastAPI, BackgroundTasks, HTTPException, Query
from pydantic import BaseModel
from datetime import date
from typing import Optional, Literal
import sqlite3
from database import get_db_connection
import data_loader
import logging

app = FastAPI(title="Revenue Analytics API", version="1.0.0")

class RefreshRequest(BaseModel):
    csv_path: str
    mode: str = 'append'

@app.post("/refresh-data")
async def refresh_data(request: RefreshRequest, background_tasks: BackgroundTasks):
    """Refresh data from CSV file"""
    if request.mode not in ('append', 'overwrite'):
        raise HTTPException(400, detail="Invalid mode. Use 'append' or 'overwrite'.")
    background_tasks.add_task(data_loader.load_data, request.csv_path, request.mode)
    return {"message": "Data refresh initiated."}

@app.get("/revenue/total")
async def total_revenue(start_date: date, end_date: date):
    """Get total revenue for a date range"""
    conn = get_db_connection()
    try:
        result = conn.execute('''
            SELECT SUM((UnitPrice * QuantitySold * (1 - Discount)) + ShippingCost) AS total
            FROM Orders
            WHERE DateOfSale BETWEEN ? AND ?
        ''', (start_date.isoformat(), end_date.isoformat())).fetchone()
        return {
            "total_revenue": result['total'] or 0.0,
            "start_date": start_date,
            "end_date": end_date
        }
    except sqlite3.Error as e:
        raise HTTPException(500, detail=f"Database error: {str(e)}")
    finally:
        conn.close()

@app.get("/revenue/by-product")
async def revenue_by_product(
    start_date: date, 
    end_date: date,
    limit: Optional[int] = Query(None, description="Limit number of results")
):
    """Get total revenue by product for a date range"""
    conn = get_db_connection()
    try:
        query = '''
            SELECT 
                p.ProductID, 
                p.ProductName, 
                SUM((o.UnitPrice * o.QuantitySold * (1 - o.Discount)) + o.ShippingCost) AS revenue,
                SUM(o.QuantitySold) AS total_quantity_sold,
                COUNT(o.OrderID) AS total_orders
            FROM Orders o 
            JOIN Products p ON o.ProductID = p.ProductID
            WHERE o.DateOfSale BETWEEN ? AND ?
            GROUP BY p.ProductID, p.ProductName
            ORDER BY revenue DESC
        '''
        
        params = [start_date.isoformat(), end_date.isoformat()]
        
        if limit:
            query += " LIMIT ?"
            params.append(limit)
            
        rows = conn.execute(query, params).fetchall()
        
        return {
            "products": [dict(row) for row in rows],
            "start_date": start_date,
            "end_date": end_date,
            "total_products": len(rows)
        }
    except sqlite3.Error as e:
        raise HTTPException(500, detail=f"Database error: {str(e)}")
    finally:
        conn.close()

@app.get("/revenue/by-category")
async def revenue_by_category(
    start_date: date, 
    end_date: date,
    limit: Optional[int] = Query(None, description="Limit number of results")
):
    """Get total revenue by category for a date range"""
    conn = get_db_connection()
    try:
        query = '''
            SELECT 
                p.Category, 
                SUM((o.UnitPrice * o.QuantitySold * (1 - o.Discount)) + o.ShippingCost) AS revenue,
                SUM(o.QuantitySold) AS total_quantity_sold,
                COUNT(DISTINCT p.ProductID) AS unique_products,
                COUNT(o.OrderID) AS total_orders
            FROM Orders o 
            JOIN Products p ON o.ProductID = p.ProductID
            WHERE o.DateOfSale BETWEEN ? AND ?
            GROUP BY p.Category
            ORDER BY revenue DESC
        '''
        
        params = [start_date.isoformat(), end_date.isoformat()]
        
        if limit:
            query += " LIMIT ?"
            params.append(limit)
            
        rows = conn.execute(query, params).fetchall()
        
        return {
            "categories": [dict(row) for row in rows],
            "start_date": start_date,
            "end_date": end_date,
            "total_categories": len(rows)
        }
    except sqlite3.Error as e:
        raise HTTPException(500, detail=f"Database error: {str(e)}")
    finally:
        conn.close()

@app.get("/revenue/by-region")
async def revenue_by_region(
    start_date: date, 
    end_date: date,
    limit: Optional[int] = Query(None, description="Limit number of results")
):
    """Get total revenue by region for a date range"""
    conn = get_db_connection()
    try:
        query = '''
            SELECT 
                Region, 
                SUM((UnitPrice * QuantitySold * (1 - Discount)) + ShippingCost) AS revenue,
                SUM(QuantitySold) AS total_quantity_sold,
                COUNT(DISTINCT CustomerID) AS unique_customers,
                COUNT(OrderID) AS total_orders
            FROM Orders
            WHERE DateOfSale BETWEEN ? AND ?
            GROUP BY Region
            ORDER BY revenue DESC
        '''
        
        params = [start_date.isoformat(), end_date.isoformat()]
        
        if limit:
            query += " LIMIT ?"
            params.append(limit)
            
        rows = conn.execute(query, params).fetchall()
        
        return {
            "regions": [dict(row) for row in rows],
            "start_date": start_date,
            "end_date": end_date,
            "total_regions": len(rows)
        }
    except sqlite3.Error as e:
        raise HTTPException(500, detail=f"Database error: {str(e)}")
    finally:
        conn.close()

@app.get("/revenue/trends")
async def revenue_trends(
    start_date: date,
    end_date: date,
    period: Literal["monthly", "quarterly", "yearly"] = Query("monthly", description="Time period for trends")
):
    """Get revenue trends over time for a date range"""
    conn = get_db_connection()
    try:
        # SQL date formatting based on period
        if period == "monthly":
            date_format = "%Y-%m"
            date_label = "Month"
        elif period == "quarterly":
            # SQLite doesn't have direct quarter function, so we'll calculate it
            date_format = "%Y"
            date_label = "Quarter"
        else:  # yearly
            date_format = "%Y"
            date_label = "Year"
        
        if period == "quarterly":
            query = '''
                SELECT 
                    strftime('%Y', DateOfSale) AS year,
                    CASE 
                        WHEN CAST(strftime('%m', DateOfSale) AS INTEGER) BETWEEN 1 AND 3 THEN 'Q1'
                        WHEN CAST(strftime('%m', DateOfSale) AS INTEGER) BETWEEN 4 AND 6 THEN 'Q2'
                        WHEN CAST(strftime('%m', DateOfSale) AS INTEGER) BETWEEN 7 AND 9 THEN 'Q3'
                        ELSE 'Q4'
                    END AS quarter,
                    SUM((UnitPrice * QuantitySold * (1 - Discount)) + ShippingCost) AS revenue,
                    SUM(QuantitySold) AS total_quantity_sold,
                    COUNT(OrderID) AS total_orders
                FROM Orders
                WHERE DateOfSale BETWEEN ? AND ?
                GROUP BY year, quarter
                ORDER BY year, quarter
            '''
        else:
            query = f'''
                SELECT 
                    strftime('{date_format}', DateOfSale) AS period,
                    SUM((UnitPrice * QuantitySold * (1 - Discount)) + ShippingCost) AS revenue,
                    SUM(QuantitySold) AS total_quantity_sold,
                    COUNT(OrderID) AS total_orders
                FROM Orders
                WHERE DateOfSale BETWEEN ? AND ?
                GROUP BY period
                ORDER BY period
            '''
        
        rows = conn.execute(query, (start_date.isoformat(), end_date.isoformat())).fetchall()
        
        # Format results for quarterly data
        if period == "quarterly":
            trends = []
            for row in rows:
                trends.append({
                    "period": f"{row['year']}-{row['quarter']}",
                    "revenue": row["revenue"],
                    "total_quantity_sold": row["total_quantity_sold"],
                    "total_orders": row["total_orders"]
                })
        else:
            trends = [dict(row) for row in rows]
        
        return {
            "trends": trends,
            "period_type": period,
            "start_date": start_date,
            "end_date": end_date,
            "total_periods": len(trends)
        }
    except sqlite3.Error as e:
        raise HTTPException(500, detail=f"Database error: {str(e)}")
    finally:
        conn.close()

@app.get("/revenue/summary")
async def revenue_summary(start_date: date, end_date: date):
    """Get comprehensive revenue summary for a date range"""
    conn = get_db_connection()
    try:
        # Total revenue
        total_result = conn.execute('''
            SELECT SUM((UnitPrice * QuantitySold * (1 - Discount)) + ShippingCost) AS total
            FROM Orders
            WHERE DateOfSale BETWEEN ? AND ?
        ''', (start_date.isoformat(), end_date.isoformat())).fetchone()
        
        # Top performing product
        top_product = conn.execute('''
            SELECT 
                p.ProductName, 
                SUM((o.UnitPrice * o.QuantitySold * (1 - o.Discount)) + o.ShippingCost) AS revenue
            FROM Orders o 
            JOIN Products p ON o.ProductID = p.ProductID
            WHERE o.DateOfSale BETWEEN ? AND ?
            GROUP BY p.ProductID, p.ProductName
            ORDER BY revenue DESC
            LIMIT 1
        ''', (start_date.isoformat(), end_date.isoformat())).fetchone()
        
        # Top performing category
        top_category = conn.execute('''
            SELECT 
                p.Category, 
                SUM((o.UnitPrice * o.QuantitySold * (1 - o.Discount)) + o.ShippingCost) AS revenue
            FROM Orders o 
            JOIN Products p ON o.ProductID = p.ProductID
            WHERE o.DateOfSale BETWEEN ? AND ?
            GROUP BY p.Category
            ORDER BY revenue DESC
            LIMIT 1
        ''', (start_date.isoformat(), end_date.isoformat())).fetchone()
        
        # Top performing region
        top_region = conn.execute('''
            SELECT 
                Region, 
                SUM((UnitPrice * QuantitySold * (1 - Discount)) + ShippingCost) AS revenue
            FROM Orders
            WHERE DateOfSale BETWEEN ? AND ?
            GROUP BY Region
            ORDER BY revenue DESC
            LIMIT 1
        ''', (start_date.isoformat(), end_date.isoformat())).fetchone()
        
        return {
            "summary": {
                "total_revenue": total_result['total'] or 0.0,
                "top_product": dict(top_product) if top_product else None,
                "top_category": dict(top_category) if top_category else None, 
                "top_region": dict(top_region) if top_region else None
            },
            "start_date": start_date,
            "end_date": end_date
        }
    except sqlite3.Error as e:
        raise HTTPException(500, detail=f"Database error: {str(e)}")
    finally:
        conn.close()

@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {"status": "healthy", "message": "Revenue Analytics API is running"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000, reload=True)