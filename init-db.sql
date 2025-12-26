-- SQL Server MCP Test Database Initialization Script
-- This script creates a test database with sample tables and data

USE master;
GO

-- Create the test database if it doesn't exist
IF NOT EXISTS (SELECT * FROM sys.databases WHERE name = 'MyAppDB')
BEGIN
    CREATE DATABASE MyAppDB;
    PRINT 'Database MyAppDB created successfully.';
END
ELSE
BEGIN
    PRINT 'Database MyAppDB already exists.';
END
GO

USE MyAppDB;
GO

-- Create schema
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'dbo')
BEGIN
    EXEC('CREATE SCHEMA dbo');
END
GO

-- Create Users table
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'dbo.Users') AND type in (N'U'))
BEGIN
    CREATE TABLE dbo.Users (
        id INT PRIMARY KEY IDENTITY(1,1),
        username NVARCHAR(50) NOT NULL UNIQUE,
        email NVARCHAR(100) NOT NULL UNIQUE,
        first_name NVARCHAR(50),
        last_name NVARCHAR(50),
        created_date DATETIME2 DEFAULT GETDATE(),
        last_login DATETIME2 NULL,
        is_active BIT DEFAULT 1,
        INDEX IX_Users_Email (email),
        INDEX IX_Users_CreatedDate (created_date)
    );
    PRINT 'Table dbo.Users created successfully.';
END
ELSE
BEGIN
    PRINT 'Table dbo.Users already exists.';
END
GO

-- Create Orders table
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'dbo.Orders') AND type in (N'U'))
BEGIN
    CREATE TABLE dbo.Orders (
        id INT PRIMARY KEY IDENTITY(1,1),
        user_id INT NOT NULL,
        order_date DATETIME2 DEFAULT GETDATE(),
        total_amount DECIMAL(10,2) NOT NULL,
        status NVARCHAR(20) DEFAULT 'Pending',
        FOREIGN KEY (user_id) REFERENCES dbo.Users(id),
        INDEX IX_Orders_UserId (user_id),
        INDEX IX_Orders_OrderDate (order_date)
    );
    PRINT 'Table dbo.Orders created successfully.';
END
ELSE
BEGIN
    PRINT 'Table dbo.Orders already exists.';
END
GO

-- Create Products table
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'dbo.Products') AND type in (N'U'))
BEGIN
    CREATE TABLE dbo.Products (
        id INT PRIMARY KEY IDENTITY(1,1),
        name NVARCHAR(100) NOT NULL,
        description NVARCHAR(500),
        price DECIMAL(10,2) NOT NULL,
        stock_quantity INT DEFAULT 0,
        category NVARCHAR(50),
        created_date DATETIME2 DEFAULT GETDATE(),
        INDEX IX_Products_Category (category),
        INDEX IX_Products_Name (name)
    );
    PRINT 'Table dbo.Products created successfully.';
END
ELSE
BEGIN
    PRINT 'Table dbo.Products already exists.';
END
GO

-- Insert sample data into Users table
IF NOT EXISTS (SELECT * FROM dbo.Users)
BEGIN
    INSERT INTO dbo.Users (username, email, first_name, last_name, created_date, is_active) VALUES
    ('john_doe', 'john.doe@example.com', 'John', 'Doe', '2024-01-15', 1),
    ('jane_smith', 'jane.smith@example.com', 'Jane', 'Smith', '2024-02-20', 1),
    ('bob_wilson', 'bob.wilson@example.com', 'Bob', 'Wilson', '2024-03-10', 1),
    ('alice_brown', 'alice.brown@example.com', 'Alice', 'Brown', '2024-01-05', 1),
    ('charlie_davis', 'charlie.davis@example.com', 'Charlie', 'Davis', '2024-04-12', 0);
    
    PRINT 'Sample data inserted into dbo.Users.';
END
ELSE
BEGIN
    PRINT 'Sample data already exists in dbo.Users.';
END
GO

-- Insert sample data into Products table
IF NOT EXISTS (SELECT * FROM dbo.Products)
BEGIN
    INSERT INTO dbo.Products (name, description, price, stock_quantity, category) VALUES
    ('Laptop Pro', 'High-performance laptop for professionals', 1299.99, 50, 'Electronics'),
    ('Wireless Mouse', 'Ergonomic wireless mouse', 29.99, 200, 'Accessories'),
    ('Mechanical Keyboard', 'RGB mechanical keyboard', 149.99, 75, 'Accessories'),
    ('Monitor 27"', '4K 27-inch monitor', 399.99, 30, 'Electronics'),
    ('USB-C Cable', 'High-speed USB-C cable', 19.99, 500, 'Accessories');
    
    PRINT 'Sample data inserted into dbo.Products.';
END
ELSE
BEGIN
    PRINT 'Sample data already exists in dbo.Products.';
END
GO

-- Insert sample data into Orders table
IF NOT EXISTS (SELECT * FROM dbo.Orders)
BEGIN
    DECLARE @user1 INT, @user2 INT, @user3 INT;
    SELECT @user1 = id FROM dbo.Users WHERE username = 'john_doe';
    SELECT @user2 = id FROM dbo.Users WHERE username = 'jane_smith';
    SELECT @user3 = id FROM dbo.Users WHERE username = 'bob_wilson';
    
    INSERT INTO dbo.Orders (user_id, order_date, total_amount, status) VALUES
    (@user1, '2024-05-01', 1299.99, 'Completed'),
    (@user1, '2024-05-15', 29.99, 'Completed'),
    (@user2, '2024-05-10', 149.99, 'Pending'),
    (@user2, '2024-05-20', 399.99, 'Completed'),
    (@user3, '2024-05-05', 19.99, 'Completed');
    
    PRINT 'Sample data inserted into dbo.Orders.';
END
ELSE
BEGIN
    PRINT 'Sample data already exists in dbo.Orders.';
END
GO

-- Update statistics for better query performance
UPDATE STATISTICS dbo.Users;
UPDATE STATISTICS dbo.Orders;
UPDATE STATISTICS dbo.Products;
GO

PRINT 'Database initialization completed successfully!';
GO

