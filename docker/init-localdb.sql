-- Create LocalDB database for testing
IF NOT EXISTS (SELECT * FROM sys.databases WHERE name = 'LocalDB')
BEGIN
    CREATE DATABASE LocalDB;
END
GO
