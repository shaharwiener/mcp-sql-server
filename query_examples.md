# SQL Query Examples

This document contains commonly used SQL queries that can be executed using the `get_query` tool. These examples are extracted from the previous complex system and serve as reference for typical database operations.

## User & Account Queries

### Get User Verifications
```sql
SELECT * FROM MCPay..mcp_UserVerifications 
WHERE phone = '+972507328808' 
ORDER BY id DESC
```

### Get SMS by Phone
```sql
SELECT * FROM mobydom5..sms_back_log  
WHERE Phone = '+972507328808'  
AND DATEDIFF(MINUTE, send_time, GETDATE()) <= 3 
ORDER BY id DESC
```

### Get Application Subscribers
```sql
SELECT * FROM MCPay..mcp_Application_Subscribers 
WHERE PhoneNumber = '+972507328808' 
ORDER BY id DESC
```

### Get Notification Tokens
```sql
SELECT TOP 100 * FROM MCPay..Application_Notification_Tokens  
WHERE Phone_Number = '+972507328808' 
ORDER BY id DESC
```

### Count User Verifications Same Day
```sql
SELECT DATEADD(day, DATEDIFF(day, 0, CreateDate), 0) AS day, 
       COUNT(*) AS occurrence_count 
FROM MCPay..mcp_UserVerifications 
WHERE Phone = '+972507328808' 
GROUP BY DATEADD(day, DATEDIFF(day, 0, CreateDate), 0) 
ORDER BY day DESC
```

### Get Statement Header by Account
```sql
SELECT * FROM MCPay..mcp_Statement_Header 
WHERE Account_Id = 3514189 
AND Date_Created >= DATEADD(MINUTE, -15, CURRENT_TIMESTAMP) 
ORDER BY id DESC
```

### Get Accounts with Custom Conditions
```sql
SELECT TOP 10 * 
FROM mobydom5.dbo.accounts 
WHERE active = 1 AND type = 1
ORDER BY Id DESC
```

## Car & Driver Queries

### Get Cars by Account
```sql
SELECT * FROM mobydom5..cars  
WHERE account = 3514189 
AND active = 1 
AND car_no_N = '1234567' 
ORDER BY id DESC
```

### Check if Car Exists
```sql
SELECT * FROM mobydom5..cars 
WHERE car_no_N = '1234567'
```

### Get Car IDs by Car Number
```sql
SELECT id FROM mobydom5..cars 
WHERE car_no_N = '1234567'
```

### Get Drivers by Account
```sql
SELECT * FROM mobydom5..drivers 
WHERE account_id = 3514189 
ORDER BY id DESC
```

## Parking & Policy Queries

### Get Last Parking Log
```sql
SELECT * FROM mobydom5..parking_log 
WHERE car_no = '1234567' 
ORDER BY id DESC
```

### Get Parking Policies by City
```sql
SELECT * FROM mobydom5..parking_policies 
WHERE city = 133 
AND active = 1 
ORDER BY id DESC
```

### Get Permit Types by City
```sql
SELECT * FROM mobydom5..permit_types  
WHERE name LIKE '%Tel Aviv%'
```

### Get On-Street Zone Information
```sql
SELECT  
    city.city_pango_id as city, 
    pp.id as policies, 
    pz.id as zone, 
    city.name, 
    pr.id as rate, 
    pz.name as zone_name, 
    pr.max_charge as max_charge, 
    pr.max_daily_charge as max_daily_charge,
    pr.time_range,
    pr.price,
    pz.daily_charge_type
FROM mobydom5..parking_rates pr
INNER JOIN mobydom5..parking_zones pz ON pz.id = pr.zone
INNER JOIN mobydom5..parking_policies pp ON pz.policy = pp.id
INNER JOIN mobydom5..cities city ON city.id = pp.city
WHERE city.name LIKE '%Tel Aviv%' 
AND pz.name LIKE '%Center%'
AND pp.active = 1
AND pr.terminating = 0
```

## Billing & Payment Queries

### Get Billing Authorizations
```sql
SELECT * FROM Billing.blackbox.Authorizations
WHERE TransactionIdentifier = 'TXN123456789'
```

### Get Card Expiration Info
```sql
SELECT * FROM mobydom5.dbo.card_types 
WHERE id = 1
```

## External System Queries

### Get GTFS Trips (MySQL)
```sql
SELECT * FROM GTFS_DB.Trips_Replica_1 
ORDER BY route_id DESC
```

### Get CarWash Orders (MySQL)
```sql
SELECT OrderGuid FROM CarWashDB.WashOrders 
WHERE CarNumber = 1234567 
AND OrderStatus = 'Completed' 
ORDER BY Id DESC
```

## Usage Notes

1. **Phone Numbers**: Always include country code (e.g., '+972507328808')
2. **Account IDs**: Use numeric values without quotes (e.g., 3514189)
3. **Car Numbers**: Use string format with quotes (e.g., '1234567')
4. **Database Detection**: The system will auto-detect the database from table references
5. **Date Filtering**: Use SQL Server date functions like DATEADD, DATEDIFF for time-based queries
6. **TOP Clause**: Use TOP N to limit results and improve performance

## Common Patterns

### Recent Records (Last 15 minutes)
```sql
WHERE Date_Created >= DATEADD(MINUTE, -15, CURRENT_TIMESTAMP)
```

### Active Records Only
```sql
WHERE active = 1
```

### Phone-based Lookup
```sql
WHERE Phone = '+972507328808' OR PhoneNumber = '+972507328808'
```

### Account-based Filtering
```sql
WHERE account = 3514189 OR Account_Id = 3514189
```
