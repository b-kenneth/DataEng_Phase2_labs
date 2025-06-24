-- Query 1: Highest Revenue-Generating Location

SELECT 
    pickup_location_name,
    pickup_city,
    pickup_state,
    total_revenue,
    total_transactions,
    avg_transaction_amount
FROM processed_location_metrics
ORDER BY total_revenue DESC
LIMIT 10;


-- Query 2: Most Rented Vehicle Type

SELECT 
    vehicle_type,
    brand,
    total_transactions,
    total_revenue,
    avg_transaction_amount
FROM processed_vehicle_type_metrics
ORDER BY total_transactions DESC
LIMIT 10;


-- Query 3: Top-Spending Users

SELECT 
    first_name,
    last_name,
    email,
    total_spending,
    total_transactions,
    avg_spending_per_transaction,
    is_active
FROM processed_user_metrics
ORDER BY total_spending DESC
LIMIT 10;


-- Query 4: Daily Revenue Trends

SELECT 
    rental_date,
    total_revenue,
    total_transactions,
    unique_users,
    avg_transaction_amount
FROM processed_daily_metrics
ORDER BY rental_date DESC
LIMIT 15;


-- Query 5: Peak Hour Analysis

SELECT 
    rental_hour,
    total_transactions,
    total_revenue,
    avg_transaction_amount
FROM processed_hourly_metrics
ORDER BY total_transactions DESC;


-- Query 6: Brand Performance Analysis

SELECT 
    brand,
    total_revenue,
    total_transactions,
    vehicle_types_offered,
    total_vehicles
FROM processed_brand_metrics
ORDER BY total_revenue DESC;


-- Query 7: Active vs Inactive User Comparison

SELECT 
    CASE 
        WHEN is_active = 1 THEN 'Active Users'
        ELSE 'Inactive Users'
    END as user_status,
    unique_users,
    total_transactions,
    total_revenue,
    avg_transaction_amount,
    total_rental_hours
FROM processed_user_activity_metrics;
