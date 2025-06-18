-- Create schemas to organize our data layers
CREATE SCHEMA IF NOT EXISTS raw_data;
CREATE SCHEMA IF NOT EXISTS presentation;

-- === Create Tables in the RAW_DATA Schema ===
-- These tables will hold the initial, untransformed data from S3.

-- 1. apartments table
CREATE TABLE IF NOT EXISTS raw_data.apartments (
    id INT,
    title VARCHAR(255),
    source VARCHAR(50),
    price DECIMAL(10, 2),
    currency VARCHAR(10),
    listing_created_on TIMESTAMP,
    is_active BOOLEAN,
    last_modified_timestamp TIMESTAMP
);

-- 2. apartment_attributes table
CREATE TABLE IF NOT EXISTS raw_data.apartment_attributes (
    id INT,
    category VARCHAR(50),
    body VARCHAR(65535),
    amenities VARCHAR(65535),
    bathrooms INT,
    bedrooms INT,
    fee DECIMAL(10, 2),
    has_photo BOOLEAN,
    pets_allowed BOOLEAN,
    price_display VARCHAR(50),
    price_type VARCHAR(50),
    square_feet INT,
    address VARCHAR(255),
    cityname VARCHAR(100),
    state VARCHAR(50),
    latitude DECIMAL(10, 8),
    longitude DECIMAL(11, 8)
);

-- 3. bookings table
CREATE TABLE IF NOT EXISTS raw_data.bookings (
    booking_id INT,
    user_id INT,
    apartment_id INT,
    booking_date TIMESTAMP,
    checkin_date DATE,
    checkout_date DATE,
    total_price DECIMAL(10, 2),
    currency VARCHAR(10),
    booking_status VARCHAR(50),
    payment_status VARCHAR(50),
    num_guests INT
);

-- 4. user_viewings table
CREATE TABLE IF NOT EXISTS raw_data.user_viewings (
    user_id INT,
    apartment_id INT,
    viewed_at TIMESTAMP,
    is_wishlisted BOOLEAN,
    call_to_action VARCHAR(50)
);



-- Create schemas to organize our data layers
CREATE SCHEMA IF NOT EXISTS curated;
-- CREATE SCHEMA IF NOT EXISTS presentation;

-- === Create Tables in the RAW Schema ===
-- These tables will hold the initial, untransformed data from S3.

-- 1. apartments table
CREATE TABLE IF NOT EXISTS curated.apartments (
    id INT,
    title VARCHAR(255),
    source VARCHAR(50),
    price DECIMAL(10, 2),
    currency VARCHAR(10),
    price_usd DECIMAL(10, 2),
    listing_age_days INT,
    listing_created_on TIMESTAMP,
    is_active BOOLEAN,
    last_modified_timestamp TIMESTAMP
);

-- 2. apartment_attributes table
CREATE TABLE IF NOT EXISTS curated.apartment_attributes (
    id INT,
    category VARCHAR(50),
    body VARCHAR(65535),
    amenities VARCHAR(65535),
    bathrooms INT,
    bedrooms INT,
    fee DECIMAL(10, 2),
    has_photo BOOLEAN,
    pets_allowed BOOLEAN,
    price_display VARCHAR(50),
    price_type VARCHAR(50),
    square_feet INT,
    cost_per_sq_foot INT,
    address VARCHAR(255),
    cityname VARCHAR(100),
    state VARCHAR(50),
    latitude DECIMAL(10, 8),
    longitude DECIMAL(11, 8)
);

-- 3. bookings table
CREATE TABLE IF NOT EXISTS curated.bookings (
    booking_id INT,
    user_id INT,
    apartment_id INT,
    booking_date TIMESTAMP,
    checkin_date DATE,
    checkout_date DATE,
    total_price DECIMAL(10, 2),
    currency VARCHAR(10),
    total_price_usd DECIMAL(10, 2),
    booking_status VARCHAR(50),
    payment_status VARCHAR(50),
    num_guests INT
);

-- 4. user_viewings table
CREATE TABLE IF NOT EXISTS curated.user_viewings (
    user_id INT,
    apartment_id INT,
    viewed_at TIMESTAMP,
    is_wishlisted BOOLEAN,
    call_to_action VARCHAR(50),
    viewing_year VARCHAR(50),
    viewing_month VARCHAR(50),
    viewing_day_of_week VARCHAR(50)
);




