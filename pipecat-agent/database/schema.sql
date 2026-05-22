CREATE TABLE IF NOT EXISTS hotels (
    id SERIAL PRIMARY KEY,
    hotel_name VARCHAR(255) NOT NULL,
    city VARCHAR(100) NOT NULL,
    room_type VARCHAR(100) NOT NULL,
    price INTEGER NOT NULL,
    available_rooms INTEGER NOT NULL
);


CREATE TABLE IF NOT EXISTS bookings (
    id SERIAL PRIMARY KEY,
    customer_name VARCHAR(255) NOT NULL,
    hotel_name VARCHAR(255) NOT NULL,
    city VARCHAR(100) NOT NULL,
    room_type VARCHAR(100) NOT NULL,
    check_in_date DATE NOT NULL,
    nights INTEGER NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);


INSERT INTO hotels (
    hotel_name,
    city,
    room_type,
    price,
    available_rooms
)
VALUES
('Grand Palace', 'Kochi', 'Deluxe', 2500, 5),

('Grand Palace', 'Kochi', 'Premium', 4000, 3),

('Sea View Resort', 'Kochi', 'Standard', 1800, 7),

('Sea View Resort', 'Kochi', 'Deluxe', 3200, 4),

('Hill Top Inn', 'Munnar', 'Deluxe', 2800, 6),

('Luxury Stay', 'Bangalore', 'Premium', 5000, 2);