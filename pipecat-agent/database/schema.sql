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
    customer_phone VARCHAR(50),
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

-- =====================
-- KOCHI
-- =====================
('Palm Inn', 'Kochi', 'Standard', 1800, 6),
('Palm Inn', 'Kochi', 'Deluxe', 2800, 4),
('Palm Inn', 'Kochi', 'Premium', 4200, 2),

('City Stay', 'Kochi', 'Standard', 1700, 5),
('City Stay', 'Kochi', 'Deluxe', 2600, 4),
('City Stay', 'Kochi', 'Premium', 4000, 3),

('Metro Inn', 'Kochi', 'Standard', 1900, 7),
('Metro Inn', 'Kochi', 'Deluxe', 2900, 5),
('Metro Inn', 'Kochi', 'Premium', 4300, 2),

-- =====================
-- CALICUT
-- =====================
('Blue Inn', 'Calicut', 'Standard', 1700, 7),
('Blue Inn', 'Calicut', 'Deluxe', 2600, 5),
('Blue Inn', 'Calicut', 'Premium', 3900, 3),

('Lake Stay', 'Calicut', 'Standard', 1800, 6),
('Lake Stay', 'Calicut', 'Deluxe', 2800, 4),
('Lake Stay', 'Calicut', 'Premium', 4100, 2),

('Park Inn', 'Calicut', 'Standard', 1750, 8),
('Park Inn', 'Calicut', 'Deluxe', 2700, 5),
('Park Inn', 'Calicut', 'Premium', 3950, 3),

-- =====================
-- KANNUR
-- =====================
('Green Stay', 'Kannur', 'Standard', 1600, 8),
('Green Stay', 'Kannur', 'Deluxe', 2500, 4),
('Green Stay', 'Kannur', 'Premium', 3800, 3),

('Beach Inn', 'Kannur', 'Standard', 1750, 7),
('Beach Inn', 'Kannur', 'Deluxe', 2650, 5),
('Beach Inn', 'Kannur', 'Premium', 3950, 2),

('Royal Stay', 'Kannur', 'Standard', 1800, 5),
('Royal Stay', 'Kannur', 'Deluxe', 2750, 4),
('Royal Stay', 'Kannur', 'Premium', 4100, 2),

-- =====================
-- MUNNAR
-- =====================
('Hill Inn', 'Munnar', 'Standard', 2200, 5),
('Hill Inn', 'Munnar', 'Deluxe', 3200, 4),
('Hill Inn', 'Munnar', 'Premium', 4500, 2),

('Mist Stay', 'Munnar', 'Standard', 2300, 6),
('Mist Stay', 'Munnar', 'Deluxe', 3300, 4),
('Mist Stay', 'Munnar', 'Premium', 4700, 2),

('Cloud Inn', 'Munnar', 'Standard', 2100, 5),
('Cloud Inn', 'Munnar', 'Deluxe', 3100, 3),
('Cloud Inn', 'Munnar', 'Premium', 4400, 2),

-- =====================
-- WAYANAD
-- =====================
('Forest Inn', 'Wayanad', 'Standard', 2100, 6),
('Forest Inn', 'Wayanad', 'Deluxe', 3100, 4),
('Forest Inn', 'Wayanad', 'Premium', 4400, 2),

('Nature Stay', 'Wayanad', 'Standard', 2200, 5),
('Nature Stay', 'Wayanad', 'Deluxe', 3200, 4),
('Nature Stay', 'Wayanad', 'Premium', 4500, 2),

('Green Leaf', 'Wayanad', 'Standard', 2000, 7),
('Green Leaf', 'Wayanad', 'Deluxe', 3000, 5),
('Green Leaf', 'Wayanad', 'Premium', 4300, 2),

-- =====================
-- THRISSUR
-- =====================
('Royal Inn', 'Thrissur', 'Standard', 1800, 7),
('Royal Inn', 'Thrissur', 'Deluxe', 2900, 5),
('Royal Inn', 'Thrissur', 'Premium', 4200, 3),

('Temple Stay', 'Thrissur', 'Standard', 1700, 6),
('Temple Stay', 'Thrissur', 'Deluxe', 2700, 4),
('Temple Stay', 'Thrissur', 'Premium', 4000, 2),

('City Inn', 'Thrissur', 'Standard', 1850, 5),
('City Inn', 'Thrissur', 'Deluxe', 2800, 4),
('City Inn', 'Thrissur', 'Premium', 4100, 2),

-- =====================
-- TRIVANDRUM
-- =====================
('Capital Stay', 'Trivandrum', 'Standard', 2000, 6),
('Capital Stay', 'Trivandrum', 'Deluxe', 3000, 4),
('Capital Stay', 'Trivandrum', 'Premium', 4300, 3),

('Sky Inn', 'Trivandrum', 'Standard', 1900, 7),
('Sky Inn', 'Trivandrum', 'Deluxe', 2900, 5),
('Sky Inn', 'Trivandrum', 'Premium', 4200, 2),

('Metro Stay', 'Trivandrum', 'Standard', 2100, 5),
('Metro Stay', 'Trivandrum', 'Deluxe', 3100, 4),
('Metro Stay', 'Trivandrum', 'Premium', 4400, 2),

-- =====================
-- ALAPPUZHA
-- =====================
('Backwater Inn', 'Alappuzha', 'Standard', 1900, 5),
('Backwater Inn', 'Alappuzha', 'Deluxe', 3000, 4),
('Backwater Inn', 'Alappuzha', 'Premium', 4200, 2),

('River Stay', 'Alappuzha', 'Standard', 1800, 6),
('River Stay', 'Alappuzha', 'Deluxe', 2900, 5),
('River Stay', 'Alappuzha', 'Premium', 4100, 2),

('Lake Inn', 'Alappuzha', 'Standard', 1950, 5),
('Lake Inn', 'Alappuzha', 'Deluxe', 3050, 4),
('Lake Inn', 'Alappuzha', 'Premium', 4300, 2),

-- =====================
-- PALAKKAD
-- =====================
('Park Inn', 'Palakkad', 'Standard', 1700, 6),
('Park Inn', 'Palakkad', 'Deluxe', 2600, 4),
('Park Inn', 'Palakkad', 'Premium', 3900, 2),

('Hill Stay', 'Palakkad', 'Standard', 1800, 5),
('Hill Stay', 'Palakkad', 'Deluxe', 2750, 4),
('Hill Stay', 'Palakkad', 'Premium', 4050, 2),

('Palm Stay', 'Palakkad', 'Standard', 1750, 6),
('Palm Stay', 'Palakkad', 'Deluxe', 2650, 4),
('Palm Stay', 'Palakkad', 'Premium', 3950, 2),

-- =====================
-- KOLLAM
-- =====================
('Ocean Inn', 'Kollam', 'Standard', 1800, 5),
('Ocean Inn', 'Kollam', 'Deluxe', 2900, 4),
('Ocean Inn', 'Kollam', 'Premium', 4100, 2),

('Sea Stay', 'Kollam', 'Standard', 1850, 6),
('Sea Stay', 'Kollam', 'Deluxe', 2950, 4),
('Sea Stay', 'Kollam', 'Premium', 4200, 2),

('Blue Stay', 'Kollam', 'Standard', 1750, 5),
('Blue Stay', 'Kollam', 'Deluxe', 2850, 4),
('Blue Stay', 'Kollam', 'Premium', 4000, 2);