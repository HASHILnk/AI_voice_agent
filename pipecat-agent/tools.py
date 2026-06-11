from database.db import get_connection

#search_room()
#book_room()
#get_bookings()



def search_rooms(city):

    conn = get_connection()
    cursor = conn.cursor()

    query = """
    SELECT hotel_name, room_type, price, available_rooms
    FROM hotels
    WHERE city = %s
    """

    cursor.execute(query, (city,))

    rows = cursor.fetchall()

    hotels = []

    for row in rows:
        hotels.append({
            "hotel_name": row[0],
            "room_type": row[1],
            "price": row[2],
            "available_rooms": row[3]
        })

    cursor.close()
    conn.close()

    return hotels





def book_room(
    customer_name,
    hotel_name,
    city,
    room_type,
    check_in_date,
    nights
):

    if room_type:
        room_type_lower = room_type.lower()
        if "standard" in room_type_lower:
            room_type = "Standard"
        elif "deluxe" in room_type_lower:
            room_type = "Deluxe"
        elif "premium" in room_type_lower:
            room_type = "Premium"
        else:
            room_type = room_type.strip().title()

    conn = get_connection()
    cursor = conn.cursor()

    # Check if the hotel exists in the database
    cursor.execute(
        "SELECT DISTINCT hotel_name, city FROM hotels WHERE LOWER(hotel_name) = LOWER(%s) LIMIT 1;",
        (hotel_name,)
    )
    db_hotel = cursor.fetchone()
    if not db_hotel:
        conn.close()
        return {
            "status": "failed",
            "message": f"Hotel '{hotel_name}' not found in system"
        }

    hotel_name = db_hotel[0]
    if not city:
        city = db_hotel[1]

    if room_type:
        cursor.execute(
            "SELECT DISTINCT room_type FROM hotels WHERE LOWER(hotel_name) = LOWER(%s) AND LOWER(room_type) = LOWER(%s) LIMIT 1;",
            (hotel_name, room_type)
        )
        db_room_type = cursor.fetchone()
        if not db_room_type:
            conn.close()
            return {
                "status": "failed",
                "message": f"Room type '{room_type}' not found for hotel '{hotel_name}'"
            }
        room_type = db_room_type[0]

    # Check room availability
    availability_query = """
    SELECT available_rooms
    FROM hotels
    WHERE hotel_name = %s
    AND city = %s
    AND room_type = %s
    """

    cursor.execute(
        availability_query,
        (hotel_name, city, room_type)
    )

    result = cursor.fetchone()

    if not result:
        conn.close()
        return {
            "status": "failed",
            "message": "Hotel room not found"
        }

    available_rooms = result[0]

    if available_rooms <= 0:
        conn.close()
        return {
            "status": "failed",
            "message": "No rooms available"
        }

    # Save booking
    booking_query = """
    INSERT INTO bookings
    (
        customer_name,
        hotel_name,
        city,
        room_type,
        check_in_date,
        nights
    )
    VALUES (%s, %s, %s, %s, %s, %s)
    """

    cursor.execute(
        booking_query,
        (
            customer_name,
            hotel_name,
            city,
            room_type,
            check_in_date,
            nights
        )
    )

    # Reduce available room count
    update_query = """
    UPDATE hotels
    SET available_rooms = available_rooms - 1
    WHERE hotel_name = %s
    AND city = %s
    AND room_type = %s
    """

    cursor.execute(
        update_query,
        (hotel_name, city, room_type)
    )

    conn.commit()

    cursor.close()
    conn.close()

    return {
        "status": "success",
        "message": "Room booked successfully"
    }





def get_bookings(customer_name):

    conn = get_connection()
    cursor = conn.cursor()

    query = """
    SELECT
        hotel_name,
        city,
        room_type,
        check_in_date,
        nights
    FROM bookings
    WHERE customer_name = %s
    """

    cursor.execute(query, (customer_name,))

    rows = cursor.fetchall()

    bookings = []

    for row in rows:
        bookings.append({
            "hotel_name": row[0],
            "city": row[1],
            "room_type": row[2],
            "check_in_date": str(row[3]),
            "nights": row[4]
        })

    cursor.close()
    conn.close()

    return bookings