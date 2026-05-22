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

    conn = get_connection()
    cursor = conn.cursor()

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