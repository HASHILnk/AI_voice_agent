import psycopg2


def get_connection():
    conn = psycopg2.connect(
        host="hotel-db",
        database="hotel_booking",
        user="postgres",
        password="password123",
        port=5432
    )

    return conn 