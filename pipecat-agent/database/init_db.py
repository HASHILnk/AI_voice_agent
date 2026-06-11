from database.db import get_connection
import psycopg2

def initialize_database():
    try:
        conn = get_connection()
        cursor = conn.cursor()

        # Check if hotels table exists and has data to avoid duplicate inserts
        cursor.execute("SELECT to_regclass('public.hotels');")
        table_exists = cursor.fetchone()[0]
        
        has_data = False
        if table_exists:
            cursor.execute("SELECT COUNT(*) FROM hotels;")
            has_data = cursor.fetchone()[0] > 0

        if not has_data:
            with open("database/schema.sql", "r") as file:
                sql_script = file.read()

            cursor.execute(sql_script)
            conn.commit()
            print("Database initialized successfully!")
        else:
            print("Database already initialized with data.")

        # Ensure customer_phone column exists (migration for existing database)
        cursor.execute("ALTER TABLE bookings ADD COLUMN IF NOT EXISTS customer_phone VARCHAR(50);")
        conn.commit()

        cursor.close()
        conn.close()
    except Exception as e:
        print("Database initialization error:", e)

if __name__ == "__main__":
    initialize_database()