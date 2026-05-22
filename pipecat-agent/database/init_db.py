from db import get_connection


def initialize_database():
    conn = get_connection()
    cursor = conn.cursor()

    with open("database/schema.sql", "r") as file:
        sql_script = file.read()

    cursor.execute(sql_script)

    conn.commit()

    cursor.close()
    conn.close()

    print("Database initialized successfully!")


initialize_database()