import os
from dotenv import load_dotenv, find_dotenv
import psycopg2
from datetime import datetime

load_dotenv(find_dotenv())

DATABASE_URL = os.getenv("DATABASE_URL", "")
DB_USER = os.getenv("DB_USER", "")
DB_PASSWORD = os.getenv("DB_PASSWORD", "")
DB_HOST = os.getenv("DB_HOST", "")
DB_PORT = os.getenv("DB_PORT", "")
DB_NAME = os.getenv("DB_NAME", "")
DB_LOG_SCHEMA = os.getenv("DB_LOG_SCHEMA", "")
GROQ_API_KEY = os.getenv("GROQ_API_KEY", "")

if not DATABASE_URL:
    raise ValueError("DATABASE_URL is not set in environment variables")

def test_db_connection():
    """Synchronous function to test database connection using psycopg2."""
    start_time = datetime.now()
    try:
        connection = psycopg2.connect(
            user=DB_USER,
            password=DB_PASSWORD,
            host=DB_HOST,
            port=DB_PORT,
            database=DB_NAME,
        )
        cursor = connection.cursor()
        cursor.execute("SELECT 1")
        cursor.fetchone()
        cursor.close()
        connection.close()
        end_time = datetime.now()
        time_taken = end_time - start_time
        return True, "Database connection successful", time_taken
    except Exception as e:
        end_time = datetime.now()
        time_taken = end_time - start_time
        return False, str(e), time_taken