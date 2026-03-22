import os

import pymysql
from dotenv import load_dotenv

load_dotenv()

MYSQL_HOST = os.getenv("MYSQL_HOST", "127.0.0.1")
MYSQL_PORT = int(os.getenv("MYSQL_PORT", "3306"))
MYSQL_USER = os.getenv("MYSQL_USER", "root")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD", "")
MYSQL_DATABASE = os.getenv("MYSQL_DATABASE", "restaurant_analytics")


def main():
    conn = pymysql.connect(
        host=MYSQL_HOST,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        port=MYSQL_PORT,
        charset="utf8mb4",
        autocommit=True,
    )
    with conn.cursor() as cur:
        cur.execute(f"CREATE DATABASE IF NOT EXISTS `{MYSQL_DATABASE}` CHARACTER SET utf8mb4")
        cur.execute(f"USE `{MYSQL_DATABASE}`")
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS reviews (
                id INT PRIMARY KEY,
                user_id VARCHAR(32),
                shop_id VARCHAR(32),
                dish VARCHAR(64),
                rating FLOAT,
                review_text TEXT,
                review_time DATETIME,
                tags VARCHAR(255),
                sentiment FLOAT,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                INDEX idx_review_time (review_time),
                INDEX idx_shop_id (shop_id),
                INDEX idx_dish (dish)
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS analysis_cache (
                review_id INT PRIMARY KEY,
                embedding_json LONGTEXT NULL,
                entities_json LONGTEXT NULL,
                keywords_json LONGTEXT NULL,
                updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                CONSTRAINT fk_review_id FOREIGN KEY (review_id) REFERENCES reviews(id) ON DELETE CASCADE
            )
            """
        )
    conn.close()
    print("MySQL schema initialized.")


if __name__ == "__main__":
    main()
