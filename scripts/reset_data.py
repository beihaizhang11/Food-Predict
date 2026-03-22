import os

import pymysql
from dotenv import load_dotenv
from neo4j import GraphDatabase

load_dotenv()


def reset_mysql() -> None:
    conn = pymysql.connect(
        host=os.getenv("MYSQL_HOST", "127.0.0.1"),
        user=os.getenv("MYSQL_USER", "root"),
        password=os.getenv("MYSQL_PASSWORD", ""),
        port=int(os.getenv("MYSQL_PORT", "3306")),
        database=os.getenv("MYSQL_DATABASE", "restaurant_analytics"),
        charset="utf8mb4",
        autocommit=True,
    )
    with conn.cursor() as cur:
        cur.execute("DELETE FROM analysis_cache")
        cur.execute("DELETE FROM reviews")
    conn.close()
    print("MySQL data cleared.")


def reset_neo4j() -> None:
    driver = GraphDatabase.driver(
        os.getenv("NEO4J_URI", "bolt://127.0.0.1:7687"),
        auth=(os.getenv("NEO4J_USER", "neo4j"), os.getenv("NEO4J_PASSWORD", "neo4j")),
    )
    with driver.session() as session:
        session.run("MATCH (n) DETACH DELETE n")
    driver.close()
    print("Neo4j graph cleared.")


if __name__ == "__main__":
    reset_mysql()
    reset_neo4j()
