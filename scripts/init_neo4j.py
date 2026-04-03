import os

from dotenv import load_dotenv
from neo4j import GraphDatabase

load_dotenv()

NEO4J_URI = os.getenv("NEO4J_URI", "bolt://127.0.0.1:7687")
NEO4J_USER = os.getenv("NEO4J_USER", "neo4j")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD", "neo4j")


def main():
    driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
    statements = [
        "CREATE CONSTRAINT shop_id IF NOT EXISTS FOR (s:Shop) REQUIRE s.id IS UNIQUE",
        "CREATE CONSTRAINT review_id IF NOT EXISTS FOR (r:Review) REQUIRE r.id IS UNIQUE",
        "CREATE CONSTRAINT factor_name IF NOT EXISTS FOR (f:Factor) REQUIRE f.name IS UNIQUE",
    ]
    with driver.session() as session:
        for stmt in statements:
            session.run(stmt)
    driver.close()
    print("Neo4j constraints initialized.")


if __name__ == "__main__":
    main()
