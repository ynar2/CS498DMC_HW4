import pandas as pd
from neo4j import GraphDatabase

URI = "bolt://localhost:7687"
USER = "neo4j"
PASSWORD = "easyPassword"

driver = GraphDatabase.driver(URI, auth=(USER, PASSWORD))

def setup_constraints(session):
    # Setup constraints for unique property lookups
    session.run("CREATE CONSTRAINT IF NOT EXISTS FOR (d:Driver) REQUIRE d.driver_id IS UNIQUE")
    session.run("CREATE CONSTRAINT IF NOT EXISTS FOR (c:Company) REQUIRE c.name IS UNIQUE")
    session.run("CREATE CONSTRAINT IF NOT EXISTS FOR (a:Area) REQUIRE a.area_id IS UNIQUE")

def load_batch(tx, rows):
    cypher = """
    UNWIND $rows AS row
    MERGE (d:Driver {driver_id: row.driver_id})
    MERGE (c:Company {name: row.company})
    MERGE (d)-[:WORKS_FOR]->(c)
    MERGE (a:Area {area_id: row.dropoff_area})
    CREATE (d)-[:TRIP {
        trip_id: row.trip_id,
        fare: toFloat(row.fare),
        trip_seconds: toInteger(row.trip_seconds)
    }]->(a)
    """
    tx.run(cypher, rows=rows)

def load_data():
    df = pd.read_csv("taxi_trips_clean.csv")
    records = df.to_dict(orient="records")
    batch_size = 1000

    with driver.session() as session:
        setup_constraints(session)
        print("Constraints created.")

        for i in range(0, len(records), batch_size):
            batch = records[i:i + batch_size]
            session.execute_write(load_batch, batch)
            print(f"Loaded batch {i // batch_size + 1}/{(len(records) + batch_size - 1) // batch_size}")

if __name__ == "__main__":
    load_data()
    driver.close()
    print("Graph loading complete!")
