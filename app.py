from flask import Flask, jsonify, request
from neo4j import GraphDatabase
import os
import json

app = Flask(__name__)

URI = "bolt://localhost:7687"
USER = "neo4j"
PASSWORD = "easyPassword"
driver = GraphDatabase.driver(URI, auth=(USER, PASSWORD))

@app.route("/")
def home():
    return jsonify({"status": "running", "message": "CS 498 Homework 4 API"})

@app.route("/api/drivers/<driver_id>/company", methods=["GET"])
def get_driver_company(driver_id):
    query = """
    MATCH (d:Driver {driver_id: $driver_id})-[:WORKS_FOR]->(c:Company)
    RETURN c.name AS company
    """
    with driver.session() as session:
        result = session.run(query, driver_id=driver_id)
        record = result.single()
        if record:
            return jsonify({"driver_id": driver_id, "company": record["company"]})
        return jsonify({"error": "Driver not found"}), 404

@app.route("/api/companies/<company_name>/revenue", methods=["GET"])
def get_company_revenue(company_name):
    cypher = """
    MATCH (c:Company {name: $company_name})<-[:WORKS_FOR]-(d:Driver)-[t:TRIP]->(a:Area)
    RETURN sum(t.fare) AS total_revenue
    """
    with driver.session() as session:
        result = session.run(cypher, company_name=company_name)
        record = result.single()
        revenue = record["total_revenue"] if record and record["total_revenue"] is not None else 0.0
        return jsonify({"company": company_name, "total_revenue": round(float(revenue), 2)})

@app.route("/api/spark/summary", methods=["GET"])
def get_spark_summary():
    processed_dir = "processed_data"
    if not os.path.exists(processed_dir):
        return jsonify({"error": "Processed data not found"}), 404
        
    summary_data = []
    for filename in os.listdir(processed_dir):
        if filename.startswith("part-") and filename.endswith(".json"):
            filepath = os.path.join(processed_dir, filename)
            with open(filepath, "r") as f:
                for line in f:
                    if line.strip():
                        summary_data.append(json.loads(line))
                        
    return jsonify(summary_data)

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000)
