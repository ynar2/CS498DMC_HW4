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

@app.route("/graph-summary", methods=["GET"])
def graph_summary():
    query = """
    MATCH (d:Driver)
    OPTIONAL MATCH (c:Company)
    OPTIONAL MATCH (a:Area)
    RETURN count(DISTINCT d) AS driver_count, count(DISTINCT c) AS company_count, count(DISTINCT a) AS area_count
    """
    with driver.session() as session:
        record = session.run(query).single()
        return jsonify({
            "drivers": record["driver_count"],
            "companies": record["company_count"],
            "areas": record["area_count"]
        })

@app.route("/top-companies", methods=["GET"])
def top_companies():
    n = int(request.args.get("n", 5))
    query = """
    MATCH (c:Company)<-[:WORKS_FOR]-(d:Driver)-[t:TRIP]->(a:Area)
    RETURN c.name AS company, sum(t.fare) AS total_revenue
    ORDER BY total_revenue DESC
    LIMIT $n
    """
    with driver.session() as session:
        results = session.run(query, n=n)
        data = [{"company": r["company"], "total_revenue": r["total_revenue"]} for r in results]
        return jsonify(data)

@app.route("/high-fare-trips", methods=["GET"])
def high_fare_trips():
    area_id = request.args.get("area_id")
    min_fare = float(request.args.get("min_fare", 0.0))
    query = """
    MATCH (d:Driver)-[t:TRIP]->(a:Area {area_id: $area_id})
    WHERE t.fare >= $min_fare
    RETURN t.trip_id AS trip_id, t.fare AS fare, d.driver_id AS driver_id
    LIMIT 50
    """
    with driver.session() as session:
        results = session.run(query, area_id=area_id, min_fare=min_fare)
        data = [{"trip_id": r["trip_id"], "fare": r["fare"], "driver_id": r["driver_id"]} for r in results]
        return jsonify(data)

@app.route("/co-area-drivers", methods=["GET"])
def co_area_drivers():
    driver_id = request.args.get("driver_id")
    query = """
    MATCH (d1:Driver {driver_id: $driver_id})-[t1:TRIP]->(a:Area)<-[t2:TRIP]-(d2:Driver)
    WHERE d1 <> d2
    RETURN DISTINCT d2.driver_id AS co_driver_id
    LIMIT 20
    """
    with driver.session() as session:
        results = session.run(query, driver_id=driver_id)
        data = [r["co_driver_id"] for r in results]
        return jsonify(data)

@app.route("/avg-fare-by-company", methods=["GET"])
def avg_fare_by_company():
    query = """
    MATCH (c:Company)<-[:WORKS_FOR]-(d:Driver)-[t:TRIP]->(a:Area)
    RETURN c.name AS company, avg(t.fare) AS avg_fare
    """
    with driver.session() as session:
        results = session.run(query)
        data = [{"company": r["company"], "avg_fare": r["avg_fare"]} for r in results]
        return jsonify(data)

@app.route("/area-stats", methods=["GET"])
def area_stats():
    area_id = request.args.get("area_id")
    query = """
    MATCH (d:Driver)-[t:TRIP]->(a:Area {area_id: $area_id})
    RETURN count(t) AS trip_count, avg(t.fare) AS avg_fare, sum(t.fare) AS total_revenue
    """
    with driver.session() as session:
        record = session.run(query, area_id=area_id).single()
        if record:
            return jsonify({
                "area_id": area_id,
                "trip_count": record["trip_count"],
                "avg_fare": record["avg_fare"] if record["avg_fare"] else 0.0,
                "total_revenue": record["total_revenue"] if record["total_revenue"] else 0.0
            })
        return jsonify({"error": "Area not found"}), 404

@app.route("/top-pickup-areas", methods=["GET"])
def top_pickup_areas():
    n = int(request.args.get("n", 5))
    query = """
    MATCH (d:Driver)-[t:TRIP]->(a:Area)
    RETURN a.area_id AS area_id, count(t) AS trip_count
    ORDER BY trip_count DESC
    LIMIT $n
    """
    with driver.session() as session:
        results = session.run(query, n=n)
        data = [{"area_id": r["area_id"], "trip_count": r["trip_count"]} for r in results]
        return jsonify(data)

@app.route("/company-compare", methods=["GET"])
def company_compare():
    company1 = request.args.get("company1")
    company2 = request.args.get("company2")
    query = """
    MATCH (c:Company)<-[:WORKS_FOR]-(d:Driver)-[t:TRIP]->(a:Area)
    WHERE c.name IN [$c1, $c2]
    RETURN c.name AS company, sum(t.fare) AS total_revenue, count(t) AS trip_count
    """
    with driver.session() as session:
        results = session.run(query, c1=company1, c2=company2)
        data = [{"company": r["company"], "total_revenue": r["total_revenue"], "trip_count": r["trip_count"]} for r in results]
        return jsonify(data)


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000)
