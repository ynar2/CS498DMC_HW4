import os

from flask import Flask, jsonify, request
from neo4j import GraphDatabase
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

app = Flask(__name__)

NEO4J_URI = os.environ.get("NEO4J_URI", "bolt://localhost:7687")
NEO4J_USER = os.environ.get("NEO4J_USER", "neo4j")
NEO4J_PASSWORD = os.environ.get("NEO4J_PASSWORD", "easyPassword")

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CSV_PATH = os.path.join(BASE_DIR, "taxi_trips_clean.csv")

driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))

spark = (
    SparkSession.builder
    .appName("HW4-API")
    .master("local[*]")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")


def read_trips():
    """Read the cleaned CSV into a Spark DataFrame."""
    return spark.read.csv(CSV_PATH, header=True, inferSchema=True)


def bad_request(msg):
    return jsonify({"error": msg}), 400


@app.route("/")
def home():
    return jsonify({"status": "running"})

@app.route("/graph-summary", methods=["GET"])
def graph_summary():
    q = """
    CALL { MATCH (d:Driver)  RETURN count(d) AS driver_count }
    CALL { MATCH (c:Company) RETURN count(c) AS company_count }
    CALL { MATCH (a:Area)    RETURN count(a) AS area_count }
    CALL { MATCH ()-[t:TRIP]->() RETURN count(t) AS trip_count }
    RETURN driver_count, company_count, area_count, trip_count
    """
    with driver.session() as s:
        r = s.run(q).single()
    return jsonify({
        "driver_count": r["driver_count"],
        "company_count": r["company_count"],
        "area_count": r["area_count"],
        "trip_count": r["trip_count"],
    })


@app.route("/top-companies", methods=["GET"])
def top_companies():
    try:
        n = int(request.args.get("n", 5))
    except ValueError:
        return bad_request("n must be an integer")

    q = """
    MATCH (c:Company)<-[:WORKS_FOR]-(d:Driver)-[t:TRIP]->(:Area)
    RETURN c.name AS name, count(t) AS trip_count
    ORDER BY trip_count DESC, name ASC
    LIMIT $n
    """
    with driver.session() as s:
        rows = s.run(q, n=n)
        companies = [{"name": r["name"], "trip_count": r["trip_count"]} for r in rows]
    return jsonify({"companies": companies})


@app.route("/high-fare-trips", methods=["GET"])
def high_fare_trips():
    try:
        area_id = int(request.args["area_id"])
        min_fare = float(request.args["min_fare"])
    except (KeyError, ValueError):
        return bad_request("area_id (int) and min_fare (float) are required")

    q = """
    MATCH (d:Driver)-[t:TRIP]->(a:Area {area_id: $area_id})
    WHERE t.fare > $min_fare
    RETURN t.trip_id AS trip_id, t.fare AS fare, d.driver_id AS driver_id
    ORDER BY fare DESC, trip_id ASC
    """
    with driver.session() as s:
        rows = s.run(q, area_id=area_id, min_fare=min_fare)
        trips = [
            {"trip_id": r["trip_id"], "fare": r["fare"], "driver_id": r["driver_id"]}
            for r in rows
        ]
    return jsonify({"trips": trips})


@app.route("/co-area-drivers", methods=["GET"])
def co_area_drivers():
    driver_id = request.args.get("driver_id")
    if not driver_id:
        return bad_request("driver_id is required")

    q = """
    MATCH (d1:Driver {driver_id: $driver_id})-[:TRIP]->(a:Area)<-[:TRIP]-(d2:Driver)
    WHERE d1 <> d2
    RETURN d2.driver_id AS driver_id, count(DISTINCT a) AS shared_areas
    ORDER BY shared_areas DESC, driver_id ASC
    """
    with driver.session() as s:
        rows = s.run(q, driver_id=driver_id)
        result = [
            {"driver_id": r["driver_id"], "shared_areas": r["shared_areas"]}
            for r in rows
        ]
    return jsonify({"co_area_drivers": result})


@app.route("/avg-fare-by-company", methods=["GET"])
def avg_fare_by_company():
    q = """
    MATCH (c:Company)<-[:WORKS_FOR]-(d:Driver)-[t:TRIP]->(:Area)
    RETURN c.name AS name, avg(t.fare) AS avg_fare
    ORDER BY avg_fare DESC, name ASC
    """
    with driver.session() as s:
        rows = s.run(q)
        companies = [
            {"name": r["name"], "avg_fare": round(r["avg_fare"], 2)} for r in rows
        ]
    return jsonify({"companies": companies})

@app.route("/area-stats", methods=["GET"])
def area_stats():
    try:
        area_id = int(request.args["area_id"])
    except (KeyError, ValueError):
        return bad_request("area_id (int) is required")

    df = read_trips()
    row = (
        df.filter(F.col("dropoff_area") == area_id)
        .groupBy()
        .agg(
            F.count("*").alias("trip_count"),
            F.avg("fare").alias("avg_fare"),
            F.avg("trip_seconds").alias("avg_trip_seconds"),
        )
        .collect()[0]
    )

    trip_count = int(row["trip_count"])
    avg_fare = row["avg_fare"]
    avg_secs = row["avg_trip_seconds"]
    return jsonify({
        "area_id": area_id,
        "trip_count": trip_count,
        "avg_fare": round(avg_fare, 2) if avg_fare is not None else 0.0,
        "avg_trip_seconds": round(avg_secs, 2) if avg_secs is not None else 0.0,
    })


@app.route("/top-pickup-areas", methods=["GET"])
def top_pickup_areas():
    try:
        n = int(request.args.get("n", 5))
    except ValueError:
        return bad_request("n must be an integer")

    df = read_trips()
    rows = (
        df.groupBy("pickup_area")
        .agg(F.count("*").alias("trip_count"))
        .orderBy(F.desc("trip_count"), F.asc("pickup_area"))
        .limit(n)
        .collect()
    )
    areas = [
        {"pickup_area": int(r["pickup_area"]), "trip_count": int(r["trip_count"])}
        for r in rows
    ]
    return jsonify({"areas": areas})


@app.route("/company-compare", methods=["GET"])
def company_compare():
    company1 = request.args.get("company1")
    company2 = request.args.get("company2")
    if not company1 or not company2:
        return bad_request("company1 and company2 are required")

    df = read_trips().withColumn(
        "fare_per_minute", F.col("fare") / (F.col("trip_seconds") / 60.0)
    )
    df.createOrReplaceTempView("trips")

    def esc(s):
        return s.replace("'", "''")

    rows = spark.sql(f"""
        SELECT company,
               COUNT(*)                         AS trip_count,
               ROUND(AVG(fare), 2)              AS avg_fare,
               ROUND(AVG(fare_per_minute), 2)   AS avg_fare_per_minute,
               ROUND(AVG(trip_seconds), 2)      AS avg_trip_seconds
        FROM trips
        WHERE company IN ('{esc(company1)}', '{esc(company2)}')
        GROUP BY company
    """).collect()

    by_name = {r["company"]: r for r in rows}
    if company1 not in by_name or company2 not in by_name:
        return jsonify({"error": "one or more companies not found"})

    comparison = []
    for name in (company1, company2):
        r = by_name[name]
        comparison.append({
            "company": name,
            "trip_count": int(r["trip_count"]),
            "avg_fare": float(r["avg_fare"]),
            "avg_fare_per_minute": float(r["avg_fare_per_minute"]),
            "avg_trip_seconds": float(r["avg_trip_seconds"]),
        })
    return jsonify({"comparison": comparison})


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000, threaded=True)
