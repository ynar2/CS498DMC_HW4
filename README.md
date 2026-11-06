# CS 498 — Homework 4: Graph Databases & Spark

This project loads the Chicago taxi trip dataset into a Neo4j property graph, preprocesses it with PySpark, and exposes both through a Flask API running on a GCP VM (Ubuntu 22.04) at port 8000.

## Repository Contents

| File | Description |
|---|---|
| `Team.txt` | Team member NetIDs |
| `HW4.txt` | External IP and port of the GCP VM |
| `password.txt` | Autograder password |
| `load_graph.py` | Loads `taxi_trips_clean.csv` into Neo4j (`:Driver`, `:Company`, `:Area` nodes; `:WORKS_FOR` and `:TRIP` relationships) |
| `preprocess.py` | PySpark preprocessing: adds `fare_per_minute`, computes a company-level summary with Spark SQL, writes JSON to `processed_data/` |
| `app.py` | Flask API with the five Neo4j endpoints and three PySpark endpoints |
| `fdb_answers.txt` | Part 3 FoundationDB answers |
| `Design.pdf` | Part 4 written analysis |

## Graph Schema

**Nodes**
- `:Driver {driver_id: string}`
- `:Company {name: string}`
- `:Area {area_id: integer}`

**Relationships**
- `(:Driver)-[:WORKS_FOR]->(:Company)`
- `(:Driver)-[:TRIP {trip_id: string, fare: float, trip_seconds: int}]->(:Area)`, which points to the trip's dropoff area

Nodes are created with `MERGE`, with uniqueness constraints on `driver_id`, `name`, and `area_id`.

## Running

```bash
# 1. Clean the raw data (provided clean.py) -> taxi_trips_clean.csv
python3 clean.py

# 2. Load the graph into Neo4j
python3 load_graph.py

# 3. Run PySpark preprocessing -> processed_data/
python3 preprocess.py

# 4. Start the API (Neo4j password supplied via environment variable)
export NEO4J_PASSWORD='<neo4j password>'
nohup python3 app.py > app.log 2>&1 &
```

## API Endpoints

All endpoints return JSON.

### Neo4j (Part 1)

| Endpoint | Description |
|---|---|
| `GET /graph-summary` | Counts of Driver, Company, and Area nodes and of TRIP relationships |
| `GET /top-companies?n=<int>` | Top `n` companies by number of trips made by their drivers |
| `GET /high-fare-trips?area_id=<int>&min_fare=<float>` | Trips to a dropoff area with fare greater than `min_fare`, sorted by fare descending |
| `GET /co-area-drivers?driver_id=<string>` | Other drivers sharing at least one dropoff area, with shared-area counts, sorted descending |
| `GET /avg-fare-by-company` | Average fare per company (2 decimals), sorted descending |

### PySpark (Part 2.2)

| Endpoint | Description |
|---|---|
| `GET /area-stats?area_id=<int>` | Trip count, average fare, and average trip duration for a dropoff area (`filter` + `groupBy().agg()`) |
| `GET /top-pickup-areas?n=<int>` | Top `n` pickup areas by trip count (`groupBy().agg().orderBy()`) |
| `GET /company-compare?company1=<str>&company2=<str>` | Side-by-side metrics for two companies, computed with a single `spark.sql()` query using `WHERE company IN (...)`. Returns `{"error": "one or more companies not found"}` if either company is missing |

## Part 2.1 — PySpark Preprocessing Output

`preprocess.py` reads `taxi_trips_clean.csv` with `spark.read.csv(header=True, inferSchema=True)`, adds `fare_per_minute = fare / (trip_seconds / 60.0)` using `withColumn()`, registers the DataFrame as the temp view `trips`, runs the company summary query with `spark.sql()`, and writes the result to `processed_data/` as JSON (one record per line).

**`processed_data/` contents:**

```
$ ls -l processed_data/
total 4
-rw-r--r-- 1 ymoinikunta ymoinikunta    0 Nov  6 20:52 _SUCCESS
-rw-r--r-- 1 ymoinikunta ymoinikunta 1709 Nov  6 20:52 part-00000-77e14019-0947-4b12-b09e-3939449857b6-c000.json
```

**Contents of the part file:**

```
$ cat processed_data/part-*.json
<paste output here>
```

**Screenshot:**

<img width="891" height="56" alt="processed_data folder listing" src="https://github.com/user-attachments/assets/5c5cc6ee-4ca4-46a1-93f8-5c19ab16cd0b" />
