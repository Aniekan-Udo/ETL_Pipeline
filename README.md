# MongoDB ETL Pipeline on Apache Airflow

A daily Apache Airflow pipeline that extracts bottle records from a MongoDB collection, cleans and
validates them with pandas, and loads the cleaned records into a separate analytics collection.
It runs on Astronomer's Astro Runtime in Docker.

```
MongoDB: rewards-ap-v3.BottleDetail
        │  extract  (Airflow MongoHook, connection "mongo_rewards_ap_v3")
        ▼
staging JSON file (/tmp, one per DAG run)
        │  transform (pandas)
        ▼
MongoDB: rewards-ap-v3.transformed_bottle_details
           load (insert_many)
```

## What the DAG does

The DAG is `mongo_etl_dag` in [`dags/Bottle_mapping.py`](dags/Bottle_mapping.py), written with Airflow's
TaskFlow API and scheduled `@daily` (no catch-up).

1. **Extract**: connects through an Airflow connection (`MongoHook`), so credentials live in Airflow,
   not in the code. It reads every document in `BottleDetail` and writes it to a staging JSON file
   whose name includes the DAG run, with a custom encoder for `datetime` and `ObjectId` values. Only the
   file path goes through XCom, not the data itself.
2. **Transform**: reads the staging file and deletes it, then checks that the data is a list of
   records before building a DataFrame. It converts `dateAdded`, `createdAt` and `updatedAt` to real
   timestamps (unparseable values become nulls instead of failing the run) and drops the source `_id`
   so the target collection assigns its own.
3. **Load**: inserts the cleaned records into `transformed_bottle_details` and logs how many were
   written. An empty extract is logged and skipped at each step, so it never fails the run.

Every task logs row counts, column types and a sample of the data, and closes its MongoDB client
even on failure.

## Run it locally

Requirements: Docker and the [Astro CLI](https://www.astronomer.io/docs/astro/cli/install-cli).

```bash
# 1. Start a local MongoDB (development credentials only)
docker compose up -d

# 2. Start Airflow (webserver, scheduler, Postgres metadata DB) on Astro Runtime
astro dev start
```

3. In the Airflow UI (http://localhost:8080), add a connection:
   - Connection ID: `mongo_rewards_ap_v3`
   - Type: MongoDB, plus the host, port and credentials of your MongoDB instance
4. Turn on `mongo_etl_dag`, or trigger it manually.

## Tests

`tests/dags/test_dag_example.py` and `.astro/test_dag_integrity_default.py` are Astronomer's DAG
integrity tests: they import every DAG and fail on import errors. Run them with `astro dev pytest`.

## Project layout

| Path | What it is |
|---|---|
| `dags/Bottle_mapping.py` | The ETL DAG |
| `dags/exampledag.py` | Astronomer's starter example DAG (not part of this pipeline) |
| `docker-compose.yml` | Local MongoDB for development |
| `Dockerfile` | Astro Runtime image |
| `requirements.txt` | Airflow, pymongo and the Airflow MongoDB provider |
| `tests/` | DAG integrity tests |

## Limitations and next steps

- Each run reloads the whole source collection, and the load appends, so rerunning inserts the same
  records again. Next step: an incremental extract on `updatedAt` and upserts keyed on the source ID.
- The target is MongoDB. For SQL reporting, the next step would be loading into a relational
  warehouse such as PostgreSQL.
