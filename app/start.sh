#!/usr/bin/env bash
set -e

pip install --no-cache-dir -r requirements.txt

# Lance l’ETL (Postgres -> Neo4j)
python etl.py

# Démarre l’API
uvicorn main:app --host 0.0.0.0 --port 8000
