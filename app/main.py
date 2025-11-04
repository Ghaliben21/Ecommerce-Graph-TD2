import os
from fastapi import FastAPI, Query
from neo4j import GraphDatabase

NEO4J_URI = os.getenv("NEO4J_URI", "bolt://localhost:7687")
NEO4J_USER = os.getenv("NEO4J_USER", "neo4j")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD", "password")

driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
app = FastAPI(title="E-Commerce Graph Reco API", version="0.1.0")

@app.get("/health")
def health():
    try:
        with driver.session() as s:
            s.run("RETURN 1")
        return {"status": "ok"}
    except Exception as e:
        return {"status": "error", "detail": str(e)}

@app.get("/recommendations/{customer_id}")
def recommend_for_customer(customer_id: int, limit: int = Query(5, ge=1, le=50)):
    cypher = """
    MATCH (c:Customer {id:$customer_id})-[:PLACED]->(:Order)-[:CONTAINS]->(p:Product)
    MATCH (p)<-[:CONTAINS]-(:Order)<-[:PLACED]-(other:Customer)
    WHERE other <> c
    MATCH (other)-[:PLACED]->(:Order)-[:CONTAINS]->(rec:Product)
    WHERE NOT (c)-[:PLACED]->(:Order)-[:CONTAINS]->(:Product {id: rec.id})
    RETURN rec.id AS product_id, rec.name AS product_name, count(*) AS score
    ORDER BY score DESC
    LIMIT $limit
    """
    with driver.session() as s:
        rows = s.run(cypher, customer_id=customer_id, limit=limit)
        return [r.data() for r in rows]

@app.get("/similar/{product_id}")
def similar_products(product_id: int, limit: int = Query(5, ge=1, le=50)):
    cypher = """
    MATCH (p:Product {id:$product_id})<-[:CONTAINS]-(o:Order)-[:CONTAINS]->(rec:Product)
    WHERE rec <> p
    RETURN rec.id AS product_id, rec.name AS product_name, count(*) AS score
    ORDER BY score DESC
    LIMIT $limit
    """
    with driver.session() as s:
        rows = s.run(cypher, product_id=product_id, limit=limit)
        return [r.data() for r in rows]

@app.get("/")
def root():
    return {"status": "ok", "docs": "/docs"}