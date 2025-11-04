import os
import time
from pathlib import Path
import psycopg2
from psycopg2.extras import RealDictCursor
from neo4j import GraphDatabase


# ---------- Connexions ----------
PG = dict(
    host=os.getenv("POSTGRES_HOST", "postgres"),
    dbname=os.getenv("POSTGRES_DB", "shop"),
    user=os.getenv("POSTGRES_USER", "postgres"),
    password=os.getenv("POSTGRES_PASSWORD", "postgres"),
)

NEO4J_URI = os.getenv("NEO4J_URI", "bolt://neo4j:7687")
NEO4J_USER = os.getenv("NEO4J_USER", "neo4j")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD", "password")


# ---------- Helpers demandés ----------
def run_cypher(session, query: str, **params):
    """Exécuter une requête Cypher avec paramètres."""
    return session.run(query, **params)


def run_cypher_file(session, filepath: Path):
    if not filepath.exists():
        return
    text = filepath.read_text(encoding="utf-8")
    # retirer lignes Browser (':something') et commentaires
    cleaned_lines = []
    for line in text.splitlines():
        l = line.strip()
        if not l or l.startswith("//") or l.startswith(":"):
            continue
        cleaned_lines.append(line)
    cleaned = "\n".join(cleaned_lines)

    # exécuter chaque statement terminé par ';'
    for stmt in [s.strip() for s in cleaned.split(";") if s.strip()]:
        session.run(stmt)


def chunk(iterable, size: int):
    """Découpe un itérable en lots de taille 'size' (pour batch insert)."""
    batch = []
    for item in iterable:
        batch.append(item)
        if len(batch) >= size:
            yield batch
            batch = []
    if batch:
        yield batch


def wait_for_postgres(timeout=120):
    """Attendre que Postgres accepte les connexions."""
    start = time.time()
    while True:
        try:
            with psycopg2.connect(**PG) as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT 1;")
            print("Postgres is ready.")
            return
        except Exception as e:
            if time.time() - start > timeout:
                raise RuntimeError(f"Postgres not ready after {timeout}s: {e}")
            time.sleep(2)


def wait_for_neo4j(timeout=120):
    """Attendre que Neo4j accepte les connexions Bolt."""
    start = time.time()
    while True:
        try:
            with GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD)) as driver:
                with driver.session() as s:
                    s.run("RETURN 1").consume()
            print("Neo4j is ready.")
            return
        except Exception as e:
            if time.time() - start > timeout:
                raise RuntimeError(f"Neo4j not ready after {timeout}s: {e}")
            time.sleep(2)


# ---------- ETL principal ----------
def etl():
    """
    ETL principal : Postgres -> Neo4j.

    Étapes :
      1) Attend Postgres & Neo4j
      2) (Optionnel) Exécute un fichier Cypher de schéma (queries.cypher) s'il existe
      3) Extrait les tables de Postgres
      4) Transforme (crée des catégories à partir de products.category)
      5) Charge dans Neo4j : Customer, Product, Order, Category + relations
    """
    print("ETL: waiting for dependencies...")
    wait_for_postgres()
    wait_for_neo4j()

    # --- Extraction Postgres ---
    print("ETL: reading from Postgres...")
    with psycopg2.connect(**PG) as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:

            cur.execute("SELECT id, name FROM customers;")
            customers = cur.fetchall()

            cur.execute("SELECT id, name, category FROM products;")
            products = cur.fetchall()

            cur.execute("SELECT id, customer_id FROM orders;")
            orders = cur.fetchall()

            cur.execute("SELECT order_id, product_id, qty FROM order_items;")
            order_items = cur.fetchall()

    # --- Transformations ---
    categories = sorted({(p["category"] or "").strip() for p in products if p["category"]})
    categories = [{"name": c} for c in categories]

    # --- Chargement Neo4j ---
    print("ETL: writing to Neo4j...")
    driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
    with driver.session() as s:
        # 0) (Optionnel) exécuter un fichier de schéma si présent
        queries_path = Path(__file__).with_name("queries.cypher")
        run_cypher_file(s, queries_path)

        # 1) Contraintes minimales
        run_cypher(s, "CREATE CONSTRAINT customer_id IF NOT EXISTS FOR (c:Customer) REQUIRE c.id IS UNIQUE")
        run_cypher(s, "CREATE CONSTRAINT product_id  IF NOT EXISTS FOR (p:Product)  REQUIRE p.id IS UNIQUE")
        run_cypher(s, "CREATE CONSTRAINT order_id    IF NOT EXISTS FOR (o:Order)    REQUIRE o.id IS UNIQUE")
        run_cypher(s, "CREATE CONSTRAINT category_pk IF NOT EXISTS FOR (c:Category) REQUIRE c.name IS UNIQUE")

        # 2) (option TP) Nettoyage si nécessaire – décommente pour repartir à blanc
        # run_cypher(s, "MATCH (n) DETACH DELETE n")

        # 3) Nœuds Category
        for batch in chunk(categories, 100):
            s.run("""
                UNWIND $rows AS row
                MERGE (:Category {name: row.name})
            """, rows=batch)

        # 4) Nœuds Product + relation IN_CATEGORY
        for batch in chunk(products, 100):
            s.run("""
                UNWIND $rows AS row
                MERGE (p:Product {id: row.id})
                  ON CREATE SET p.name = row.name, p.category = row.category
                  ON MATCH  SET p.name = row.name, p.category = row.category
                WITH p, row
                CALL {
                  WITH p, row
                  WITH p, row WHERE row.category IS NOT NULL AND row.category <> ''
                  MERGE (c:Category {name: row.category})
                  MERGE (p)-[:IN_CATEGORY]->(c)
                }
                RETURN count(*) AS c
            """, rows=batch)

        # 5) Nœuds Customer
        for batch in chunk(customers, 200):
            s.run("""
                UNWIND $rows AS row
                MERGE (:Customer {id: row.id, name: row.name})
            """, rows=batch)

        # 6) Nœuds Order + relation PLACED (Customer->Order)
        for batch in chunk(orders, 200):
            s.run("""
                UNWIND $rows AS row
                MERGE (o:Order {id: row.id})
                WITH o, row
                MATCH (c:Customer {id: row.customer_id})
                MERGE (c)-[:PLACED]->(o)
            """, rows=batch)

        # 7) Relations CONTAINS (Order->Product)
        for batch in chunk(order_items, 500):
            s.run("""
                UNWIND $rows AS row
                MATCH (o:Order {id: row.order_id})
                MATCH (p:Product {id: row.product_id})
                MERGE (o)-[r:CONTAINS]->(p)
                SET r.qty = COALESCE(r.qty, 0) + COALESCE(row.qty, 1)
            """, rows=batch)

    driver.close()
    print("ETL: done.")


if __name__ == "__main__":
    etl()
