import logging
from neo4j import GraphDatabase, basic_auth
from neo4j.exceptions import ServiceUnavailable, Neo4jError

# --- Configuration -----------------------------------------------------------
NEO4J_URI      = "bolt://localhost:7687"
NEO4J_USER     = "neo4j"
NEO4J_PASSWORD = "muttabbocks"
GDS_GRAPH_NAME = "user_friends_graph"
GAMMA          = 0.5             # <‑‑ lower → bigger communities, higher → smaller

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(levelname)s %(message)s")

# --- Cypher / GDS queries ----------------------------------------------------
CHECK_GRAPH_EXISTS = f"""
CALL gds.graph.exists('{GDS_GRAPH_NAME}') YIELD exists
RETURN exists
"""

PROJECT_GRAPH = f"""
CALL gds.graph.project(
  '{GDS_GRAPH_NAME}',
  'User',
  {{
    FRIENDS: {{
      type: 'FRIENDS',
      orientation: 'UNDIRECTED'
    }}
  }}
)
YIELD graphName, nodeCount, relationshipCount
RETURN graphName, nodeCount, relationshipCount
"""

RUN_LEIDEN = f"""
CALL gds.leiden.write(
  '{GDS_GRAPH_NAME}',
  {{
    writeProperty: 'clusterId',
    gamma: {GAMMA}
  }}
)
YIELD communityCount, modularity
RETURN communityCount, modularity
"""

DROP_GRAPH = f"""
CALL gds.graph.drop('{GDS_GRAPH_NAME}') YIELD graphName
RETURN graphName
"""

# --- Driver logic ------------------------------------------------------------
def run_leiden_clustering(driver):
    logging.info("Starting Leiden clustering…")

    with driver.session() as session:
        try:
            # 1 · Drop existing projection if present
            if session.run(CHECK_GRAPH_EXISTS).single()[0]:
                logging.info("Dropping old projection…")
                session.run(DROP_GRAPH)

            # 2 · Project User‑FRIENDS graph (undirected)
            logging.info("Projecting graph…")
            meta = session.run(PROJECT_GRAPH).single()
            logging.info(f"Projected: {meta['nodeCount']} nodes, "
                         f"{meta['relationshipCount']} relationships")

            # 3 · Run Leiden
            logging.info(f"Running Leiden (gamma ={GAMMA}) …")
            stats = session.run(RUN_LEIDEN).single()
            logging.info(f"Leiden done – {stats['communityCount']} communities, "
                         f"modularity {stats['modularity']:.4f}")

        except Neo4jError as e:
            logging.error(f"Neo4j error {e.code}: {e.message}")
            raise
        finally:
            # Always drop projection to free memory
            if session.run(CHECK_GRAPH_EXISTS).single()[0]:
                logging.info("Cleaning up projection…")
                session.run(DROP_GRAPH)
                logging.info("Projection dropped.")

# --- Main --------------------------------------------------------------------
if __name__ == "__main__":
    driver = None
    try:
        driver = GraphDatabase.driver(
            NEO4J_URI, auth=basic_auth(NEO4J_USER, NEO4J_PASSWORD))
        driver.verify_connectivity()
        logging.info("Connected to Neo4j.")
        run_leiden_clustering(driver)
        logging.info("Leiden clustering finished.")
    except ServiceUnavailable as e:
        logging.error(f"Cannot reach Neo4j @ {NEO4J_URI}: {e}")
    except Exception as e:
        logging.error(f"Script failed: {e}")
    finally:
        if driver:
            driver.close()
            logging.info("Neo4j connection closed.")
