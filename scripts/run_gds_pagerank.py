import os
import logging
import pickle
import numpy as np
from neo4j import GraphDatabase, basic_auth
from neo4j.exceptions import ServiceUnavailable, Neo4jError

# --- Configuration ---
NEO4J_URI = "bolt://localhost:7687"
NEO4J_USER = "neo4j"
NEO4J_PASSWORD = "muttabbocks" # Replace with your password if different
GDS_GRAPH_NAME = "user_friends_graph_pagerank" # Use a distinct name or reuse carefully
PAGERANK_PROPERTY = "pagerankScore"
DISTRIBUTION_FILE = "../offline_assets/pagerank_distribution.pkl" # Store PageRank distribution
USEFUL_VOTE_DISTRIBUTION_FILE = "../offline_assets/useful_vote_distribution.pkl" # Store useful vote distribution

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- GDS Queries ---
CHECK_GRAPH_EXISTS_QUERY = f"""
CALL gds.graph.exists('{GDS_GRAPH_NAME}') YIELD exists
RETURN exists
"""

PROJECT_GRAPH_QUERY = f"""
CALL gds.graph.project(
    '{GDS_GRAPH_NAME}',
    'User',
    'FRIENDS'
)
YIELD graphName, nodeCount, relationshipCount
RETURN graphName, nodeCount, relationshipCount
"""

RUN_PAGERANK_QUERY = f"""
CALL gds.pageRank.write(
    '{GDS_GRAPH_NAME}',
    {{ writeProperty: '{PAGERANK_PROPERTY}' }}
)
YIELD nodePropertiesWritten, ranIterations
RETURN nodePropertiesWritten, ranIterations
"""

DROP_GRAPH_QUERY = f"""
CALL gds.graph.drop('{GDS_GRAPH_NAME}') YIELD graphName
RETURN graphName
"""

GET_PAGERANK_SCORES_QUERY = f"""
MATCH (u:User)
WHERE u.{PAGERANK_PROPERTY} IS NOT NULL
RETURN u.{PAGERANK_PROPERTY} AS score
"""

GET_USER_TOTAL_USEFUL_VOTES_QUERY = """
MATCH (u:User)-[:WROTE]->(r:Review)
WHERE r.useful > 0
RETURN u.user_id AS userId, sum(r.useful) AS totalUsefulVotes
"""

def run_pagerank(driver):
    """Projects the User-FRIENDS graph, runs PageRank, and writes pagerankScore."""
    logging.info("Starting PageRank calculation process...")

    with driver.session() as session:
        try:
            # 1. Check if graph projection exists and drop if it does
            logging.info(f"Checking for existing graph projection: {GDS_GRAPH_NAME}")
            result = session.run(CHECK_GRAPH_EXISTS_QUERY)
            graph_exists = result.single()[0]
            if graph_exists:
                logging.info(f"Graph projection '{GDS_GRAPH_NAME}' exists. Dropping it...")
                session.run(DROP_GRAPH_QUERY)
                logging.info(f"Dropped graph projection '{GDS_GRAPH_NAME}'.")

            # 2. Project the graph
            logging.info(f"Projecting graph '{GDS_GRAPH_NAME}'...")
            result = session.run(PROJECT_GRAPH_QUERY)
            summary = result.single()
            logging.info(f"Graph projected: Name='{summary['graphName']}', Nodes={summary['nodeCount']}, Relationships={summary['relationshipCount']}")

            # 3. Run PageRank and write results
            logging.info("Running PageRank algorithm...")
            result = session.run(RUN_PAGERANK_QUERY)
            summary = result.single()
            logging.info(f"PageRank complete: Properties written={summary['nodePropertiesWritten']}, Iterations={summary['ranIterations']}. Wrote '{PAGERANK_PROPERTY}' property to User nodes.")

        except Neo4jError as e:
            logging.error(f"A Neo4j error occurred during PageRank: {e.message} (Code: {e.code})")
            # Attempt cleanup
            try:
                result = session.run(CHECK_GRAPH_EXISTS_QUERY)
                if result.single()[0]:
                    logging.info("Attempting to drop graph projection after error...")
                    session.run(DROP_GRAPH_QUERY)
                    logging.info("Graph projection dropped.")
            except Exception as drop_e:
                logging.error(f"Failed to drop graph projection after error: {drop_e}")
            raise
        except Exception as e:
            logging.error(f"An unexpected error occurred during PageRank: {e}")
            raise
        finally:
            # 4. Drop the graph projection (ensure cleanup)
            try:
                logging.info(f"Attempting final cleanup: Dropping graph projection '{GDS_GRAPH_NAME}'...")
                result = session.run(CHECK_GRAPH_EXISTS_QUERY)
                if result.single()[0]:
                    session.run(DROP_GRAPH_QUERY)
                    logging.info(f"Successfully dropped graph projection '{GDS_GRAPH_NAME}'.")
                else:
                    logging.info("Graph projection does not exist, no need to drop.")
            except Exception as e:
                logging.error(f"Error during final graph drop: {e}")

def calculate_and_save_distribution(driver, output_path):
    """Fetches PageRank scores and saves their distribution."""
    logging.info(f"Fetching {PAGERANK_PROPERTY} scores from User nodes...")
    scores = []
    try:
        with driver.session() as session:
            result = session.run(GET_PAGERANK_SCORES_QUERY)
            scores = [record["score"] for record in result]
        
        if not scores:
            logging.warning("No PageRank scores found in the database. Cannot calculate distribution.")
            return

        logging.info(f"Fetched {len(scores)} scores. Calculating distribution...")
        # Store the sorted list of scores, which allows calculating any percentile later
        scores.sort()
        distribution_data = np.array(scores) # Use numpy array for potential efficiency

        # Ensure the output directory exists
        os.makedirs(os.path.dirname(output_path), exist_ok=True)

        logging.info(f"Saving distribution data to {output_path}...")
        with open(output_path, 'wb') as f:
            pickle.dump(distribution_data, f)
        logging.info("PageRank distribution data saved successfully.")

    except Neo4jError as e:
        logging.error(f"A Neo4j error occurred fetching PageRank scores: {e.message} (Code: {e.code})")
        raise
    except IOError as e:
        logging.error(f"Failed to write PageRank distribution file to {output_path}: {e}")
        raise
    except Exception as e:
        logging.error(f"An unexpected error occurred during PageRank distribution calculation/saving: {e}")
        raise

def calculate_and_save_useful_vote_distribution(driver, output_path):
    """Fetches total useful votes per user and saves their distribution."""
    logging.info("Fetching total useful votes per user...")
    votes = []
    try:
        with driver.session() as session:
            result = session.run(GET_USER_TOTAL_USEFUL_VOTES_QUERY)
            # We only need the distribution of vote counts, not user IDs
            votes = [record["totalUsefulVotes"] for record in result if record["totalUsefulVotes"] is not None]
        
        if not votes:
            logging.warning("No user useful vote counts found. Cannot calculate distribution.")
            return

        logging.info(f"Fetched total useful votes for {len(votes)} users. Calculating distribution...")
        # Store the sorted list of vote counts
        votes.sort()
        distribution_data = np.array(votes)

        # Ensure the output directory exists
        os.makedirs(os.path.dirname(output_path), exist_ok=True)

        logging.info(f"Saving useful vote distribution data to {output_path}...")
        with open(output_path, 'wb') as f:
            pickle.dump(distribution_data, f)
        logging.info("Useful vote distribution data saved successfully.")

    except Neo4jError as e:
        logging.error(f"A Neo4j error occurred fetching useful votes: {e.message} (Code: {e.code})")
        raise
    except IOError as e:
        logging.error(f"Failed to write useful vote distribution file to {output_path}: {e}")
        raise
    except Exception as e:
        logging.error(f"An unexpected error occurred during useful vote distribution calculation/saving: {e}")
        raise

if __name__ == "__main__":
    driver = None
    try:
        driver = GraphDatabase.driver(NEO4J_URI, auth=basic_auth(NEO4J_USER, NEO4J_PASSWORD))
        driver.verify_connectivity()
        logging.info("Successfully connected to Neo4j.")
        
        # Run PageRank calculation
        run_pagerank(driver)
        
        # Calculate and save the PageRank distribution
        calculate_and_save_distribution(driver, DISTRIBUTION_FILE)

        # Calculate and save the useful vote distribution
        calculate_and_save_useful_vote_distribution(driver, USEFUL_VOTE_DISTRIBUTION_FILE)
        
        logging.info("PageRank and Useful Vote distribution saving script finished successfully.")
        
    except ServiceUnavailable as e:
        logging.error(f"Could not connect to Neo4j at {NEO4J_URI}: {e}")
    except Exception as e:
        logging.error(f"Script failed: {e}")
    finally:
        if driver:
            driver.close()
            logging.info("Neo4j connection closed.")
