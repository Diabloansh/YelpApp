import os
import logging
import pickle
import numpy as np
from scipy import stats
from neo4j import GraphDatabase, basic_auth
from neo4j.exceptions import ServiceUnavailable, Neo4jError

# --- Configuration ---
NEO4J_URI = "bolt://localhost:7687"
NEO4J_USER = "neo4j"
NEO4J_PASSWORD = "muttabbocks"  # Replace with your password if different
PAGERANK_PROPERTY = "pagerankScore"  # Property name on User nodes for PageRank

# Input distribution files
PAGERANK_DISTRIBUTION_FILE = "../offline_assets/pagerank_distribution.pkl"
USEFUL_VOTE_DISTRIBUTION_FILE = "../offline_assets/useful_vote_distribution.pkl"

# Output distribution file
COMPOSITE_METRIC_DISTRIBUTION_FILE = "../offline_assets/composite_metric_distribution.pkl"

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- Neo4j Query ---
GET_USER_RAW_SCORES_QUERY = f"""
MATCH (u:User)
OPTIONAL MATCH (u)-[:WROTE]->(r:Review)
RETURN u.user_id AS userId,
       u.{PAGERANK_PROPERTY} AS rawPagerank,
       COALESCE(sum(r.useful), 0) AS rawTotalUsefulVotes
"""

def calculate_and_save_composite_metric_distribution(driver):
    """
    Calculates the composite metric (pagerank_percentile * useful_vote_percentile)
    for all users and saves its distribution.
    """
    logging.info("Starting composite metric distribution calculation...")

    # 1. Load prerequisite distributions
    try:
        logging.info(f"Loading PageRank distribution from {PAGERANK_DISTRIBUTION_FILE}...")
        with open(PAGERANK_DISTRIBUTION_FILE, 'rb') as f:
            pagerank_scores_list = pickle.load(f)
        if not isinstance(pagerank_scores_list, np.ndarray):
            pagerank_scores_list = np.array(pagerank_scores_list)
        logging.info(f"Loaded PageRank distribution with {len(pagerank_scores_list)} scores.")
    except FileNotFoundError:
        logging.error(f"PageRank distribution file not found: {PAGERANK_DISTRIBUTION_FILE}. Cannot proceed.")
        return
    except Exception as e:
        logging.error(f"Error loading PageRank distribution: {e}")
        return

    try:
        logging.info(f"Loading useful vote distribution from {USEFUL_VOTE_DISTRIBUTION_FILE}...")
        with open(USEFUL_VOTE_DISTRIBUTION_FILE, 'rb') as f:
            useful_votes_list = pickle.load(f)
        if not isinstance(useful_votes_list, np.ndarray):
            useful_votes_list = np.array(useful_votes_list)
        logging.info(f"Loaded useful vote distribution with {len(useful_votes_list)} counts.")
    except FileNotFoundError:
        logging.error(f"Useful vote distribution file not found: {USEFUL_VOTE_DISTRIBUTION_FILE}. Cannot proceed.")
        return
    except Exception as e:
        logging.error(f"Error loading useful vote distribution: {e}")
        return

    if len(pagerank_scores_list) == 0:
        logging.warning("PageRank distribution is empty. Composite metrics may not be meaningful.")
    if len(useful_votes_list) == 0:
        logging.warning("Useful vote distribution is empty. Composite metrics may not be meaningful.")

    all_composite_metrics = []
    logging.info("Fetching user raw scores from Neo4j...")

    try:
        with driver.session() as session:
            results = session.run(GET_USER_RAW_SCORES_QUERY)
            user_count = 0
            for record in results:
                user_id = record["userId"]
                user_raw_pagerank = record["rawPagerank"]
                user_raw_useful_votes = record["rawTotalUsefulVotes"]
                user_count += 1

                if user_raw_pagerank is None:
                    logging.warning(f"User {user_id} has null PageRank score. Skipping for composite metric.")
                    continue

                # Calculate PageRank percentile (0-1 range)
                if len(pagerank_scores_list) > 0:
                    pagerank_percentile = stats.percentileofscore(pagerank_scores_list, user_raw_pagerank, kind='rank') / 100.0
                else:
                    pagerank_percentile = 0.0  # Default if distribution is empty

                # Calculate useful vote percentile (0-1 range)
                if len(useful_votes_list) > 0:
                    useful_vote_percentile = stats.percentileofscore(useful_votes_list, user_raw_useful_votes, kind='rank') / 100.0
                else:
                    useful_vote_percentile = 0.0 # Default if distribution is empty
                
                composite_metric = pagerank_percentile * useful_vote_percentile
                all_composite_metrics.append(composite_metric)

                if user_count % 10000 == 0: # Log progress
                    logging.info(f"Processed {user_count} users for composite metrics...")
            
            logging.info(f"Finished processing {user_count} users.")

    except Neo4jError as e:
        logging.error(f"Neo4j error fetching user scores: {e.message} (Code: {e.code})")
        return
    except Exception as e:
        logging.error(f"Unexpected error fetching user scores: {e}", exc_info=True)
        return

    if not all_composite_metrics:
        logging.warning("No composite metrics were calculated. Output file will not be created.")
        return

    # Sort the composite metrics before saving
    all_composite_metrics.sort()
    composite_distribution_data = np.array(all_composite_metrics)

    # Ensure the output directory exists
    try:
        os.makedirs(os.path.dirname(COMPOSITE_METRIC_DISTRIBUTION_FILE), exist_ok=True)
        logging.info(f"Saving composite metric distribution to {COMPOSITE_METRIC_DISTRIBUTION_FILE}...")
        with open(COMPOSITE_METRIC_DISTRIBUTION_FILE, 'wb') as f:
            pickle.dump(composite_distribution_data, f)
        logging.info(f"Composite metric distribution data saved successfully with {len(composite_distribution_data)} entries.")
    except IOError as e:
        logging.error(f"Failed to write composite metric distribution file: {e}")
    except Exception as e:
        logging.error(f"Unexpected error saving composite metric distribution: {e}", exc_info=True)


if __name__ == "__main__":
    driver = None
    try:
        driver = GraphDatabase.driver(NEO4J_URI, auth=basic_auth(NEO4J_USER, NEO4J_PASSWORD))
        driver.verify_connectivity()
        logging.info("Successfully connected to Neo4j.")
        
        calculate_and_save_composite_metric_distribution(driver)
        
        logging.info("Composite metric distribution script finished.")
        
    except ServiceUnavailable as e:
        logging.error(f"Could not connect to Neo4j at {NEO4J_URI}: {e}")
    except Exception as e:
        logging.error(f"Script failed: {e}", exc_info=True)
    finally:
        if driver:
            driver.close()
            logging.info("Neo4j connection closed.")
