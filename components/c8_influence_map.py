import os
import logging
import pickle
from pathlib import Path
import numpy as np
from scipy import stats # For percentile calculation (percentileofscore)
from neo4j import GraphDatabase, basic_auth
from neo4j.exceptions import ServiceUnavailable, Neo4jError
from typing import Optional

# --- Configuration ---
# Determine the absolute path to the project's root directory
PROJECT_ROOT = Path(__file__).resolve().parent.parent

NEO4J_URI = "bolt://localhost:7687"
NEO4J_USER = "neo4j"
NEO4J_PASSWORD = "muttabbocks" # Replace with your password if different
PAGERANK_PROPERTY = "pagerankScore"
PAGERANK_DISTRIBUTION_FILE = PROJECT_ROOT / "offline_assets" / "pagerank_distribution.pkl"
USEFUL_VOTE_DISTRIBUTION_FILE = PROJECT_ROOT / "offline_assets" / "useful_vote_distribution.pkl"
COMPOSITE_METRIC_DISTRIBUTION_FILE = PROJECT_ROOT / "offline_assets" / "composite_metric_distribution.pkl"

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- Load Precomputed Distributions ---
try:
    logging.info(f"Loading PageRank distribution from {PAGERANK_DISTRIBUTION_FILE}...")
    with open(PAGERANK_DISTRIBUTION_FILE, 'rb') as f:
        pagerank_distribution = pickle.load(f)
    logging.info(f"Loaded PageRank distribution with {len(pagerank_distribution)} scores.")
    # Ensure it's a numpy array if needed, though list might be fine for percentileofscore
    if not isinstance(pagerank_distribution, np.ndarray):
         pagerank_distribution = np.array(pagerank_distribution)
except FileNotFoundError:
    logging.error(f"PageRank distribution file not found: {PAGERANK_DISTRIBUTION_FILE}. Run run_gds_pagerank.py first.")
    pagerank_distribution = None
except Exception as e:
    logging.error(f"Error loading PageRank distribution: {e}")
    pagerank_distribution = None

try:
    logging.info(f"Loading useful vote distribution from {USEFUL_VOTE_DISTRIBUTION_FILE}...")
    with open(USEFUL_VOTE_DISTRIBUTION_FILE, 'rb') as f:
        useful_vote_distribution = pickle.load(f)
    logging.info(f"Loaded useful vote distribution with {len(useful_vote_distribution)} counts.")
    if not isinstance(useful_vote_distribution, np.ndarray):
         useful_vote_distribution = np.array(useful_vote_distribution)
except FileNotFoundError:
    logging.error(f"Useful vote distribution file not found: {USEFUL_VOTE_DISTRIBUTION_FILE}. Run run_gds_pagerank.py first.")
    useful_vote_distribution = None
except Exception as e:
    logging.error(f"Error loading useful vote distribution: {e}")
    useful_vote_distribution = None

try:
    logging.info(f"Loading composite metric distribution from {COMPOSITE_METRIC_DISTRIBUTION_FILE}...")
    with open(COMPOSITE_METRIC_DISTRIBUTION_FILE, 'rb') as f:
        composite_metric_distribution = pickle.load(f)
    logging.info(f"Loaded composite metric distribution with {len(composite_metric_distribution)} values.")
    if not isinstance(composite_metric_distribution, np.ndarray):
         composite_metric_distribution = np.array(composite_metric_distribution)
except FileNotFoundError:
    logging.error(f"Composite metric distribution file not found: {COMPOSITE_METRIC_DISTRIBUTION_FILE}. Run the script to generate it first.")
    composite_metric_distribution = None
except Exception as e:
    logging.error(f"Error loading composite metric distribution: {e}")
    composite_metric_distribution = None

# --- Neo4j Queries ---
GET_USER_PAGERANK_QUERY = f"""
MATCH (u:User {{user_id: $userId}})
RETURN u.{PAGERANK_PROPERTY} AS pagerankScore
LIMIT 1
"""

GET_USER_TOTAL_USEFUL_VOTES_QUERY = """
MATCH (u:User {user_id: $userId})-[:WROTE]->(r:Review)
WHERE r.useful > 0
RETURN sum(r.useful) AS totalUsefulVotes
"""

# --- Main Function ---

def get_overall_influence_percentile(driver, user_id: str) -> Optional[float]:
    """
    Calculates the user's overall influence percentile.
    This is based on their composite metric (pagerank_percentile * useful_vote_percentile),
    compared against a precomputed distribution of such composite metrics.

    Args:
        driver: The Neo4j driver instance.
        user_id: The ID of the user (prefixed, e.g., 'u-xxxxx').

    Returns:
        The user's overall influence percentile (0-100), or None if prerequisites
        are missing, user data is missing, or an error occurs.
    """
    logging.info(f"Calculating overall influence percentile for user: {user_id}")

    if pagerank_distribution is None or \
       useful_vote_distribution is None or \
       composite_metric_distribution is None:
        logging.error("One or more precomputed distributions (PageRank, Useful Vote, Composite Metric) not loaded. Cannot calculate percentile.")
        return None

    user_pagerank = None
    user_total_useful = 0 # Default to 0 if no useful votes found

    try:
        with driver.session() as session:
            # 1. Get user's PageRank score
            result_pr = session.run(GET_USER_PAGERANK_QUERY, userId=user_id)
            record_pr = result_pr.single()

            if record_pr and record_pr[PAGERANK_PROPERTY] is not None:
                user_pagerank = record_pr[PAGERANK_PROPERTY]
                if not isinstance(user_pagerank, float): # GDS PageRank is usually float
                    try:
                        user_pagerank = float(user_pagerank)
                    except (ValueError, TypeError):
                        logging.error(f"PageRank score '{user_pagerank}' for user {user_id} is not a valid float. Type: {type(user_pagerank)}")
                        return None
                logging.info(f"Found PageRank score {user_pagerank} for user {user_id}.")
            else:
                if record_pr and record_pr[PAGERANK_PROPERTY] is None:
                     logging.warning(f"User {user_id} found, but {PAGERANK_PROPERTY} is null. Influence score cannot be calculated.")
                elif record_pr: # Property key might be missing if not set on node
                     logging.warning(f"User {user_id} found, but {PAGERANK_PROPERTY} property is missing or null. Record data: {record_pr.data()}. Influence score cannot be calculated.")
                else: # User not found
                     logging.warning(f"User {user_id} not found by PageRank query. Influence score cannot be calculated.")
                return None # Need PageRank score

            # 2. Get user's total useful votes
            result_votes = session.run(GET_USER_TOTAL_USEFUL_VOTES_QUERY, userId=user_id)
            record_votes = result_votes.single()
            if record_votes and record_votes["totalUsefulVotes"] is not None:
                 user_total_useful = record_votes["totalUsefulVotes"]
            # If no useful votes, user_total_useful remains 0, which is handled by percentileofscore

        logging.info(f"User {user_id}: Raw PageRank={user_pagerank:.4f}, Raw Total Useful Votes={user_total_useful}")

        # 3. Calculate user's individual PageRank percentile and Useful Vote percentile (0-1 range)
        if len(pagerank_distribution) > 0:
            pagerank_percentile_for_user = stats.percentileofscore(pagerank_distribution, user_pagerank, kind='rank') / 100.0
        else:
            logging.warning(f"PageRank distribution is empty. Assuming 0 percentile for PageRank for user {user_id}.")
            pagerank_percentile_for_user = 0.0

        if len(useful_vote_distribution) > 0:
            useful_percentile_for_user = stats.percentileofscore(useful_vote_distribution, user_total_useful, kind='rank') / 100.0
        else:
            logging.warning(f"Useful vote distribution is empty. Assuming 0 percentile for useful votes for user {user_id}.")
            useful_percentile_for_user = 0.0
        
        # 4. Calculate user's composite metric
        user_composite_metric = pagerank_percentile_for_user * useful_percentile_for_user
        logging.info(f"User {user_id}: PageRank Percentile={pagerank_percentile_for_user:.4f}, Useful Vote Percentile={useful_percentile_for_user:.4f}, Composite Metric={user_composite_metric:.4f}")

        # 5. Calculate overall influence percentile using the composite metric distribution
        if len(composite_metric_distribution) > 0:
            overall_influence_percentile = stats.percentileofscore(composite_metric_distribution, user_composite_metric, kind='rank')
        else:
            logging.warning("Composite metric distribution is empty. Cannot calculate overall influence percentile.")
            return None
            
        logging.info(f"Calculated Overall Influence Percentile for user {user_id}: {overall_influence_percentile:.2f}%")
        return overall_influence_percentile

    except Neo4jError as e:
        logging.error(f"Neo4j error calculating overall influence percentile for user '{user_id}': {e.message} (Code: {e.code})")
        return None
    except Exception as e:
        logging.error(f"Unexpected error calculating influence for user '{user_id}': {e}", exc_info=True)
        return None

if __name__ == "__main__":
    # Example Usage (replace with a valid user ID from your graph)
    test_user_id = "u-q20A17Oy-SlmZTdTAK_Mxw"

    if pagerank_distribution is None or \
       useful_vote_distribution is None or \
       composite_metric_distribution is None:
        print("Cannot run test: One or more precomputed distribution files failed to load.")
    else:
        driver = None
        try:
            driver = GraphDatabase.driver(NEO4J_URI, auth=basic_auth(NEO4J_USER, NEO4J_PASSWORD))
            driver.verify_connectivity()
            logging.info("Successfully connected to Neo4j for testing.")
            
            percentile = get_overall_influence_percentile(driver, test_user_id)
            
            if percentile is not None:
                print(f"\nOverall Influence Percentile for User {test_user_id}: {percentile:.2f}%")
            else:
                print(f"\nCould not calculate overall influence percentile for user {test_user_id}.")

        except ServiceUnavailable as e:
            logging.error(f"Could not connect to Neo4j at {NEO4J_URI}: {e}")
        except Exception as e:
            logging.error(f"Script failed during testing: {e}")
        finally:
            if driver:
                driver.close()
                logging.info("Neo4j connection closed.")
