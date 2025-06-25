import logging
from neo4j import GraphDatabase, basic_auth
from neo4j.exceptions import ServiceUnavailable, Neo4jError
from typing import Optional, List, Tuple, Any

# --- Configuration ---
NEO4J_URI = "bolt://localhost:7687"
NEO4J_USER = "neo4j"
NEO4J_PASSWORD = "muttabbocks" # Replace with your password if different
CLUSTER_PROPERTY = "clusterId" # Property where Louvain result is stored

# Define generic categories to filter out
GENERIC_CATEGORIES = {
    "Restaurants", "Food", "Nightlife", "Bars", "Diners", # Added "Diners" as it's similar to "Diner"
    "Cafes", "Bakeries", "Event Planning & Services", "Grocery" # Added "Cafes", "Bakeries"
} # Using plural based on typical category naming, adjust if your IDs are singular

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- Neo4j Queries ---
# Query to get user's cluster ID
GET_USER_CLUSTER_QUERY = f"""
MATCH (u:User {{user_id: $userId}})
RETURN u.{CLUSTER_PROPERTY} AS clusterId
LIMIT 1
"""

# Query to get cluster summary (top non-generic categories)
GET_CLUSTER_SUMMARY_QUERY = """
MATCH (u:User {{ {cluster_property}: $clusterId }})-[:WROTE]->(:Review)-[:REVIEWS]->(:Business)-[:IN_CATEGORY]->(c:Category)
WHERE NOT c.category_id IN $genericCategories
RETURN c.category_id AS category, count(c) AS count
ORDER BY count DESC
LIMIT 10
""" # Used f-string for cluster_property here for consistency, though CLUSTER_PROPERTY is "clusterId"

def get_taste_cluster(driver, user_id: str) -> Optional[Tuple[Any, List[Tuple[str, int]]]]:
    """
    Retrieves the user's pre-computed taste cluster ID and
    the top 5 non-generic categories for that cluster.

    Args:
        driver: The Neo4j driver instance.
        user_id: The ID of the user (prefixed, e.g., 'u-xxxxx').

    Returns:
        A tuple (cluster_id, top_categories_list) if successful.
        'cluster_id' is the integer ID of the cluster.
        'top_categories_list' is a list of (category_name, count) tuples.
        Returns None if the user or cluster ID cannot be determined, or if clusterId is not an int.
        Returns (cluster_id, []) if cluster ID is found but summary fails or yields no results.
    """
    logging.info(f"Retrieving taste cluster and summary for user: {user_id}")
    cluster_id_val: Optional[int] = None # Explicitly aiming for an int

    # --- Part 1: Get User's Cluster ID ---
    try:
        with driver.session() as session:
            result = session.run(GET_USER_CLUSTER_QUERY, userId=user_id)
            record = result.single()

            if record:
                fetched_cluster_id = record["clusterId"]
                if fetched_cluster_id is not None:
                    if isinstance(fetched_cluster_id, int):
                        cluster_id_val = fetched_cluster_id
                        logging.info(f"Found cluster ID {cluster_id_val} for user {user_id}.")
                    else:
                        logging.warning(f"Cluster ID {fetched_cluster_id} for user {user_id} is not an integer (Type: {type(fetched_cluster_id)}). Cannot proceed to get cluster summary.")
                        return None # Critical: Summary query expects an int clusterId
                else:
                    logging.warning(f"User {user_id} found, but {CLUSTER_PROPERTY} is null. Run Louvain script (S3) first.")
                    return None
            else:
                logging.warning(f"User {user_id} not found. Run Louvain script (S3) first.")
                return None

    except Neo4jError as e:
        logging.error(f"Neo4j error retrieving cluster for user '{user_id}': {e.message} (Code: {e.code})")
        return None
    except Exception as e:
        logging.error(f"Unexpected error retrieving cluster for user '{user_id}': {e}")
        return None

    # If cluster_id_val is still None (e.g., due to type mismatch handled above, or earlier returns), exit.
    if cluster_id_val is None:
        # This path should ideally not be reached if previous checks are comprehensive, but as a safeguard:
        logging.error(f"Cluster ID for user {user_id} was not determined or was not an integer.")
        return None

    # --- Part 2: Get Cluster Summary ---
    top_categories: List[Tuple[str, int]] = []
    # Convert set to list for the Cypher query parameter
    generic_categories_list = list(GENERIC_CATEGORIES)

    try:
        with driver.session() as session:
            logging.info(f"Retrieving summary for cluster ID: {cluster_id_val}, excluding {len(generic_categories_list)} generic categories.")
            # Pass CLUSTER_PROPERTY name to the query string formatting
            summary_query_formatted = GET_CLUSTER_SUMMARY_QUERY.format(cluster_property=CLUSTER_PROPERTY)
            result = session.run(
                summary_query_formatted,
                clusterId=cluster_id_val,
                genericCategories=generic_categories_list
            )
            for record in result:
                top_categories.append((record["category"], record["count"]))
            
            if top_categories:
                logging.info(f"Found top non-generic categories for cluster {cluster_id_val}: {top_categories}")
            else:
                logging.info(f"No non-generic categories found for cluster {cluster_id_val} (or all were filtered out).")
            
            return cluster_id_val, top_categories

    except Neo4jError as e:
        logging.error(f"Neo4j error retrieving summary for cluster {cluster_id_val}: {e.message} (Code: {e.code})")
        return cluster_id_val, [] # Return cluster ID with empty categories on summary error
    except Exception as e:
        logging.error(f"Unexpected error retrieving summary for cluster {cluster_id_val}: {e}")
        return cluster_id_val, [] # Return cluster ID with empty categories on summary error

if __name__ == "__main__":
    # Example Usage (replace with a valid user ID from your graph)
    test_user_id = "u-_BcWyKQL16ndpBdggh2kNA" # Example, ensure this user exists and has a clusterId

    driver = None
    try:
        driver = GraphDatabase.driver(NEO4J_URI, auth=basic_auth(NEO4J_USER, NEO4J_PASSWORD))
        driver.verify_connectivity()
        logging.info("Successfully connected to Neo4j for testing.")
        
        cluster_info = get_taste_cluster(driver, test_user_id)
        
        if cluster_info:
            cluster_id, categories = cluster_info
            print(f"\n--- User Taste Profile for {test_user_id} ---")
            print(f"Taste Cluster ID: {cluster_id}")
            if categories:
                print("Top Non-Generic Categories in this Cluster:")
                for category, count in categories:
                    print(f"  - {category} (count: {count})")
            else:
                print(f"No specific non-generic categories found for cluster {cluster_id} (or all were generic/summary retrieval issue).")
        else:
             print(f"\nCould not retrieve taste cluster information for user {test_user_id}.")

    except ServiceUnavailable as e:
        logging.error(f"Could not connect to Neo4j at {NEO4J_URI}: {e}")
    except Exception as e:
        logging.error(f"Script failed during testing: {e}")
    finally:
        if driver:
            driver.close()
            logging.info("Neo4j connection closed.")