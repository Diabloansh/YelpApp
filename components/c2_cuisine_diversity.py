import logging
import math
from collections import Counter
from neo4j import GraphDatabase, basic_auth
from neo4j.exceptions import ServiceUnavailable, Neo4jError

# --- Configuration ---
NEO4J_URI = "bolt://localhost:7687"
NEO4J_USER = "neo4j"
NEO4J_PASSWORD = "muttabbocks" # Replace with your password if different

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- Neo4j Query ---
GET_USER_REVIEWED_CATEGORIES_QUERY = """
MATCH (u:User {user_id: $userId})-[:WROTE]->(:Review)-[:REVIEWS]->(b:Business)-[:IN_CATEGORY]->(c:Category)
RETURN c.category_id AS categoryId, count(c) AS categoryCount
ORDER BY categoryCount DESC
"""

def calculate_shannon_entropy(counts: Counter) -> float:
    """Calculates Shannon entropy for a Counter object."""
    total_count = sum(counts.values())
    if total_count == 0:
        return 0.0
    
    entropy = 0.0
    for count in counts.values():
        probability = count / total_count
        if probability > 0: # Avoid log(0)
            entropy -= probability * math.log2(probability)
            
    return entropy

def get_cuisine_diversity(driver, user_id: str) -> tuple[Counter, float]:
    """
    Calculates the user's cuisine diversity based on reviewed business categories.

    Args:
        driver: The Neo4j driver instance.
        user_id: The ID of the user (prefixed, e.g., 'u-xxxxx').

    Returns:
        A tuple containing:
        - A Counter object with category names as keys and review counts as values.
        - The calculated Shannon entropy (diversity score) as a float.
        Returns (Counter(), 0.0) if the user has no relevant reviews or an error occurs.
    """
    logging.info(f"Calculating cuisine diversity for user: {user_id}")
    category_counts = Counter()
    diversity_score = 0.0

    try:
        with driver.session() as session:
            result = session.run(GET_USER_REVIEWED_CATEGORIES_QUERY, userId=user_id)
            records = list(result) # Fetch all results

            if not records:
                logging.info(f"No reviewed categories found for user {user_id}.")
                return category_counts, diversity_score # Return empty counter, 0 score

            # Populate the Counter
            for record in records:
                category_counts[record["categoryId"]] = record["categoryCount"]
            # Remove generic categories
            GENERIC_CATEGORIES = {"Restaurants", "Food", "Nightlife", "Bars","Diner","Cafe","Bakery","Event Planning & Services","Grocery"}
            for generic in GENERIC_CATEGORIES:
                category_counts.pop(generic, None)

            # Calculate diversity score
            diversity_score = calculate_shannon_entropy(category_counts)
            
            logging.info(f"Successfully calculated cuisine diversity for user {user_id}. Score: {diversity_score:.4f}")
            return category_counts, diversity_score

    except Neo4jError as e:
        logging.error(f"Neo4j error calculating diversity for user '{user_id}': {e.message} (Code: {e.code})")
        return Counter(), 0.0 # Return empty counter, 0 score on error
    except Exception as e:
        logging.error(f"Unexpected error calculating diversity for user '{user_id}': {e}")
        return Counter(), 0.0 # Return empty counter, 0 score on error

if __name__ == "__main__":
    # Example Usage (replace with a valid user ID from your graph)
    test_user_id = "u-_BcWyKQL16ndpBdggh2kNA" # Replace with a real user ID like 'u-...'

    driver = None
    try:
        driver = GraphDatabase.driver(NEO4J_URI, auth=basic_auth(NEO4J_USER, NEO4J_PASSWORD))
        driver.verify_connectivity()
        logging.info("Successfully connected to Neo4j for testing.")
        
        counts, score = get_cuisine_diversity(driver, test_user_id)
        
        if counts:
            print(f"\nCuisine Counts for User: {test_user_id}")
            # Print top 10 categories for brevity
            for category, count in counts.most_common(10):
                print(f"  - {category}: {count}")
            if len(counts) > 10:
                print("  ...")
            print(f"\nDiversity Score (Shannon Entropy): {score:.4f}")
        else:
             print(f"\nCould not retrieve or calculate cuisine diversity data for user {test_user_id}.")

    except ServiceUnavailable as e:
        logging.error(f"Could not connect to Neo4j at {NEO4J_URI}: {e}")
    except Exception as e:
        logging.error(f"Script failed during testing: {e}")
    finally:
        if driver:
            driver.close()
            logging.info("Neo4j connection closed.")
