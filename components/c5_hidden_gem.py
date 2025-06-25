import logging
from neo4j import GraphDatabase, basic_auth
from neo4j.exceptions import ServiceUnavailable, Neo4jError

# --- Configuration ---
NEO4J_URI = "bolt://localhost:7687"
NEO4J_USER = "neo4j"
NEO4J_PASSWORD = "muttabbocks" # Replace with your password if different
CURRENT_POPULARITY_THRESHOLD = 100 # Min reviews business must have now
PAST_UNPOPULARITY_THRESHOLD = 20 # Max reviews business could have had when user reviewed

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- Neo4j Query ---
# This query finds businesses the user reviewed that are popular now (>100 reviews)
# and then calculates how many reviews that business had at the time of the user's review.
# It filters for cases where the count at that time was low (<20).
# WARNING: This query can be slow without proper indexing, especially on Review.date.
# An index `CREATE INDEX review_date_index FOR (r:Review) ON (r.date)` is recommended.
FIND_HIDDEN_GEMS_QUERY = f"""
MATCH (u:User {{user_id: $userId}})-[:WROTE]->(r:Review)-[:REVIEWS]->(b:Business)
WHERE b.review_count > $currentThreshold
WITH u, r, b
CALL {{
    WITH b, r
    MATCH (b)<-[:REVIEWS]-(r2:Review)
    WHERE r2.date <= r.date
    RETURN count(r2) AS reviewsAtTime
}}
WITH u, r, b, reviewsAtTime
WHERE reviewsAtTime < $pastThreshold AND reviewsAtTime > 0
WITH 
    b.business_id AS businessId, 
    b.name AS businessName, 
    r.date AS userReviewDate, 
    reviewsAtTime, 
    b.review_count AS currentReviewCount
WITH 
    businessId, 
    businessName, 
    userReviewDate, 
    reviewsAtTime, 
    currentReviewCount,
    ((toFloat(currentReviewCount) - toFloat(reviewsAtTime)) / toFloat(reviewsAtTime)) * 100 AS percentIncrease
ORDER BY percentIncrease DESC
LIMIT 5
RETURN businessId, businessName, userReviewDate, reviewsAtTime, currentReviewCount, percentIncrease
"""



def find_hidden_gems(driver, user_id: str) -> list[dict]:
    """
    Finds hidden gems reviewed by the user.

    Args:
        driver: The Neo4j driver instance.
        user_id: The ID of the user (prefixed, e.g., 'u-xxxxx').

    Returns:
        A list of dictionaries, each representing a hidden gem business
        with details like ID, name, user review date, review count then,
        and review count now. Returns an empty list if none are found
        or an error occurs.
    """
    logging.info(f"Finding hidden gems for user: {user_id}")
    hidden_gems = []

    try:
        with driver.session() as session:
            result = session.run(
                FIND_HIDDEN_GEMS_QUERY, 
                userId=user_id, 
                currentThreshold=CURRENT_POPULARITY_THRESHOLD,
                pastThreshold=PAST_UNPOPULARITY_THRESHOLD
            )
            
            # Collect results into a list of dictionaries
            for record in result:
                 hidden_gems.append({
                     "business_id": record["businessId"],
                     "business_name": record["businessName"],
                     "user_review_date": record["userReviewDate"],
                     "reviews_at_time": record["reviewsAtTime"],
                     "current_review_count": record["currentReviewCount"]
                 })

            logging.info(f"Found {len(hidden_gems)} hidden gems for user {user_id}.")
            return hidden_gems

    except Neo4jError as e:
        logging.error(f"Neo4j error finding hidden gems for user '{user_id}': {e.message} (Code: {e.code})")
        logging.error("Ensure an index exists on Review.date for performance: CREATE INDEX review_date_index FOR (r:Review) ON (r.date)")
        return [] # Return empty list on error
    except Exception as e:
        logging.error(f"Unexpected error finding hidden gems for user '{user_id}': {e}")
        return [] # Return empty list on error

if __name__ == "__main__":
    # Example Usage (replace with a valid user ID from your graph)
    test_user_id = "u-_BcWyKQL16ndpBdggh2kNA"

    driver = None
    try:
        driver = GraphDatabase.driver(NEO4J_URI, auth=basic_auth(NEO4J_USER, NEO4J_PASSWORD))
        driver.verify_connectivity()
        logging.info("Successfully connected to Neo4j for testing.")
        
        gems = find_hidden_gems(driver, test_user_id)
        
        if gems:
            print(f"\nHidden Gems Found for User: {test_user_id}")
            for gem in gems:
                print(f"  - Name: {gem['business_name']} (ID: {gem['business_id']})")
                print(f"    Reviewed on: {gem['user_review_date']}")
                print(f"    Reviews then: {gem['reviews_at_time']}, Reviews now: {gem['current_review_count']}")
        else:
             print(f"\nNo hidden gems found or error occurred for user {test_user_id}.")

    except ServiceUnavailable as e:
        logging.error(f"Could not connect to Neo4j at {NEO4J_URI}: {e}")
    except Exception as e:
        logging.error(f"Script failed during testing: {e}")
    finally:
        if driver:
            driver.close()
            logging.info("Neo4j connection closed.")
