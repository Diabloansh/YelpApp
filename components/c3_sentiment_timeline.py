import logging
from collections import defaultdict
from neo4j import GraphDatabase, basic_auth
from neo4j.exceptions import ServiceUnavailable, Neo4jError

# --- Configuration ---
NEO4J_URI = "bolt://localhost:7687"
NEO4J_USER = "neo4j"
NEO4J_PASSWORD = "muttabbocks" # Replace with your password if different

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- Neo4j Query ---
# Fetch year, stars, and polarity for each review by the user
GET_REVIEW_SENTIMENT_DATA_QUERY = """
MATCH (u:User {user_id: $userId})-[:WROTE]->(r:Review)
WHERE r.date IS NOT NULL AND r.stars IS NOT NULL AND r.polarity IS NOT NULL
WITH datetime(replace(r.date, ' ', 'T')).year AS year, r.stars AS stars, r.polarity AS polarity
RETURN year, stars, polarity
ORDER BY year
"""



def calculate_mood_score(stars: float, polarity: float) -> float:
    """Calculates the mood score based on stars and polarity."""
    # Ensure stars is treated as float for division
    # Clamp polarity between -1 and 1 if necessary (TextBlob should already do this)
    clamped_polarity = max(-1.0, min(1.0, polarity))
    mood = 0.7 * (float(stars) / 5.0) + 0.3 * clamped_polarity
    return mood

def get_sentiment_timeline(driver, user_id: str) -> dict[int, float]:
    """
    Calculates the user's sentiment timeline (average mood score per year).

    Args:
        driver: The Neo4j driver instance.
        user_id: The ID of the user (prefixed, e.g., 'u-xxxxx').

    Returns:
        A dictionary where keys are years (int) and values are the
        average mood score (float) for that year.
        Returns an empty dictionary if the user has no relevant reviews
        or an error occurs.
    """
    logging.info(f"Calculating sentiment timeline for user: {user_id}")
    yearly_scores = defaultdict(lambda: {'total_score': 0.0, 'count': 0})
    timeline = {}

    try:
        with driver.session() as session:
            result = session.run(GET_REVIEW_SENTIMENT_DATA_QUERY, userId=user_id)
            records = list(result) # Fetch all results

            if not records:
                logging.info(f"No reviews with sentiment data found for user {user_id}.")
                return timeline # Return empty dict

            # Calculate mood score for each review and aggregate by year
            for record in records:
                year = record["year"]
                stars = record["stars"]
                polarity = record["polarity"]
                
                if year is None or stars is None or polarity is None:
                    logging.warning(f"Skipping review with missing data for user {user_id}: Year={year}, Stars={stars}, Polarity={polarity}")
                    continue

                mood_score = calculate_mood_score(stars, polarity)
                yearly_scores[year]['total_score'] += mood_score
                yearly_scores[year]['count'] += 1

            # Calculate average score per year
            for year, data in yearly_scores.items():
                if data['count'] > 0:
                    timeline[year] = data['total_score'] / data['count']

            # Sort timeline by year for better presentation
            timeline = dict(sorted(timeline.items()))

            logging.info(f"Successfully calculated sentiment timeline for user {user_id}.")
            return timeline

    except Neo4jError as e:
        logging.error(f"Neo4j error calculating timeline for user '{user_id}': {e.message} (Code: {e.code})")
        return {} # Return empty dict on error
    except Exception as e:
        logging.error(f"Unexpected error calculating timeline for user '{user_id}': {e}")
        return {} # Return empty dict on error

if __name__ == "__main__":
    # Example Usage (replace with a valid user ID from your graph)
    test_user_id = "u-_BcWyKQL16ndpBdggh2kNA"

    driver = None
    try:
        driver = GraphDatabase.driver(NEO4J_URI, auth=basic_auth(NEO4J_USER, NEO4J_PASSWORD))
        driver.verify_connectivity()
        logging.info("Successfully connected to Neo4j for testing.")
        
        timeline_data = get_sentiment_timeline(driver, test_user_id)
        
        if timeline_data:
            print(f"\nSentiment Timeline (Avg Mood Score per Year) for User: {test_user_id}")
            for year, avg_score in timeline_data.items():
                print(f"  - {year}: {avg_score:.4f}")
        else:
             print(f"\nCould not retrieve or calculate sentiment timeline data for user {test_user_id}.")

    except ServiceUnavailable as e:
        logging.error(f"Could not connect to Neo4j at {NEO4J_URI}: {e}")
    except Exception as e:
        logging.error(f"Script failed during testing: {e}")
    finally:
        if driver:
            driver.close()
            logging.info("Neo4j connection closed.")
