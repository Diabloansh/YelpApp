import logging
import pandas as pd
from neo4j import GraphDatabase, basic_auth
from neo4j.exceptions import ServiceUnavailable, Neo4jError

# --- Configuration ---
NEO4J_URI = "bolt://localhost:7687"
NEO4J_USER = "neo4j"
NEO4J_PASSWORD = "muttabbocks" # Replace with your password if different

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- Neo4j Query ---
# Assumes Review.date is stored as a DateTime compatible string (e.g., ISO 8601)
# Neo4j's datetime functions extract dayOfWeek (1=Mon, 7=Sun) and hour.
GET_REVIEW_TIMESTAMPS_QUERY = """
MATCH (u:User {user_id: $userId})-[:WROTE]->(r:Review)
WHERE r.date IS NOT NULL
WITH datetime(replace(r.date, ' ', 'T')) AS reviewDateTime
RETURN reviewDateTime.dayOfWeek AS dayOfWeek, reviewDateTime.hour AS hour, count(*) AS reviewCount
"""

def get_review_rhythm(driver, user_id: str) -> pd.DataFrame:
    """
    Calculates the user's review rhythm heatmap data.

    Args:
        driver: The Neo4j driver instance.
        user_id: The ID of the user (prefixed, e.g., 'u-xxxxx').

    Returns:
        A Pandas DataFrame representing the 7x24 heatmap,
        with days (1-7) as index and hours (0-23) as columns.
        Returns an empty DataFrame if the user has no reviews or an error occurs.
    """
    logging.info(f"Calculating review rhythm for user: {user_id}")
    
    # Initialize an empty DataFrame with days 1-7 and hours 0-23
    # Ensure all cells exist, filled with 0 initially.
    days = range(1, 8) # Monday=1 to Sunday=7
    hours = range(0, 24)
    heatmap_df = pd.DataFrame(0, index=days, columns=hours)
    heatmap_df.index.name = 'DayOfWeek'
    heatmap_df.columns.name = 'HourOfDay'

    try:
        with driver.session() as session:
            result = session.run(GET_REVIEW_TIMESTAMPS_QUERY, userId=user_id)
            records = list(result) # Fetch all results

            if not records:
                logging.info(f"No reviews found for user {user_id}.")
                return heatmap_df # Return empty heatmap

            # Populate the DataFrame with counts from the query results
            for record in records:
                day = record["dayOfWeek"]
                hour = record["hour"]
                count = record["reviewCount"]
                if day in heatmap_df.index and hour in heatmap_df.columns:
                    heatmap_df.loc[day, hour] = count
                else:
                    logging.warning(f"Received unexpected day/hour from Neo4j: Day={day}, Hour={hour}. Skipping.")

            logging.info(f"Successfully calculated review rhythm for user {user_id}.")
            return heatmap_df

    except Neo4jError as e:
        logging.error(f"Neo4j error calculating rhythm for user '{user_id}': {e.message} (Code: {e.code})")
        return pd.DataFrame() # Return empty DataFrame on error
    except Exception as e:
        logging.error(f"Unexpected error calculating rhythm for user '{user_id}': {e}")
        return pd.DataFrame() # Return empty DataFrame on error

if __name__ == "__main__":
    # Example Usage (replace with a valid user ID from your graph)
    test_user_id = "u-_BcWyKQL16ndpBdggh2kNA" # Replace with a real user ID like 'u-...'

    driver = None
    try:
        driver = GraphDatabase.driver(NEO4J_URI, auth=basic_auth(NEO4J_USER, NEO4J_PASSWORD))
        driver.verify_connectivity()
        logging.info("Successfully connected to Neo4j for testing.")
        
        rhythm_data = get_review_rhythm(driver, test_user_id)
        
        if not rhythm_data.empty:
            print(f"\nReview Rhythm Heatmap Data for User: {test_user_id}")
            print(rhythm_data)
            # Check if there's any activity
            if rhythm_data.values.sum() == 0:
                 print("\nNote: User has reviews, but counts might be zero if query failed to populate.")
        else:
             print(f"\nCould not retrieve or calculate rhythm data for user {test_user_id}.")

    except ServiceUnavailable as e:
        logging.error(f"Could not connect to Neo4j at {NEO4J_URI}: {e}")
    except Exception as e:
        logging.error(f"Script failed during testing: {e}")
    finally:
        if driver:
            driver.close()
            logging.info("Neo4j connection closed.")
