import os
import logging
import json
from neo4j import GraphDatabase, basic_auth
from neo4j.exceptions import ServiceUnavailable, Neo4jError

# --- Configuration ---
NEO4J_URI = "bolt://localhost:7687"
NEO4J_USER = "neo4j"
NEO4J_PASSWORD = "muttabbocks" # Replace with your password if different
OUTPUT_FILE = "../offline_assets/category_top_businesses.jsonl" # Store in offline_assets
TOP_N_BUSINESSES = 10 # Number of top businesses to retrieve per category

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- Neo4j Queries ---
GET_CATEGORIES_QUERY = """
MATCH (c:Category)
RETURN c.category_id AS categoryId
ORDER BY categoryId
"""

GET_TOP_BUSINESSES_QUERY = f"""
MATCH (cat:Category {{category_id: $categoryId}})<-[:IN_CATEGORY]-(b:Business)
WHERE b.avgStar IS NOT NULL AND b.review_count IS NOT NULL
RETURN b.business_id AS businessId, b.avgStar AS avgStar, b.review_count AS reviewCount
ORDER BY avgStar DESC, reviewCount DESC
LIMIT $limit
"""

def get_all_categories(driver):
    """Fetches all unique category IDs from Neo4j."""
    categories = []
    logging.info("Fetching all category IDs...")
    try:
        with driver.session() as session:
            result = session.run(GET_CATEGORIES_QUERY)
            categories = [record["categoryId"] for record in result]
    except Neo4jError as e:
        logging.error(f"A Neo4j error occurred fetching categories: {e.message} (Code: {e.code})")
        raise
    except Exception as e:
        logging.error(f"An unexpected error occurred fetching categories: {e}")
        raise
    logging.info(f"Found {len(categories)} unique categories.")
    return categories

def get_top_businesses_for_category(driver, category_id, limit):
    """Fetches the top N business IDs for a given category."""
    business_ids = []
    try:
        with driver.session() as session:
            result = session.run(GET_TOP_BUSINESSES_QUERY, categoryId=category_id, limit=limit)
            business_ids = [record["businessId"] for record in result]
    except Neo4jError as e:
        # Log error but allow script to continue with other categories if possible
        logging.error(f"Neo4j error fetching top businesses for category '{category_id}': {e.message} (Code: {e.code})")
    except Exception as e:
        logging.error(f"Unexpected error fetching top businesses for category '{category_id}': {e}")
    return business_ids

def generate_popularity_lists(driver, categories, output_path, limit):
    """Generates the category popularity list and writes it to a JSONL file."""
    logging.info(f"Generating popularity lists for {len(categories)} categories...")
    
    # Ensure the output directory exists
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    
    count = 0
    try:
        with open(output_path, 'w', encoding='utf-8') as f_out:
            for category_id in categories:
                top_businesses = get_top_businesses_for_category(driver, category_id, limit)
                if top_businesses: # Only write if we found businesses
                    data = {"category": category_id, "top_businesses": top_businesses}
                    json.dump(data, f_out)
                    f_out.write('\n')
                    count += 1
                    if count % 100 == 0:
                         logging.info(f"Processed {count}/{len(categories)} categories...")
                else:
                    logging.warning(f"No businesses found for category: {category_id}")

        logging.info(f"Finished generating popularity lists. Wrote data for {count} categories to {output_path}")
        
    except IOError as e:
        logging.error(f"Failed to write popularity list file to {output_path}: {e}")
        raise
    except Exception as e:
        logging.error(f"An unexpected error occurred during list generation: {e}")
        raise

if __name__ == "__main__":
    driver = None
    try:
        driver = GraphDatabase.driver(NEO4J_URI, auth=basic_auth(NEO4J_USER, NEO4J_PASSWORD))
        driver.verify_connectivity()
        logging.info("Successfully connected to Neo4j.")
        
        # Get all categories
        all_categories = get_all_categories(driver)
        
        # Generate and save the popularity lists
        if all_categories:
            generate_popularity_lists(driver, all_categories, OUTPUT_FILE, TOP_N_BUSINESSES)
        else:
            logging.warning("No categories found in the database. Skipping popularity list generation.")
            
        logging.info("Category popularity list generation script finished successfully.")
        
    except ServiceUnavailable as e:
        logging.error(f"Could not connect to Neo4j at {NEO4J_URI}: {e}")
    except Exception as e:
        logging.error(f"Script failed: {e}")
    finally:
        if driver:
            driver.close()
            logging.info("Neo4j connection closed.")
