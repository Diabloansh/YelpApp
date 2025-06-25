import os
import logging
import json
import random
from collections import defaultdict, Counter
from neo4j import GraphDatabase, basic_auth
from neo4j.exceptions import ServiceUnavailable, Neo4jError

# --- Configuration ---
NEO4J_URI = "bolt://localhost:7687"
NEO4J_USER = "neo4j"
NEO4J_PASSWORD = "muttabbocks" # Replace with your password if different
CATEGORY_POPULARITY_FILE = "../offline_assets/category_top_businesses.jsonl"
OUTPUT_FILE = "../offline_assets/recommender_evaluation.txt"
NUM_TEST_USERS = 100 # Number of users to evaluate
RECOMMENDATION_K = 10 # Top-K recommendations (for Precision@K)
HOLD_OUT_N = 10 # Number of recent reviews to hold out per user for testing

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- Neo4j Queries ---
GET_RANDOM_USERS_QUERY = """
MATCH (u:User)-[:WROTE]->(r:Review)-[:REVIEWS]->(:Business) // Ensure review links to a business
WITH u, count(r) AS reviewCount    // Group by user and count relevant reviews
WHERE reviewCount > $hold_out_n    // Filter users based on the count of relevant reviews
RETURN u.user_id AS userId
ORDER BY rand()
LIMIT $limit
"""

# CORRECTED QUERY: Use single braces for property matching
GET_USER_REVIEWS_QUERY = """
MATCH (u:User {user_id: $userId})-[:WROTE]->(r:Review)-[:REVIEWS]->(b:Business) // Corrected syntax {user_id: ...}
RETURN b.business_id AS businessId, r.date AS reviewDate
ORDER BY r.date DESC
"""

# (GET_BUSINESS_CATEGORIES_QUERY remains the same)
GET_BUSINESS_CATEGORIES_QUERY = """
MATCH (b:Business {business_id: $businessId})-[:IN_CATEGORY]->(c:Category) // Corrected syntax {business_id: ...}
RETURN c.category_id AS categoryId
"""

# --- Helper Functions ---

def load_category_popularity(filepath):
    """Loads the category -> [top businesses] mapping from the JSONL file."""
    popularity = {}
    logging.info(f"Loading category popularity data from {filepath}...")
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            for line in f:
                data = json.loads(line)
                popularity[data["category"]] = data["top_businesses"]
        logging.info(f"Loaded popularity data for {len(popularity)} categories.")
    except FileNotFoundError:
        logging.error(f"Category popularity file not found: {filepath}")
        raise
    except json.JSONDecodeError as e:
        logging.error(f"Error decoding JSON in {filepath}: {e}")
        raise
    except Exception as e:
        logging.error(f"Error loading popularity data: {e}")
        raise
    return popularity

def get_user_reviews(driver, user_id):
    """Fetches all reviewed business IDs for a user, ordered by review date descending."""
    reviews = []
    try:
        with driver.session() as session:
            result = session.run(GET_USER_REVIEWS_QUERY, userId=user_id)
            reviews = [(record["businessId"], record["reviewDate"]) for record in result]
    except Neo4jError as e:
        logging.error(f"Neo4j error fetching reviews for user '{user_id}': {e.message} (Code: {e.code})")
    except Exception as e:
        logging.error(f"Unexpected error fetching reviews for user '{user_id}': {e}")
    return reviews

def get_business_categories(driver, business_id):
    """Fetches the categories for a given business."""
    categories = []
    try:
        with driver.session() as session:
            result = session.run(GET_BUSINESS_CATEGORIES_QUERY, businessId=business_id)
            categories = [record["categoryId"] for record in result]
    except Neo4jError as e:
        logging.error(f"Neo4j error fetching categories for business '{business_id}': {e.message} (Code: {e.code})")
    except Exception as e:
        logging.error(f"Unexpected error fetching categories for business '{business_id}': {e}")
    return categories

def get_recommendations(driver, user_train_reviews, category_popularity, k):
    """Generates top-K recommendations based on user's training reviews and category popularity."""
    if not user_train_reviews:
        return []

    # Find user's preferred categories based on training reviews
    category_counter = Counter()
    reviewed_business_ids = set(bid for bid, _ in user_train_reviews)

    for business_id, _ in user_train_reviews:
        categories = get_business_categories(driver, business_id)
        category_counter.update(categories)

    # Get top categories (can adjust how many to consider)
    top_user_categories = [cat for cat, _ in category_counter.most_common(5)] # Consider top 5 categories

    # Generate recommendations from popular businesses in those categories
    recommendations = {} # Use dict to store potential score/rank if needed, here just business_id -> source_category
    candidate_businesses = set()

    for category in top_user_categories:
        if category in category_popularity:
            for business_id in category_popularity[category]:
                if business_id not in reviewed_business_ids and business_id not in candidate_businesses:
                     # Simple approach: add to candidates. Could add ranking logic here.
                     candidate_businesses.add(business_id)
                     recommendations[business_id] = category # Track source for potential debugging

    # Return top K candidates (order might be arbitrary here without ranking)
    # Convert set to list and take top K
    final_recs = list(candidate_businesses)[:k] 
    
    return final_recs

def evaluate_recommender(driver, category_popularity, num_users, k, hold_out_n):
    """Evaluates the recommender using Precision@K."""
    logging.info(f"Starting evaluation for {num_users} users, Precision@{k}, holding out {hold_out_n} review(s).")
    
    test_user_ids = []
    try:
        with driver.session() as session:
            result = session.run(GET_RANDOM_USERS_QUERY, limit=num_users,hold_out_n=HOLD_OUT_N)
            test_user_ids = [record["userId"] for record in result]
    except Neo4jError as e:
        logging.error(f"Failed to fetch test users: {e.message}")
        return None, 0
    except Exception as e:
        logging.error(f"Unexpected error fetching test users: {e}")
        return None, 0

    if not test_user_ids:
        logging.error("No suitable test users found.")
        return None, 0
        
    logging.info(f"Selected {len(test_user_ids)} users for evaluation.")

    hits = 0
    evaluated_users = 0

    for user_id in test_user_ids:
        all_reviews = get_user_reviews(driver, user_id)
        
        if len(all_reviews) <= hold_out_n:
            logging.warning(f"Skipping user {user_id}: Not enough reviews ({len(all_reviews)}) to hold out {hold_out_n}.")
            continue

        # Split reviews: hold out the most recent N
        test_reviews = all_reviews[:hold_out_n]
        train_reviews = all_reviews[hold_out_n:]
        
        test_business_ids = set(bid for bid, _ in test_reviews)
        
        # Get recommendations based on training data
        recommendations = get_recommendations(driver, train_reviews, category_popularity, k)
        
        # Check if any held-out business is in the top-K recommendations
        hit = False
        for rec_business_id in recommendations:
            if rec_business_id in test_business_ids:
                hit = True
                break
        
        if hit:
            hits += 1
            
        evaluated_users += 1
        if evaluated_users % 10 == 0:
             logging.info(f"Evaluated {evaluated_users}/{len(test_user_ids)} users...")

    if evaluated_users == 0:
        logging.error("No users were successfully evaluated.")
        return None, 0

    precision_at_k = hits / evaluated_users
    logging.info(f"Evaluation complete: Hits={hits}, Evaluated Users={evaluated_users}")
    logging.info(f"Precision@{k} = {precision_at_k:.4f}")
    
    return precision_at_k, evaluated_users

if __name__ == "__main__":
    driver = None
    precision = None
    num_evaluated = 0
    
    try:
        # Load pre-computed data
        category_popularity_data = load_category_popularity(CATEGORY_POPULARITY_FILE)
        
        # Connect to Neo4j
        driver = GraphDatabase.driver(NEO4J_URI, auth=basic_auth(NEO4J_USER, NEO4J_PASSWORD))
        driver.verify_connectivity()
        logging.info("Successfully connected to Neo4j.")
        
        # Run evaluation
        precision, num_evaluated = evaluate_recommender(driver, category_popularity_data, NUM_TEST_USERS, RECOMMENDATION_K, HOLD_OUT_N)
        
        # Write results
        if precision is not None:
            logging.info(f"Writing evaluation results to {OUTPUT_FILE}...")
            os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
            with open(OUTPUT_FILE, 'w', encoding='utf-8') as f_out:
                f_out.write(f"Recommender Evaluation Results\n")
                f_out.write(f"-----------------------------\n")
                f_out.write(f"Test Users Sampled: {NUM_TEST_USERS}\n")
                f_out.write(f"Users Successfully Evaluated: {num_evaluated}\n")
                f_out.write(f"Reviews Held Out Per User: {HOLD_OUT_N}\n")
                f_out.write(f"Recommendation List Size (K): {RECOMMENDATION_K}\n")
                f_out.write(f"Precision@{RECOMMENDATION_K}: {precision:.4f}\n")
            logging.info("Results written successfully.")
        else:
             logging.error("Evaluation failed, results not written.")

    except FileNotFoundError:
        logging.error(f"Prerequisite file {CATEGORY_POPULARITY_FILE} not found. Run generate_category_popularity.py first.")
    except ServiceUnavailable as e:
        logging.error(f"Could not connect to Neo4j at {NEO4J_URI}: {e}")
    except Exception as e:
        logging.error(f"Script failed: {e}", exc_info=True) # Include traceback for debugging
    finally:
        if driver:
            driver.close()
            logging.info("Neo4j connection closed.")

    logging.info("Recommender evaluation script finished.")
