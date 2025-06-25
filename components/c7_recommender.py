import os
import logging
import json
from pathlib import Path
from collections import Counter
from neo4j import GraphDatabase, basic_auth
from neo4j.exceptions import ServiceUnavailable, Neo4jError
from typing import Optional

# --- Configuration ---
# Determine the absolute path to the project's root directory
PROJECT_ROOT = Path(__file__).resolve().parent.parent

NEO4J_URI = "bolt://localhost:7687"
NEO4J_USER = "neo4j"
NEO4J_PASSWORD = "muttabbocks"  # Replace with your password if different
CATEGORY_POPULARITY_FILE = (
    PROJECT_ROOT / "offline_assets" / "category_top_businesses.jsonl"
)
RECOMMENDATION_K = 5  # Number of recommendations to return
TOP_USER_CATEGORIES_N = 5  # Number of user's top categories to consider
# NEW: Minimum review count for a business to be recommended
MIN_REVIEW_COUNT_FOR_RECOMMENDATION = 10


logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

# --- Load Precomputed Data ---
try:
    category_popularity = {}
    logging.info(f"Loading category popularity data from {CATEGORY_POPULARITY_FILE}...")
    with open(CATEGORY_POPULARITY_FILE, "r", encoding="utf-8") as f:
        for line in f:
            data = json.loads(line)
            category_popularity[data["category"]] = data["top_businesses"]
    logging.info(f"Loaded popularity data for {len(category_popularity)} categories.")
except FileNotFoundError:
    logging.error(
        f"Category popularity file not found: {CATEGORY_POPULARITY_FILE}. Run generate_category_popularity.py first."
    )
    category_popularity = None  # Indicate failure
except json.JSONDecodeError as e:
    logging.error(f"Error decoding JSON in {CATEGORY_POPULARITY_FILE}: {e}")
    category_popularity = None
except Exception as e:
    logging.error(f"Error loading popularity data: {e}")
    category_popularity = None

GENERIC_CATEGORIES = {
    "Restaurants",
    "Food",
    "Nightlife",
    "Bars",
    "Diner",
    "Cafe",
    "Bakery",
    "Event Planning & Services",
    "Grocery",
}

# --- Neo4j Queries ---
GET_USER_REVIEWED_BUSINESSES_QUERY = """
MATCH (u:User {user_id: $userId})-[:WROTE]->(:Review)-[:REVIEWS]->(b:Business)
RETURN DISTINCT b.business_id AS businessId
"""

GET_BUSINESS_CATEGORIES_QUERY = """
MATCH (b:Business {business_id: $businessId})-[:IN_CATEGORY]->(c:Category)
RETURN c.category_id AS categoryId
"""

# MODIFIED QUERY: Added b.review_count
GET_BUSINESS_DETAILS_QUERY = """
MATCH (b:Business {business_id: $businessId})
OPTIONAL MATCH (b)-[:IN_CATEGORY]->(c:Category)
RETURN b.name AS name,
       b.business_id AS business_id,
       b.avgStar AS avgStar,
       b.review_count AS review_count,  // Assuming 'review_count' is the property name
       collect(DISTINCT c.category_id) AS categories
"""


# --- Helper Functions ---


def get_user_reviewed_businesses(driver, user_id):
    """Fetches the set of business IDs reviewed by the user."""
    reviewed_ids = set()
    try:
        with driver.session() as session:
            result = session.run(GET_USER_REVIEWED_BUSINESSES_QUERY, userId=user_id)
            reviewed_ids = {record["businessId"] for record in result}
    except Neo4jError as e:
        logging.error(
            f"Neo4j error fetching reviewed businesses for user '{user_id}': {e.message}"
        )
    except Exception as e:
        logging.error(
            f"Unexpected error fetching reviewed businesses for user '{user_id}': {e}"
        )
    return reviewed_ids


def get_business_categories(driver, business_id):
    """Fetches the categories for a given business (reused from evaluation script)."""
    categories = []
    try:
        with driver.session() as session:
            result = session.run(GET_BUSINESS_CATEGORIES_QUERY, businessId=business_id)
            categories = [record["categoryId"] for record in result]
    except Neo4jError as e:
        logging.error(
            f"Neo4j error fetching categories for business '{business_id}': {e.message}"
        )
    except Exception as e:
        logging.error(
            f"Unexpected error fetching categories for business '{business_id}': {e}"
        )
    return categories


def get_business_details(driver, business_id: str) -> Optional[dict]:
    """Fetches the name, categories, avgStar, and review_count for a given business.""" # Docstring updated
    try:
        with driver.session() as session:
            result = session.run(GET_BUSINESS_DETAILS_QUERY, businessId=business_id)
            record = result.single()
            if record:
                details = record.data()
                # Ensure review_count is an int, default to 0 if missing or not a number
                # This handles cases where review_count might be None or not an integer from DB
                try:
                    details['review_count'] = int(details.get('review_count', 0))
                except (ValueError, TypeError):
                    details['review_count'] = 0

                if "categories" in details and isinstance(details["categories"], list):
                    details["categories"] = [
                        cat
                        for cat in details["categories"]
                        if cat not in GENERIC_CATEGORIES
                    ]
                return details
    except Neo4jError as e:
        logging.error(
            f"Neo4j error fetching details for business '{business_id}': {e.message}"
        )
    except Exception as e:
        logging.error(
            f"Unexpected error fetching details for business '{business_id}': {e}"
        )
    return None


# --- Main Recommender Function ---


def recommend_businesses(driver, user_id: str) -> list[dict]:
    """
    Recommends businesses for a user based on their reviewed categories,
    pre-computed category popularity lists, and minimum review count.

    Args:
        driver: The Neo4j driver instance.
        user_id: The ID of the user (prefixed, e.g., 'u-xxxxx').

    Returns:
        A list of recommended business IDs (up to K), or an empty list
        if prerequisites are missing, user has no reviews, or an error occurs.
    """
    logging.info(f"Generating recommendations for user: {user_id}")

    if category_popularity is None:
        logging.error(
            "Category popularity data not loaded. Cannot generate recommendations."
        )
        return []

    reviewed_business_ids = get_user_reviewed_businesses(driver, user_id)
    if not reviewed_business_ids:
        logging.warning(
            f"Could not fetch reviewed businesses for user {user_id}, or user has no reviews. Proceeding based on potential global popularity if applicable."
        )
        # Proceeding, as user might have no reviews, but we might still recommend based on general prefs later
        # However, current logic relies on user's preferred categories from reviews.

    category_counter = Counter()
    logging.info(
        f"Finding preferred categories for user {user_id} based on {len(reviewed_business_ids)} reviewed businesses."
    )
    for business_id in reviewed_business_ids:
        categories = get_business_categories(driver, business_id)
        category_counter.update(categories)

    for generic in GENERIC_CATEGORIES:
        category_counter.pop(generic, None)

    if not category_counter:
        logging.warning(f"Could not determine preferred categories for user {user_id} after filtering generic ones. Cannot generate category-based recommendations.")
        # Potentially fall back to globally popular items across all categories here,
        # or return empty if strict category preference is required.
        # For now, returning empty if no specific categories found.
        return []

    top_user_categories = [
        cat for cat, _ in category_counter.most_common(TOP_USER_CATEGORIES_N)
    ]
    logging.info(
        f"User {user_id}'s top {len(top_user_categories)} non-generic categories: {top_user_categories}"
    )

    candidate_business_details = []
    seen_candidate_ids = set()

    for category in top_user_categories:
        if category in category_popularity:
            for business_id_from_pop_list in category_popularity[category]:
                if (
                    business_id_from_pop_list not in reviewed_business_ids
                    and business_id_from_pop_list not in seen_candidate_ids
                ):
                    details = get_business_details(driver, business_id_from_pop_list)
                    if details:
                        # MODIFIED: Filter by review count
                        if details.get("review_count", 0) >= MIN_REVIEW_COUNT_FOR_RECOMMENDATION:
                            candidate_business_details.append(details)
                            seen_candidate_ids.add(business_id_from_pop_list)
                        else:
                            logging.info(
                                f"Skipping business '{details.get('name', business_id_from_pop_list)}' (ID: {business_id_from_pop_list}) "
                                f"due to low review count: {details.get('review_count', 0)} "
                                f"(threshold: {MIN_REVIEW_COUNT_FOR_RECOMMENDATION})."
                            )
                    else:
                        logging.warning(
                            f"Could not fetch details for candidate business ID: {business_id_from_pop_list}"
                        )

                    if len(candidate_business_details) >= RECOMMENDATION_K:
                        break
        if len(candidate_business_details) >= RECOMMENDATION_K:
            break

    final_recs = candidate_business_details[:RECOMMENDATION_K]
    logging.info(
        f"Generated {len(final_recs)} recommendations for user {user_id}: {[rec['business_id'] for rec in final_recs]}"
    )

    return final_recs


if __name__ == "__main__":
    test_user_id = (
        "u-_BcWyKQL16ndpBdggh2kNA"
    )

    if category_popularity is None:
        print("Cannot run test: Category popularity data failed to load.")
    else:
        driver = None
        try:
            driver = GraphDatabase.driver(
                NEO4J_URI, auth=basic_auth(NEO4J_USER, NEO4J_PASSWORD)
            )
            driver.verify_connectivity()
            logging.info("Successfully connected to Neo4j for testing.")

            recommendations = recommend_businesses(driver, test_user_id)

            if recommendations:
                print(
                    f"\nTop {len(recommendations)} Recommendations for User: {test_user_id}"
                )
                for i, rec in enumerate(recommendations):
                    categories_str = ", ".join(rec.get("categories", []))
                    avg_star_str = f"{rec.get('avgStar', 'N/A'):.1f} ⭐" if rec.get('avgStar') is not None else "N/A"
                    # MODIFIED: Added review_count to print
                    review_count_str = f"{rec.get('review_count', 'N/A')} reviews"
                    print(f"  {i+1}. Name: {rec.get('name', 'N/A')} - Rating: {avg_star_str} ({review_count_str})")
                    print(f"     ID: {rec.get('business_id', 'N/A')}")
                    print(
                        f"     Categories: {categories_str if categories_str else 'N/A'}"
                    )
            else:
                print(f"\nCould not generate recommendations for user {test_user_id}.")

        except ServiceUnavailable as e:
            logging.error(f"Could not connect to Neo4j at {NEO4J_URI}: {e}")
        except Exception as e:
            logging.error(f"Script failed during testing: {e}")
        finally:
            if driver:
                driver.close()
                logging.info("Neo4j connection closed.")