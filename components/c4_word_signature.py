import os
import logging
import pickle
import spacy
from pathlib import Path
from collections import Counter
from neo4j import GraphDatabase, basic_auth
from neo4j.exceptions import ServiceUnavailable, Neo4jError
from sklearn.feature_extraction.text import TfidfVectorizer
import spacy

# --- Configuration ---
# Determine the absolute path to the project's root directory
# __file__ is the path to the current script (e.g., .../Project/components/c4_word_signature.py)
# .resolve().parent gives the directory of the script (e.g., .../Project/components/)
# .parent then goes up one level to the project root (e.g., .../Project/)
PROJECT_ROOT = Path(__file__).resolve().parent.parent

NEO4J_URI = "bolt://localhost:7687"
NEO4J_USER = "neo4j"
NEO4J_PASSWORD = "muttabbocks" # Replace with your password if different
IDF_VECTORIZER_PATH = PROJECT_ROOT / "offline_assets" / "idf_vector.pkl"
SPACY_MODEL = "en_core_web_sm" 
MAX_REVIEWS_FOR_SIGNATURE = 5000 # Limit number of reviews processed per user
TOP_N_TERMS = 25 # Number of top terms/bigrams to return
ALLOWED_POS = {"PROPN", "ADJ"} # Part-of-speech tags to keep

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- Load Resources ---
# Load spaCy model (consider loading once if used in a server context)
try:
    nlp = spacy.load(SPACY_MODEL)
    logging.info(f"spaCy model '{SPACY_MODEL}' loaded successfully.")
except OSError:
    logging.error(f"spaCy model '{SPACY_MODEL}' not found. Please download it: python -m spacy download {SPACY_MODEL}")
    # Depending on the application, you might want to exit or handle this differently.
    nlp = None 
except Exception as e:
     logging.error(f"Error loading spaCy model '{SPACY_MODEL}': {e}")
     nlp = None

# Load TF-IDF Vectorizer
try:
    with open(IDF_VECTORIZER_PATH, 'rb') as f:
        vectorizer = pickle.load(f)
    logging.info(f"TF-IDF vectorizer loaded successfully from {IDF_VECTORIZER_PATH}.")
    # Check if it's a TfidfVectorizer instance (basic check)
    if not isinstance(vectorizer, TfidfVectorizer):
         logging.warning(f"Loaded object from {IDF_VECTORIZER_PATH} is not a TfidfVectorizer instance.")
         vectorizer = None
except FileNotFoundError:
    logging.error(f"IDF vectorizer file not found at {IDF_VECTORIZER_PATH}. Run calculate_idf.py first.")
    vectorizer = None
except Exception as e:
    logging.error(f"Error loading IDF vectorizer: {e}")
    vectorizer = None

# --- Neo4j Query ---
GET_USER_REVIEW_TEXTS_QUERY = """
MATCH (u:User {user_id: $userId})-[:WROTE]->(r:Review)
WHERE r.text IS NOT NULL AND r.text <> ''
RETURN r.text AS text
ORDER BY r.date DESC // Get most recent reviews first
LIMIT $limit 
"""

def preprocess_text_spacy(text: str) -> list[str]:
    """Tokenizes, lemmatizes, and filters text by POS tags using spaCy."""
    if not nlp or not text:
        return []
    
    doc = nlp(text.lower()) # Process text with spaCy
    tokens = [
        token.lemma_ # Use lemma for normalization
        for token in doc 
        if token.pos_ in ALLOWED_POS and not token.is_stop and not token.is_punct and len(token.lemma_) > 1
    ]
    return tokens

def get_word_signature(driver, user_id: str) -> list[tuple[str, float]]:
    """
    Calculates the user's word signature (top TF-IDF terms/bigrams).

    Args:
        driver: The Neo4j driver instance.
        user_id: The ID of the user (prefixed, e.g., 'u-xxxxx').

    Returns:
        A list of tuples, where each tuple is (term, tfidf_score),
        sorted by score descending. Returns an empty list if prerequisites
        (spaCy model, vectorizer) are missing, user has no reviews,
        or an error occurs.
    """
    logging.info(f"Calculating word signature for user: {user_id}")
    
    if nlp is None or vectorizer is None:
        logging.error("Prerequisites (spaCy model or TF-IDF vectorizer) not loaded. Cannot calculate signature.")
        return []

    user_reviews = []
    try:
        with driver.session() as session:
            result = session.run(GET_USER_REVIEW_TEXTS_QUERY, userId=user_id, limit=MAX_REVIEWS_FOR_SIGNATURE)
            user_reviews = [record["text"] for record in result]
            
        if not user_reviews:
            logging.info(f"No review texts found for user {user_id}.")
            return []
            
        logging.info(f"Fetched {len(user_reviews)} reviews for user {user_id}.")

        # Preprocess all reviews for the user
        # Combine into a single string for TF-IDF calculation for the user's "document"
        logging.info("Preprocessing review texts with spaCy...")
        processed_tokens = []
        for review_text in user_reviews:
             processed_tokens.extend(preprocess_text_spacy(review_text))
        
        if not processed_tokens:
             logging.info(f"No valid tokens found after preprocessing for user {user_id}.")
             return []

        user_doc = " ".join(processed_tokens) # Rejoin tokens into a single document string

        # Calculate TF-IDF scores for the user's combined document
        logging.info("Calculating TF-IDF scores...")
        tfidf_matrix = vectorizer.transform([user_doc])
        
        # Extract scores and feature names (terms/bigrams)
        feature_names = vectorizer.get_feature_names_out()
        scores = tfidf_matrix.toarray().flatten()
        
        # Create term-score pairs and sort
        term_scores = [(feature_names[i], scores[i]) for i in scores.argsort()[::-1] if scores[i] > 0]
        
        # Get top N terms
        top_terms = term_scores[:TOP_N_TERMS]

        logging.info(f"Successfully calculated word signature for user {user_id}. Found {len(top_terms)} terms.")
        return top_terms

    except Neo4jError as e:
        logging.error(f"Neo4j error calculating signature for user '{user_id}': {e.message} (Code: {e.code})")
        return []
    except Exception as e:
        logging.error(f"Unexpected error calculating signature for user '{user_id}': {e}", exc_info=True)
        return []

if __name__ == "__main__":
    # Example Usage (replace with a valid user ID from your graph)
    test_user_id = "u-_BcWyKQL16ndpBdggh2kNA"

    # Ensure prerequisites are loaded before testing
    if nlp is None or vectorizer is None:
        print("Cannot run test: spaCy model or TF-IDF vectorizer failed to load.")
    else:
        driver = None
        try:
            driver = GraphDatabase.driver(NEO4J_URI, auth=basic_auth(NEO4J_USER, NEO4J_PASSWORD))
            driver.verify_connectivity()
            logging.info("Successfully connected to Neo4j for testing.")
            
            signature = get_word_signature(driver, test_user_id)
            
            if signature:
                print(f"\nWord Signature (Top {TOP_N_TERMS} terms) for User: {test_user_id}")
                for term, score in signature:
                    print(f"  - {term}: {score:.4f}")
            else:
                print(f"\nCould not retrieve or calculate word signature for user {test_user_id}.")

        except ServiceUnavailable as e:
            logging.error(f"Could not connect to Neo4j at {NEO4J_URI}: {e}")
        except Exception as e:
            logging.error(f"Script failed during testing: {e}")
        finally:
            if driver:
                driver.close()
                logging.info("Neo4j connection closed.")
