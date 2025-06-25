import os
import re
import html
import logging
import pickle
from neo4j import GraphDatabase, basic_auth
from neo4j.exceptions import ServiceUnavailable, Neo4jError
from sklearn.feature_extraction.text import TfidfVectorizer

# --- Configuration -----------------------------------------------------------
NEO4J_URI      = "bolt://localhost:7687"
NEO4J_USER     = "neo4j"
NEO4J_PASSWORD = "muttabbocks"

OUTPUT_FILE    = "../offline_assets/idf_vector.pkl"
BATCH_SIZE     = 100000
VECTORIZER_PARAMS = {
    "stop_words":  "english",
    "max_df":      0.7,
    "min_df":      2,
    "max_features":100_000,
    "ngram_range": (1, 2)
}

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s - %(levelname)s - %(message)s")

# --- Pre‑clean regexes -------------------------------------------------------
TAG_RE      = re.compile(r"<[^>]+>")
EMOJI_RE    = re.compile(
    "["
    "\U0001F300-\U0001F6FF"  # symbols & pictographs
    "\U0001F700-\U0001F77F"  # alchemical symbols
    "\U0001F780-\U0001F7FF"  # Geometric Shapes Extended
    "\U0001F800-\U0001F8FF"  # Supplemental Arrows‑C
    "\U0001F900-\U0001F9FF"  # Supplemental Symbols & Pictographs
    "\U0001FA00-\U0001FAFF"  # Symbols & Pictographs Extended‑A
    "\U00002702-\U000027B0"  # Dingbats
    "\U000024C2-\U0001F251"  # Enclosed characters
    "]+",
    flags=re.UNICODE
)

def clean_text(text: str) -> str:
    """Remove HTML tags, unescape entities, drop emojis."""
    if not text:
        return ""
    without_tags   = TAG_RE.sub(" ", text)             # strip <html>
    unescaped      = html.unescape(without_tags)       # &amp; → &
    without_emoji  = EMOJI_RE.sub(" ", unescaped)      # 🚀 → (delete)
    return without_emoji.strip()

# --- Neo4j query -------------------------------------------------------------
GET_REVIEWS_QUERY = """
MATCH (r:Review)
RETURN r.text AS text
SKIP $skip LIMIT $limit
"""

# ---------------------------------------------------------------------------
def fetch_review_texts(driver):
    """Stream all review texts, clean them, accumulate in list."""
    texts, skip, processed = [], 0, 0
    logging.info("Fetching review texts from Neo4j...")
    with driver.session() as session:
        while True:
            logging.info(f"Fetching batch SKIP={skip} LIMIT={BATCH_SIZE}")
            result = session.run(GET_REVIEWS_QUERY, skip=skip, limit=BATCH_SIZE)
            batch = [clean_text(rec["text"]) for rec in result
                     if rec["text"]]
            if not batch:
                break
            texts.extend(batch)
            processed += len(batch)
            skip += BATCH_SIZE
            logging.info(f"Fetched {processed} cleaned reviews so far…")
    logging.info(f"Finished fetching. Total cleaned reviews: {len(texts)}")
    return texts

def calculate_and_save_idf(texts, out_path):
    if not texts:
        logging.warning("No texts to vectorize.")
        return
    logging.info(f"Calculating TF‑IDF on {len(texts)} docs…")
    vec = TfidfVectorizer(**VECTORIZER_PARAMS).fit(texts)
    logging.info(f"Done. Vocabulary size: {len(vec.vocabulary_)}")
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    with open(out_path, "wb") as fh:
        pickle.dump(vec, fh)
    logging.info(f"Vectorizer saved to {out_path}")

# ---------------------------------------------------------------------------
if __name__ == "__main__":
    driver = None
    try:
        driver = GraphDatabase.driver(
            NEO4J_URI, auth=basic_auth(NEO4J_USER, NEO4J_PASSWORD))
        driver.verify_connectivity()
        logging.info("Connected to Neo4j.")
        texts = fetch_review_texts(driver)
        calculate_and_save_idf(texts, OUTPUT_FILE)
        logging.info("IDF vector creation finished successfully.")
    except ServiceUnavailable as e:
        logging.error(f"Neo4j unreachable: {e}")
    except Exception as e:
        logging.error(f"Script failed: {e}")
    finally:
        if driver:
            driver.close()
            logging.info("Neo4j connection closed.")
