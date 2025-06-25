import logging
from neo4j import GraphDatabase, Driver, basic_auth
from contextlib import asynccontextmanager
from fastapi import HTTPException

# --- Configuration ---
# TODO: Move these to environment variables or a config file for production
NEO4J_URI = "bolt://localhost:7687"
NEO4J_USER = "neo4j"
NEO4J_PASSWORD = "muttabbocks" # Replace with your password if different

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class Neo4jConnection:
    def __init__(self, uri, user, password):
        self._driver = None
        try:
            self._driver = GraphDatabase.driver(uri, auth=basic_auth(user, password))
            self._driver.verify_connectivity()
            logging.info("Successfully connected to Neo4j.")
        except Exception as e:
            logging.error(f"Failed to connect to Neo4j: {e}")
            # Depending on the application's needs, you might want to raise an error here
            # or handle it in a way that allows the app to start but report the DB issue.
            # For now, we'll let it proceed, and individual queries will fail.

    def close(self):
        if self._driver is not None:
            self._driver.close()
            logging.info("Neo4j connection closed.")

    def get_driver(self) -> Driver:
        if self._driver is None:
            # This case should ideally be handled by a more robust retry/startup mechanism
            logging.error("Neo4j driver not initialized. Attempting to reconnect...")
            try:
                self._driver = GraphDatabase.driver(NEO4J_URI, auth=basic_auth(NEO4J_USER, NEO4J_PASSWORD))
                self._driver.verify_connectivity()
                logging.info("Reconnected to Neo4j successfully.")
            except Exception as e:
                logging.error(f"Failed to reconnect to Neo4j: {e}")
                raise HTTPException(status_code=503, detail="Database connection error. Please try again later.")
        return self._driver

# Global Neo4j connection instance
neo4j_connection = Neo4jConnection(NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD)

# Dependency to get Neo4j driver for FastAPI endpoints
def get_neo4j_driver() -> Driver:
    """
    FastAPI dependency that provides a Neo4j driver instance.
    """
    driver = neo4j_connection.get_driver()
    if not driver:
        # This will be caught by FastAPI and returned as a 503 error
        raise HTTPException(status_code=503, detail="Could not connect to the database.")
    return driver

# Lifespan context manager for FastAPI app
@asynccontextmanager
async def lifespan(app):
    # Code to run on startup
    # The Neo4jConnection class already tries to connect on instantiation.
    # We can add a re-verify step here if needed, or ensure it's ready.
    logging.info("FastAPI application startup: Ensuring Neo4j connection...")
    _ = neo4j_connection.get_driver() # This will attempt connection if not already made
    yield
    # Code to run on shutdown
    logging.info("FastAPI application shutdown: Closing Neo4j connection...")
    neo4j_connection.close()
