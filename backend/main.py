import logging
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

# Import the lifespan context manager for Neo4j connection management
from backend.database import lifespan

# Import the API router for user profile endpoints
from backend.routers import user_profile

# Configure basic logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Initialize the FastAPI application with the lifespan context manager
app = FastAPI(
    title="User Profile Analysis API",
    description="Provides access to various user analytics components.",
    version="0.1.0",
    lifespan=lifespan  # Manages Neo4j connection startup and shutdown
)

# CORS (Cross-Origin Resource Sharing) Middleware
# Allows requests from a frontend running on a different port/domain (e.g., localhost:3000 for React dev server)
# Adjust origins as needed for your frontend development setup.
# Using ["*"] is permissive; for production, specify exact origins.
origins = [
    "http://localhost",         # Common for local development
    "http://localhost:3000",    # Default for Create React App
    "http://localhost:3001",    # Common alternative for React
    "http://localhost:5173",    # Default for Vite (React/Vue)
    # Add other origins if your frontend runs elsewhere
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,  # List of origins that are allowed to make requests
    allow_credentials=True, # Allow cookies to be included in requests
    allow_methods=["*"],    # Allow all methods (GET, POST, etc.)
    allow_headers=["*"],    # Allow all headers
)

# Include the user profile router
# All routes defined in user_profile.router will be prefixed with /api
app.include_router(user_profile.router, prefix="/api", tags=["User Profile"])

@app.get("/", tags=["Root"])
async def read_root():
    """
    Root endpoint for the API.
    Provides a simple welcome message and a link to the API documentation.
    """
    return {
        "message": "Welcome to the User Profile Analysis API!",
        "documentation": "/docs" # FastAPI's interactive API documentation (Swagger UI)
    }

# To run this application (from the project root directory, assuming 'backend' is a subdir):
# Ensure you have FastAPI and Uvicorn installed:
# pip install fastapi uvicorn[standard] neo4j pandas spacy scikit-learn numpy scipy
#
# Then run:
# uvicorn backend.main:app --reload
#
# The API will be available at http://127.0.0.1:8000
# Interactive API docs (Swagger UI) at http://127.0.0.1:8000/docs
# ReDoc documentation at http://127.0.0.1:8000/redoc

logger.info("FastAPI application initialized. Router included. CORS configured.")
logger.info(f"API documentation will be available at /docs and /redoc once server is running.")
