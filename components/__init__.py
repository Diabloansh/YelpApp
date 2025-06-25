# This file makes the 'components' directory a Python package.
# This allows modules within this directory to be imported elsewhere,
# for example, by the FastAPI backend.

# You can optionally make specific functions or classes available
# directly when importing the 'components' package, e.g.:
# from .c1_review_rhythm import get_review_rhythm
# from .c2_cuisine_diversity import get_cuisine_diversity
# ... and so on for all components.
# For now, we'll keep it simple and allow direct module imports like:
# from components import c1_review_rhythm
