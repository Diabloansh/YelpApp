import logging
from fastapi import APIRouter, Depends, HTTPException, Path
from neo4j import Driver
import pandas as pd
from typing import Optional

# Import Pydantic models
from backend.models import (
    ReviewRhythmData, CuisineDiversityData, SentimentTimelineData,
    WordSignatureData, WordSignatureTerm, HiddenGemsData, HiddenGemBusiness,
    TasteClusterData, TasteClusterCategory, RecommendationsData, RecommendedBusiness,
    InfluencePercentileData, UserProfileData
)

# Import Neo4j driver dependency
from backend.database import get_neo4j_driver

# Import component functions
# Assuming 'components' is in PYTHONPATH or accessible relative to where FastAPI is run
from components import c1_review_rhythm
from components import c2_cuisine_diversity
from components import c3_sentiment_timeline
from components import c4_word_signature
from components import c5_hidden_gem
from components import c6_taste_cluster
from components import c7_recommender
from components import c8_influence_map

router = APIRouter()
logger = logging.getLogger(__name__)

# --- Individual Component Endpoints ---

@router.get("/users/{user_id}/review-rhythm", response_model=Optional[ReviewRhythmData], tags=["User Profile Components"])
async def get_user_review_rhythm(
    user_id: str = Path(..., description="The ID of the user (e.g., 'u-xxxxx')"),
    driver: Driver = Depends(get_neo4j_driver)
):
    try:
        rhythm_df = c1_review_rhythm.get_review_rhythm(driver, user_id)
        if rhythm_df.empty and not c1_review_rhythm.GET_REVIEW_TIMESTAMPS_QUERY: # Check if truly empty or just no data
             # This check might need refinement based on how get_review_rhythm signals "no data" vs "error"
            logger.info(f"No review rhythm data found for user {user_id} or user does not exist.")
            return None # Or return ReviewRhythmData(data={}) if preferred for consistency
        return ReviewRhythmData.from_dataframe(rhythm_df)
    except Exception as e:
        logger.error(f"Error getting review rhythm for user {user_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to retrieve review rhythm: {str(e)}")

@router.get("/users/{user_id}/cuisine-diversity", response_model=Optional[CuisineDiversityData], tags=["User Profile Components"])
async def get_user_cuisine_diversity(
    user_id: str = Path(..., description="The ID of the user"),
    driver: Driver = Depends(get_neo4j_driver)
):
    try:
        counts, score = c2_cuisine_diversity.get_cuisine_diversity(driver, user_id)
        if not counts and score == 0.0: # Typical return for no data or error in component
            logger.info(f"No cuisine diversity data for user {user_id}.")
            return None
        return CuisineDiversityData(category_counts=dict(counts), diversity_score=score)
    except Exception as e:
        logger.error(f"Error getting cuisine diversity for user {user_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to retrieve cuisine diversity: {str(e)}")

@router.get("/users/{user_id}/sentiment-timeline", response_model=Optional[SentimentTimelineData], tags=["User Profile Components"])
async def get_user_sentiment_timeline(
    user_id: str = Path(..., description="The ID of the user"),
    driver: Driver = Depends(get_neo4j_driver)
):
    try:
        timeline = c3_sentiment_timeline.get_sentiment_timeline(driver, user_id)
        if not timeline: # Empty dict if no data
            logger.info(f"No sentiment timeline data for user {user_id}.")
            return None
        return SentimentTimelineData(timeline=timeline)
    except Exception as e:
        logger.error(f"Error getting sentiment timeline for user {user_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to retrieve sentiment timeline: {str(e)}")

@router.get("/users/{user_id}/word-signature", response_model=Optional[WordSignatureData], tags=["User Profile Components"])
async def get_user_word_signature(
    user_id: str = Path(..., description="The ID of the user"),
    driver: Driver = Depends(get_neo4j_driver)
):
    try:
        # Ensure prerequisites for c4 are met (spaCy model, vectorizer)
        if c4_word_signature.nlp is None or c4_word_signature.vectorizer is None:
            logger.error(f"Prerequisites for word signature not met for user {user_id}.")
            raise HTTPException(status_code=503, detail="Word signature prerequisites not loaded on server.")
        
        signature_tuples = c4_word_signature.get_word_signature(driver, user_id)
        if not signature_tuples:
            logger.info(f"No word signature data for user {user_id}.")
            return None
        return WordSignatureData(signature=[WordSignatureTerm(term=t, score=s) for t, s in signature_tuples])
    except HTTPException: # Re-raise if it's our own 503
        raise
    except Exception as e:
        logger.error(f"Error getting word signature for user {user_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to retrieve word signature: {str(e)}")

@router.get("/users/{user_id}/hidden-gems", response_model=Optional[HiddenGemsData], tags=["User Profile Components"])
async def get_user_hidden_gems(
    user_id: str = Path(..., description="The ID of the user"),
    driver: Driver = Depends(get_neo4j_driver)
):
    try:
        gems_list = c5_hidden_gem.find_hidden_gems(driver, user_id)
        if not gems_list:
            logger.info(f"No hidden gems data for user {user_id}.")
            return None
        return HiddenGemsData(gems=[HiddenGemBusiness(**gem) for gem in gems_list])
    except Exception as e:
        logger.error(f"Error getting hidden gems for user {user_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to retrieve hidden gems: {str(e)}")

@router.get("/users/{user_id}/taste-cluster", response_model=Optional[TasteClusterData], tags=["User Profile Components"])
async def get_user_taste_cluster(
    user_id: str = Path(..., description="The ID of the user"),
    driver: Driver = Depends(get_neo4j_driver)
):
    try:
        cluster_info = c6_taste_cluster.get_taste_cluster(driver, user_id)
        if cluster_info is None:
            logger.info(f"No taste cluster data for user {user_id} (user or cluster_id not found).")
            return None
        
        cluster_id_val, categories_tuples = cluster_info
        # Handle case where cluster_id is found but categories might be empty
        return TasteClusterData(
            cluster_id=cluster_id_val,
            top_categories=[TasteClusterCategory(category=cat, count=ct) for cat, ct in categories_tuples]
        )
    except Exception as e:
        logger.error(f"Error getting taste cluster for user {user_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to retrieve taste cluster: {str(e)}")

@router.get("/users/{user_id}/recommendations", response_model=Optional[RecommendationsData], tags=["User Profile Components"])
async def get_user_recommendations(
    user_id: str = Path(..., description="The ID of the user"),
    driver: Driver = Depends(get_neo4j_driver)
):
    try:
        if c7_recommender.category_popularity is None:
            logger.error(f"Category popularity data not loaded for recommendations for user {user_id}.")
            raise HTTPException(status_code=503, detail="Recommendation prerequisites not loaded on server.")

        recommendations_list = c7_recommender.recommend_businesses(driver, user_id)
        if not recommendations_list:
            logger.info(f"No recommendations data for user {user_id}.")
            return None
        return RecommendationsData(recommendations=[RecommendedBusiness(**rec) for rec in recommendations_list])
    except HTTPException: # Re-raise if it's our own 503
        raise
    except Exception as e:
        logger.error(f"Error getting recommendations for user {user_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to retrieve recommendations: {str(e)}")

@router.get("/users/{user_id}/influence-percentile", response_model=Optional[InfluencePercentileData], tags=["User Profile Components"])
async def get_user_influence_percentile(
    user_id: str = Path(..., description="The ID of the user"),
    driver: Driver = Depends(get_neo4j_driver)
):
    try:
        if c8_influence_map.pagerank_distribution is None or \
           c8_influence_map.useful_vote_distribution is None or \
           c8_influence_map.composite_metric_distribution is None:
            logger.error(f"Prerequisite distributions not loaded for influence percentile for user {user_id}.")
            raise HTTPException(status_code=503, detail="Influence percentile prerequisites not loaded on server.")

        percentile = c8_influence_map.get_overall_influence_percentile(driver, user_id)
        if percentile is None: # Component returns None if user not found or other issues
            logger.info(f"Could not calculate influence percentile for user {user_id}.")
            return None
        return InfluencePercentileData(overall_influence_percentile=percentile)
    except HTTPException: # Re-raise if it's our own 503
        raise
    except Exception as e:
        logger.error(f"Error getting influence percentile for user {user_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to retrieve influence percentile: {str(e)}")


# --- Composite Endpoint for Full User Profile ---

@router.get("/users/{user_id}/full-profile", response_model=UserProfileData, tags=["User Profile"])
async def get_full_user_profile(
    user_id: str = Path(..., description="The ID of the user (e.g., 'u-xxxxx')"),
    driver: Driver = Depends(get_neo4j_driver)
):
    profile_data = UserProfileData(user_id=user_id)
    
    # Helper to run component and populate profile_data
    async def run_component(func, model_field_name, *args, model_class=None, data_key=None, is_df=False, is_tuple=False, tuple_keys=None):
        try:
            raw_result = func(*args)
            if raw_result is None or (isinstance(raw_result, tuple) and all(x is None or (isinstance(x, (list, dict, pd.DataFrame)) and not x) for x in raw_result)): # Handle None or empty-like results
                logger.info(f"No data from {func.__name__} for user {user_id}.")
                setattr(profile_data, model_field_name, None)
                return

            if model_class:
                if is_df: # c1
                    df_result = raw_result
                    if df_result.empty and not getattr(args[0], 'GET_REVIEW_TIMESTAMPS_QUERY', False): # args[0] is the module for c1
                         setattr(profile_data, model_field_name, None)
                    else:
                         setattr(profile_data, model_field_name, model_class.from_dataframe(df_result))
                elif is_tuple: # c2, c6
                    if tuple_keys: # c2
                         data_dict = {key: val for key, val in zip(tuple_keys, raw_result)}
                         setattr(profile_data, model_field_name, model_class(**data_dict))
                    else: # c6
                         cluster_id_val, categories_tuples = raw_result
                         setattr(profile_data, model_field_name, model_class(
                             cluster_id=cluster_id_val,
                             top_categories=[TasteClusterCategory(category=cat, count=ct) for cat, ct in categories_tuples]
                         ))
                elif data_key == "signature": # c4
                    setattr(profile_data, model_field_name, model_class(signature=[WordSignatureTerm(term=t, score=s) for t, s in raw_result]))
                elif data_key == "gems": # c5
                    setattr(profile_data, model_field_name, model_class(gems=[HiddenGemBusiness(**gem) for gem in raw_result]))
                elif data_key == "recommendations": # c7
                    setattr(profile_data, model_field_name, model_class(recommendations=[RecommendedBusiness(**rec) for rec in raw_result]))
                else: # c3, c8
                    setattr(profile_data, model_field_name, model_class(**{data_key: raw_result}))
            else: # Should not happen if model_class is always provided
                 setattr(profile_data, model_field_name, raw_result)

        except HTTPException as e: # Propagate HTTP exceptions from prerequisites checks
            logger.warning(f"HTTPException from {func.__name__} for user {user_id}: {e.detail}")
            profile_data.errors[model_field_name] = f"Prerequisite error: {e.detail}"
            setattr(profile_data, model_field_name, None)
        except Exception as e:
            logger.error(f"Error in component {func.__name__} for user {user_id}: {e}", exc_info=True)
            profile_data.errors[model_field_name] = str(e)
            setattr(profile_data, model_field_name, None)

    # Run all components
    # Note: These are run sequentially. For performance, consider asyncio.gather for I/O bound tasks if components were async.
    # However, component scripts are synchronous and some are CPU-bound (spaCy).

    # c1 - Review Rhythm
    await run_component(c1_review_rhythm.get_review_rhythm, "review_rhythm", driver, user_id, model_class=ReviewRhythmData, is_df=True)
    
    # c2 - Cuisine Diversity
    await run_component(c2_cuisine_diversity.get_cuisine_diversity, "cuisine_diversity", driver, user_id, model_class=CuisineDiversityData, is_tuple=True, tuple_keys=["category_counts", "diversity_score"])
    
    # c3 - Sentiment Timeline
    await run_component(c3_sentiment_timeline.get_sentiment_timeline, "sentiment_timeline", driver, user_id, model_class=SentimentTimelineData, data_key="timeline")

    # c4 - Word Signature
    if c4_word_signature.nlp and c4_word_signature.vectorizer:
        await run_component(c4_word_signature.get_word_signature, "word_signature", driver, user_id, model_class=WordSignatureData, data_key="signature")
    else:
        profile_data.errors["word_signature"] = "Prerequisites (spaCy model or TF-IDF vectorizer) not loaded."
        profile_data.word_signature = None
        
    # c5 - Hidden Gems
    await run_component(c5_hidden_gem.find_hidden_gems, "hidden_gems", driver, user_id, model_class=HiddenGemsData, data_key="gems")

    # c6 - Taste Cluster
    await run_component(c6_taste_cluster.get_taste_cluster, "taste_cluster", driver, user_id, model_class=TasteClusterData, is_tuple=True)

    # c7 - Recommender
    if c7_recommender.category_popularity:
        await run_component(c7_recommender.recommend_businesses, "recommendations", driver, user_id, model_class=RecommendationsData, data_key="recommendations")
    else:
        profile_data.errors["recommendations"] = "Category popularity data not loaded."
        profile_data.recommendations = None

    # c8 - Influence Map
    if c8_influence_map.pagerank_distribution is not None and \
       c8_influence_map.useful_vote_distribution is not None and \
       c8_influence_map.composite_metric_distribution is not None:
        await run_component(c8_influence_map.get_overall_influence_percentile, "influence_percentile", driver, user_id, model_class=InfluencePercentileData, data_key="overall_influence_percentile")
    else:
        profile_data.errors["influence_percentile"] = "Prerequisite distributions not loaded."
        profile_data.influence_percentile = None
        
    return profile_data
