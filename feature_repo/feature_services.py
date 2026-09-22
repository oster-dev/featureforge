"""Feast feature-service definitions for FeatureForge."""

from feast import FeatureService
from feature_views import content_popularity_features, user_engagement_features

user_engagement_service = FeatureService(
    name="user_engagement_service",
    features=[user_engagement_features],
    description=(
        "User engagement signals for online retention, re-engagement, "
        "and personalization decisions."
    ),
)


content_popularity_service = FeatureService(
    name="content_popularity_service",
    features=[content_popularity_features],
    description=("Content popularity signals for candidate selection and ranking decisions."),
)


personalization_feature_service = FeatureService(
    name="personalization_feature_service",
    features=[
        user_engagement_features,
        content_popularity_features,
    ],
    description=(
        "Combined user and content feature bundle for contextual "
        "personalization and ranking requests."
    ),
)
