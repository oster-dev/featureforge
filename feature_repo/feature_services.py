"""Feast feature-service definitions for FeatureForge."""

from feast import FeatureService

from feature_views import content_popularity_features, user_engagement_features


personalization_feature_service = FeatureService(
    name="personalization_feature_service",
    features=[
        user_engagement_features,
        content_popularity_features,
    ],
    description=(
        "Feature bundle for personalization and ranking: user engagement "
        "signals plus content popularity signals."
    ),
)