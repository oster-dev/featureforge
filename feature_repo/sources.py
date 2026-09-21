"""Feast batch sources for FeatureForge offline feature data."""

from feast import FileSource

user_features_source = FileSource(
    name="user_engagement_batch_source",
    path="../output/offline_store/user_engagement_features",
    timestamp_field="observation_time",
)


content_features_source = FileSource(
    name="content_popularity_batch_source",
    path="../output/offline_store/content_popularity_features",
    timestamp_field="observation_time",
)
