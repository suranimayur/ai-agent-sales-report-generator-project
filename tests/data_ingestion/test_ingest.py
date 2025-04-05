
"""Tests for data ingestion module."""
import pytest
from src.data_ingestion.ingest import DataIngestion

def test_data_ingestion_initialization():
    """Test DataIngestion class initialization."""
    ingestion = DataIngestion()
    assert ingestion is not None
