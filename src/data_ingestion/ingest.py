
"""Data ingestion module."""
import pandas as pd
from pathlib import Path
import yaml
import logging
from typing import Optional, Dict, Any

class DataIngestion:
    """Handles data ingestion operations."""
    
    def __init__(self, config_path: str = 'config/config.yaml'):
        """Initialize the data ingestion class.
        
        Args:
            config_path: Path to the configuration file
        """
        self.config = self._load_config(config_path)
        self.logger = self._setup_logging()
    
    def _load_config(self, config_path: str) -> Dict[str, Any]:
        """Load configuration from YAML file."""
        with open(config_path, 'r') as f:
            return yaml.safe_load(f)
    
    def _setup_logging(self) -> logging.Logger:
        """Set up logging configuration."""
        logger = logging.getLogger(__name__)
        logger.setLevel(logging.INFO)
        return logger
    
    def read_data(self, file_path: str) -> pd.DataFrame:
        """Read data from file.
        
        Args:
            file_path: Path to the input file
            
        Returns:
            DataFrame containing the loaded data
        """
        try:
            self.logger.info(f"Reading data from {file_path}")
            return pd.read_csv(file_path)
        except Exception as e:
            self.logger.error(f"Error reading data: {str(e)}")
            raise
