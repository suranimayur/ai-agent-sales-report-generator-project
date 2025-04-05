
"""Logging utility module."""
import logging
import os
from datetime import datetime
from typing import Optional

def setup_logging(log_dir: str = 'logs', 
                 log_level: int = logging.INFO,
                 log_file: Optional[str] = None) -> logging.Logger:
    """Set up logging configuration.
    
    Args:
        log_dir: Directory to store log files
        log_level: Logging level
        log_file: Optional specific log file name
        
    Returns:
        Configured logger instance
    """
    os.makedirs(log_dir, exist_ok=True)
    
    if log_file is None:
        log_file = f'sales_processor_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'
    
    log_path = os.path.join(log_dir, log_file)
    
    logger = logging.getLogger('sales_processor')
    logger.setLevel(log_level)
    
    # File handler
    file_handler = logging.FileHandler(log_path)
    file_handler.setLevel(log_level)
    
    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(log_level)
    
    # Formatter
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    file_handler.setFormatter(formatter)
    console_handler.setFormatter(formatter)
    
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    
    return logger
