import os
import shutil
from pathlib import Path
import yaml

def create_directory_structure():
    """Create the main project directory structure."""
    directories = [
        'src',
        'src/data_ingestion',
        'src/data_processing',
        'src/data_validation',
        'src/reporting',
        'src/utils',
        'tests',
        'tests/data_ingestion',
        'tests/data_processing',
        'tests/data_validation',
        'tests/reporting',
        'config',
        'data',
        'data/raw',
        'data/processed',
        'data/reports',
        'data/curated',
        'docs',
        'logs',
        'notebooks',
        '.github/workflows'
    ]
    
    for directory in directories:
        os.makedirs(directory, exist_ok=True)
        print(f"Created directory: {directory}")

def create_config_files():
    """Create configuration files."""
    # Create config.yaml
    config_data = {
        'paths': {
            'raw_data': 'data/raw',
            'processed_data': 'data/processed',
            'reports': 'data/reports',
            'curated_data': 'data/curated',
            'logs': 'logs'
        },
        'spark': {
            'app_name': 'sales_data_processor',
            'master': 'local[*]',
            'executor_memory': '4g',
            'driver_memory': '2g'
        },
        'processing': {
            'batch_size': 10000,
            'max_workers': 4,
            'chunk_size': 1000
        },
        'validation': {
            'min_price': 0,
            'max_price': 10000,
            'min_quantity': 1,
            'max_quantity': 100,
            'required_columns': [
                'Order_ID', 'Order_Date', 'Customer_ID', 'Product_ID',
                'Quantity', 'Unit_Price', 'Total_Price', 'Final_Price'
            ]
        },
        'reporting': {
            'formats': ['csv', 'excel', 'json'],
            'default_format': 'excel',
            'date_format': '%Y-%m-%d',
            'currency': 'USD'
        }
    }
    
    with open('config/config.yaml', 'w') as f:
        yaml.dump(config_data, f, default_flow_style=False)
    print("Created config/config.yaml")

def create_source_files():
    """Create source code files with basic structure."""
    # Create __init__.py files
    for root, dirs, files in os.walk('src'):
        if '__init__.py' not in files:
            with open(os.path.join(root, '__init__.py'), 'w') as f:
                f.write('"""Package initialization."""\n')
    
    # Create main modules
    modules = {
        'src/data_ingestion/ingest.py': '''
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
''',
        'src/data_processing/processor.py': '''
"""Data processing module."""
import pandas as pd
from typing import Dict, Any
import logging

class DataProcessor:
    """Handles data processing operations."""
    
    def __init__(self, config: Dict[str, Any]):
        """Initialize the data processor.
        
        Args:
            config: Configuration dictionary
        """
        self.config = config
        self.logger = logging.getLogger(__name__)
    
    def process_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Process the input data.
        
        Args:
            df: Input DataFrame
            
        Returns:
            Processed DataFrame
        """
        try:
            self.logger.info("Starting data processing")
            # Add processing logic here
            return df
        except Exception as e:
            self.logger.error(f"Error processing data: {str(e)}")
            raise
''',
        'src/data_validation/validator.py': '''
"""Data validation module."""
import pandas as pd
from typing import Dict, Any, List
import logging

class DataValidator:
    """Handles data validation operations."""
    
    def __init__(self, config: Dict[str, Any]):
        """Initialize the data validator.
        
        Args:
            config: Configuration dictionary
        """
        self.config = config
        self.logger = logging.getLogger(__name__)
    
    def validate_data(self, df: pd.DataFrame) -> List[str]:
        """Validate the input data.
        
        Args:
            df: Input DataFrame
            
        Returns:
            List of validation errors
        """
        errors = []
        try:
            self.logger.info("Starting data validation")
            # Add validation logic here
            return errors
        except Exception as e:
            self.logger.error(f"Error validating data: {str(e)}")
            raise
''',
        'src/reporting/reporter.py': '''
"""Reporting module."""
import pandas as pd
from typing import Dict, Any
import logging

class ReportGenerator:
    """Handles report generation operations."""
    
    def __init__(self, config: Dict[str, Any]):
        """Initialize the report generator.
        
        Args:
            config: Configuration dictionary
        """
        self.config = config
        self.logger = logging.getLogger(__name__)
    
    def generate_report(self, df: pd.DataFrame, report_type: str) -> str:
        """Generate a report from the data.
        
        Args:
            df: Input DataFrame
            report_type: Type of report to generate
            
        Returns:
            Path to the generated report
        """
        try:
            self.logger.info(f"Generating {report_type} report")
            # Add report generation logic here
            return "report_path"
        except Exception as e:
            self.logger.error(f"Error generating report: {str(e)}")
            raise
''',
        'src/utils/logger.py': '''
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
'''
    }
    
    for file_path, content in modules.items():
        with open(file_path, 'w') as f:
            f.write(content)
        print(f"Created {file_path}")

def create_test_files():
    """Create test files with basic structure."""
    test_files = {
        'tests/data_ingestion/test_ingest.py': '''
"""Tests for data ingestion module."""
import pytest
from src.data_ingestion.ingest import DataIngestion

def test_data_ingestion_initialization():
    """Test DataIngestion class initialization."""
    ingestion = DataIngestion()
    assert ingestion is not None
''',
        'tests/data_processing/test_processor.py': '''
"""Tests for data processing module."""
import pytest
import pandas as pd
from src.data_processing.processor import DataProcessor

def test_data_processing():
    """Test data processing functionality."""
    processor = DataProcessor({})
    df = pd.DataFrame({'test': [1, 2, 3]})
    result = processor.process_data(df)
    assert result is not None
'''
    }
    
    for file_path, content in test_files.items():
        with open(file_path, 'w') as f:
            f.write(content)
        print(f"Created {file_path}")

def create_documentation():
    """Create documentation files."""
    # Create README.md
    readme_content = '''# Sales Data Processing Pipeline

A production-grade data processing pipeline for sales data analysis.

## Project Structure

```
.
├── config/             # Configuration files
├── data/              # Data directories
│   ├── raw/          # Raw input data
│   ├── processed/    # Processed data
│   ├── reports/      # Generated reports
│   └── curated/      # Curated datasets
├── docs/             # Documentation
├── logs/             # Log files
├── notebooks/        # Jupyter notebooks
├── src/              # Source code
│   ├── data_ingestion/
│   ├── data_processing/
│   ├── data_validation/
│   ├── reporting/
│   └── utils/
├── tests/            # Test files
└── .github/workflows # CI/CD workflows
```

## Setup

1. Create a virtual environment:
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\\Scripts\\activate
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

## Usage

1. Configure the pipeline in `config/config.yaml`
2. Run the main pipeline:
```bash
python src/main.py
```

## Testing

Run tests using pytest:
```bash
pytest tests/
```

## Contributing

1. Fork the repository
2. Create a feature branch
3. Commit your changes
4. Push to the branch
5. Create a Pull Request

## License

This project is licensed under the MIT License.
'''
    
    with open('README.md', 'w', encoding='utf-8') as f:
        f.write(readme_content)
    print("Created README.md")

def create_github_workflow():
    """Create GitHub Actions workflow file."""
    workflow_content = '''name: CI/CD Pipeline

on:
  push:
    branches: [ main ]
  pull_request:
    branches: [ main ]

jobs:
  test:
    runs-on: ubuntu-latest
    strategy:
      matrix:
        python-version: [3.8, 3.9, "3.10"]

    steps:
    - uses: actions/checkout@v2
    
    - name: Set up Python ${{ matrix.python-version }}
      uses: actions/setup-python@v2
      with:
        python-version: ${{ matrix.python-version }}
    
    - name: Install dependencies
      run: |
        python -m pip install --upgrade pip
        pip install -r requirements.txt
        pip install pytest
    
    - name: Run tests
      run: |
        pytest tests/
'''
    
    os.makedirs('.github/workflows', exist_ok=True)
    with open('.github/workflows/ci.yml', 'w') as f:
        f.write(workflow_content)
    print("Created .github/workflows/ci.yml")

def main():
    """Main function to create the project structure."""
    print("Creating production-grade project structure...")
    
    create_directory_structure()
    create_config_files()
    create_source_files()
    create_test_files()
    create_documentation()
    create_github_workflow()
    
    print("\nProject structure created successfully!")
    print("Next steps:")
    print("1. Create and activate a virtual environment")
    print("2. Install dependencies from requirements.txt")
    print("3. Configure the pipeline in config/config.yaml")
    print("4. Start developing your data processing pipeline")

if __name__ == "__main__":
    main() 