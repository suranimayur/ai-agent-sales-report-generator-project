# AI Agent Sales Report Generator

A production-grade data analytics pipeline for processing and analyzing sales data using PySpark. This project demonstrates end-to-end data processing, from raw data ingestion to generating comprehensive business reports.

## 🚀 Features

- **Data Processing Pipeline**
  - Raw data ingestion
  - Data validation and cleaning
  - Complex business aggregations
  - Multi-format report generation (CSV, Parquet)

- **Analytics Capabilities**
  - Daily sales metrics
  - Product category performance
  - Regional customer analysis
  - Payment method analysis
  - Top-performing products
  - Order status analysis
  - Shipping analysis

- **Technical Features**
  - Optimized PySpark processing
  - Configurable pipeline settings
  - Automated report generation
  - Comprehensive logging
  - Error handling and validation

## 📁 Project Structure

```
.
├── config/
│   └── config.yaml           # Configuration settings
├── data/
│   ├── raw/                 # Raw input data
│   ├── processed/           # Processed data
│   ├── curated/            # Final analytics reports
│   └── reports/            # Generated reports
├── src/
│   ├── data_ingestion/     # Data loading modules
│   ├── data_processing/    # Data processing logic
│   ├── data_validation/    # Data validation rules
│   ├── reporting/         # Report generation
│   └── utils/             # Utility functions
├── tests/                  # Test suites
├── docs/                   # Documentation
├── logs/                   # Log files
└── notebooks/             # Jupyter notebooks
```

## 🛠️ Installation

1. Clone the repository:
```bash
git clone https://github.com/suranimayur/ai-agent-sales-report-generator-project.git
cd ai-agent-sales-report-generator-project
```

2. Create and activate a conda environment:
```bash
conda create -p .venv python=3.12 -y
conda activate D:\agentic-ai-projects\.venv
```

3. Install dependencies:
```bash
pip install -r requirements.txt
```

## ⚙️ Configuration

The project uses a YAML configuration file (`config/config.yaml`) for settings:

```yaml
data_paths:
  raw: "data/raw"
  processed: "data/processed"
  curated: "data/curated"
  reports: "data/reports"

spark:
  app_name: "SalesAnalytics"
  executor_memory: "2g"
  driver_memory: "2g"
  shuffle_partitions: 8
  default_parallelism: 8
  adaptive_enabled: true
  adaptive_coalesce_enabled: true
  advisory_partition_size: "128MB"
```

## 📊 Data Processing Pipeline

1. **Data Ingestion**
   - Reads raw sales data from CSV
   - Validates data structure and content
   - Handles missing values and errors

2. **Data Processing**
   - Performs complex aggregations
   - Calculates key business metrics
   - Optimizes performance with proper partitioning

3. **Report Generation**
   - Creates multiple report types
   - Saves in CSV and Parquet formats
   - Includes timestamps for versioning

## 📈 Analytics Reports

The pipeline generates the following reports:

1. **Daily Sales Metrics**
   - Daily sales and profit
   - Order and customer counts
   - Average discount rates

2. **Product Analysis**
   - Category performance
   - Top-selling products
   - Profit margins by category

3. **Customer Analysis**
   - Regional sales distribution
   - Customer segmentation
   - Order value analysis

4. **Operational Metrics**
   - Payment method analysis
   - Order status distribution
   - Shipping performance

## 🧪 Testing

Run tests using pytest:
```bash
pytest tests/
```

## 📝 Logging

- Comprehensive logging system
- Error tracking and reporting
- Performance monitoring

## 🔄 CI/CD

GitHub Actions workflow for:
- Automated testing
- Code quality checks
- Documentation updates

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Commit your changes
4. Push to the branch
5. Create a Pull Request

## 📄 License

This project is licensed under the MIT License - see the LICENSE file for details.

## 👥 Authors

- Surani Mayur - Initial work

## 🙏 Acknowledgments

- PySpark community
- Open-source contributors
- Data engineering best practices
