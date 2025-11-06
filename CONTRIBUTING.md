# Contributing to Real-Time Streaming Data Pipeline

Thank you for your interest in contributing to this project! This guide will help you get started.

## Table of Contents

1. [Code of Conduct](#code-of-conduct)
2. [Getting Started](#getting-started)
3. [Development Setup](#development-setup)
4. [How to Contribute](#how-to-contribute)
5. [Coding Standards](#coding-standards)
6. [Testing](#testing)
7. [Pull Request Process](#pull-request-process)
8. [Reporting Issues](#reporting-issues)

## Code of Conduct

### Our Pledge

We are committed to providing a welcoming and inspiring community for all. Please be respectful and constructive in your interactions.

### Our Standards

**Positive behavior includes**:
- Using welcoming and inclusive language
- Being respectful of differing viewpoints
- Gracefully accepting constructive criticism
- Focusing on what is best for the community
- Showing empathy towards others

**Unacceptable behavior includes**:
- Harassment or discriminatory comments
- Trolling or insulting comments
- Publishing others' private information
- Any conduct that could reasonably be considered inappropriate

## Getting Started

### Prerequisites

- Python 3.8 or higher
- Docker and Docker Compose
- Git
- Basic understanding of:
  - PySpark and streaming data
  - Apache Kafka
  - Data engineering concepts

### Fork and Clone

1. Fork the repository on GitHub
2. Clone your fork locally:
   ```bash
   git clone https://github.com/YOUR_USERNAME/Real-Time-Streaming-Data-Pipeline-and-Analytics-Dashboard.git
   cd Real-Time-Streaming-Data-Pipeline-and-Analytics-Dashboard
   ```
3. Add upstream remote:
   ```bash
   git remote add upstream https://github.com/yadavanujkumar/Real-Time-Streaming-Data-Pipeline-and-Analytics-Dashboard.git
   ```

## Development Setup

### 1. Create Virtual Environment

```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

### 2. Install Dependencies

```bash
# Install all dependencies
pip install -r data_producer/requirements.txt
pip install -r spark_streaming/requirements.txt
pip install -r dashboard/requirements.txt

# Install development dependencies
pip install pytest black flake8 mypy
```

### 3. Setup Pre-commit Hooks (Optional)

```bash
pip install pre-commit
pre-commit install
```

### 4. Start Infrastructure

```bash
cd docker
docker-compose up -d
cd ..
```

## How to Contribute

### Types of Contributions

We welcome various types of contributions:

1. **Bug Fixes**: Fix issues in existing code
2. **Features**: Add new functionality
3. **Documentation**: Improve or add documentation
4. **Tests**: Add or improve test coverage
5. **Performance**: Optimize existing code
6. **Examples**: Add usage examples
7. **Tutorials**: Create learning resources

### Areas for Contribution

- **Data Producers**: Add new data source simulators
- **Stream Processing**: Enhance ETL transformations
- **Storage**: Add new storage backends (S3, Azure, GCS)
- **Visualization**: Improve dashboard or add new views
- **Monitoring**: Add observability features
- **Cloud Integration**: AWS, Azure, GCP deployment scripts
- **Testing**: Unit tests, integration tests
- **Documentation**: Guides, tutorials, API docs

## Coding Standards

### Python Style Guide

We follow PEP 8 with some modifications:

```python
# Good: Clear, descriptive names
def process_sensor_data(df, sensor_id):
    """
    Process sensor data for a specific sensor.
    
    Args:
        df: Input DataFrame
        sensor_id: Sensor identifier
        
    Returns:
        DataFrame: Processed data
    """
    filtered_df = df.filter(df.sensor_id == sensor_id)
    return filtered_df


# Bad: Unclear names, no docstring
def proc(d, s):
    return d.filter(d.s == s)
```

### Code Formatting

Use `black` for automatic formatting:

```bash
black data_producer/ spark_streaming/ dashboard/
```

### Linting

Use `flake8` for linting:

```bash
flake8 data_producer/ spark_streaming/ dashboard/ --max-line-length=100
```

### Type Hints

Use type hints for function signatures:

```python
from typing import Optional, List, Dict

def aggregate_data(
    data: List[Dict[str, float]], 
    window_size: int = 60
) -> Optional[Dict[str, float]]:
    """Aggregate data over window."""
    if not data:
        return None
    # ...
```

### Documentation

- Add docstrings to all functions and classes
- Use Google style docstrings
- Include examples in docstrings when helpful
- Update README.md for significant changes

Example:

```python
def calculate_average_temperature(readings: List[float]) -> float:
    """
    Calculate average temperature from sensor readings.
    
    Args:
        readings: List of temperature readings in Celsius
        
    Returns:
        Average temperature rounded to 2 decimal places
        
    Raises:
        ValueError: If readings list is empty
        
    Example:
        >>> calculate_average_temperature([20.5, 21.0, 20.8])
        20.77
    """
    if not readings:
        raise ValueError("Readings list cannot be empty")
    
    return round(sum(readings) / len(readings), 2)
```

## Testing

### Running Tests

```bash
# Run all tests
pytest tests/

# Run with coverage
pytest --cov=data_producer --cov=spark_streaming --cov=dashboard tests/

# Run specific test file
pytest tests/test_producer.py
```

### Writing Tests

Create test files in the `tests/` directory:

```python
# tests/test_producer.py
import pytest
from data_producer.kafka_producer import IoTSensorDataProducer


def test_generate_sensor_data():
    """Test sensor data generation."""
    producer = IoTSensorDataProducer()
    data = producer.generate_sensor_data()
    
    # Assert required fields exist
    assert 'sensor_id' in data
    assert 'temperature' in data
    assert 'humidity' in data
    
    # Assert value ranges
    assert -50 < data['temperature'] < 100
    assert 0 <= data['humidity'] <= 100


def test_sensor_data_format():
    """Test data format validation."""
    producer = IoTSensorDataProducer()
    data = producer.generate_sensor_data()
    
    # Check types
    assert isinstance(data['sensor_id'], str)
    assert isinstance(data['temperature'], float)
    assert isinstance(data['timestamp'], str)
```

### Integration Tests

```python
# tests/integration/test_pipeline.py
import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="module")
def spark():
    """Create Spark session for testing."""
    spark = SparkSession.builder \
        .appName("test") \
        .master("local[2]") \
        .getOrCreate()
    yield spark
    spark.stop()


def test_etl_transformation(spark):
    """Test ETL transformation logic."""
    # Create test data
    data = [
        ("SENSOR_001", 25.5, 60.0),
        ("SENSOR_002", 26.0, 65.0)
    ]
    df = spark.createDataFrame(data, ["sensor_id", "temperature", "humidity"])
    
    # Apply transformation
    result = df.filter(df.temperature > 25.0)
    
    # Assert
    assert result.count() == 1
```

## Pull Request Process

### 1. Create a Branch

```bash
# Update main branch
git checkout main
git pull upstream main

# Create feature branch
git checkout -b feature/your-feature-name
```

Branch naming conventions:
- `feature/` - New features
- `bugfix/` - Bug fixes
- `docs/` - Documentation updates
- `perf/` - Performance improvements

### 2. Make Changes

- Write clear, concise code
- Add tests for new functionality
- Update documentation
- Follow coding standards

### 3. Commit Changes

Write meaningful commit messages:

```bash
git add .
git commit -m "Add aggregation window configuration option

- Add window_size parameter to streaming job
- Update documentation with new parameter
- Add tests for window configuration"
```

Commit message format:
- First line: Brief summary (50 chars or less)
- Blank line
- Detailed description with:
  - What changed
  - Why it changed
  - Any breaking changes

### 4. Push Changes

```bash
git push origin feature/your-feature-name
```

### 5. Create Pull Request

1. Go to GitHub and create a Pull Request
2. Fill out the PR template:
   - Description of changes
   - Related issues
   - Testing performed
   - Screenshots (if applicable)
3. Request review from maintainers

### PR Checklist

- [ ] Code follows project style guidelines
- [ ] Self-review of code completed
- [ ] Comments added to complex code
- [ ] Documentation updated
- [ ] Tests added/updated
- [ ] All tests pass
- [ ] No new warnings
- [ ] Branch is up-to-date with main

### Review Process

- Maintainers will review your PR
- Address feedback and comments
- Make requested changes
- Once approved, PR will be merged

## Reporting Issues

### Bug Reports

Use the issue template and include:

1. **Description**: Clear description of the bug
2. **Steps to Reproduce**:
   - Step 1
   - Step 2
   - ...
3. **Expected Behavior**: What should happen
4. **Actual Behavior**: What actually happens
5. **Environment**:
   - OS and version
   - Python version
   - Spark version
   - Docker version
6. **Logs**: Relevant error messages or logs
7. **Screenshots**: If applicable

### Feature Requests

Include:

1. **Problem**: What problem does this solve?
2. **Proposed Solution**: Describe your idea
3. **Alternatives**: Other solutions considered
4. **Additional Context**: Any other information

## Development Tips

### Local Testing

```bash
# Quick test of producer
python data_producer/kafka_producer.py --count 10

# Test Spark job locally
python spark_streaming/streaming_etl.py --mode console

# Test dashboard
streamlit run dashboard/app.py
```

### Debugging

```python
# Add logging
import logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)
logger.debug("Debug message here")

# Use pdb for debugging
import pdb; pdb.set_trace()
```

### Performance Profiling

```python
# Time operations
import time
start = time.time()
# ... operation ...
print(f"Time taken: {time.time() - start:.2f}s")

# Profile with cProfile
python -m cProfile -o profile.stats spark_streaming/streaming_etl.py
```

## Resources

### Learning Resources

- [PySpark Documentation](https://spark.apache.org/docs/latest/api/python/)
- [Kafka Documentation](https://kafka.apache.org/documentation/)
- [Streamlit Documentation](https://docs.streamlit.io/)
- [Python Testing with pytest](https://docs.pytest.org/)

### Communication

- **GitHub Issues**: Bug reports and feature requests
- **Pull Requests**: Code contributions and discussions
- **Email**: For private matters

## Recognition

Contributors will be recognized in:
- README.md acknowledgments
- CONTRIBUTORS.md file
- Release notes

## Questions?

If you have questions, feel free to:
1. Open a GitHub issue
2. Check existing documentation
3. Review closed issues/PRs

Thank you for contributing! 🎉
