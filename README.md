# Wind Turbine Data Processor

## Overview
This project provides a comprehensive solution for processing wind turbine data,
including cleaning, standardising, and analysing power output data. 
It utilises PySpark for efficient data processing, handles outliers, 
and imputes missing values based on statistical methods. 
The final data is then prepared for storage in a database.

## Features
- Data ingestion from multiple CSV files.
- Data cleaning and preprocessing.
- Calculation of statistical metrics (standard deviation, mean).
- Identification and imputation of outliers.
- Aggregation of data (min, max, average power outputs).
- Database integration for storing processed results.

## Assumptions
- Data is provided in CSV format with specific columns (`timestamp`, `turbine_id`, `wind_speed`, `wind_direction`, `power_output`).
- Outliers are defined as data points where the `power_output` is more than 2 standard deviations from the mean, per turbine, within a 24-hour window.
- Missing or invalid `power_output` values are represented as NULL, 0, or blank strings in the dataset.
- The system configuration, including data paths and database credentials, is specified in a `config.ini` file.
- The timestamp format in the data is 'dd/MM/yyyy HH:mm:ss'.
- A database exists that stores the output data
  - Assumes that the timestamp and turbine_id columns will always be unique in the raw csv files and
  can act as a composite key

## Installation

1. Clone this repository to your local machine.
2. Ensure Python 3.6+ is installed.
3. Install required Python packages:

```bash
pip install -r requirements.txt'''
```

## Usage
```bash
python wind_turbine_data_processor.py
```

## Configuration
- config.ini: Configures data paths and database parameters.
- Modify the [DATA_INGESTION_PATHS] and [DB_CONFIG] sections as per your requirements.

## Database Setup
Ensure your target database is accessible and configured as per the config.ini file.

## Testing
Tests are implemented using pytest. Run the following command to execute the tests:

```bash
pytest
```