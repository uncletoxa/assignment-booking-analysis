# Commercial Booking Analysis

This Spark application analyzes KLM flight bookings to provide insights into the most popular destinations per season and day of the week over a given time period. The application processes booking data in JSON format and airport reference data in CSV format to generate a report for the KLM Network department.

## Considerations

Reasons for using Spark as a framework:
* It is highly scalable and easy to deploy in a Hadoop cluster
* It supports both local execution and cluster deployment. While local execution is less convenient than pure Python, it's possible to provide a simple interface using Docker Compose for local cluster creation
* Both local file systems and HDFS URIs are supported out of the box
* Simple implementation for streaming is possible using the Spark Streaming framework (not yet implemented)
* The Spark framework is widely adopted, meaning it can be maintained by a broad range of engineers/analysts

## Features

- Load and process booking data from JSON files (local directory or HDFS)
- Load airport reference data from a CSV file
- Optionally filter bookings based on a specified date range
- Analyze the most popular destinations per season and day of the week
- Aggregate passenger information including average age, youngest age, and oldest age
- Save the analysis results in CSV or Parquet format
- Handle corrupt or invalid JSON records gracefully

## Prerequisites

- Apache Spark cluster (version 3.5.3 or later)
- Java (version 17.0 or later)
- Python (version 3.12)
- Docker and Docker Compose (optional, for running the application in a containerized environment)

## Getting Started

1. Clone the repository and navigate to the project directory:
   ```bash
   git clone git@github.com:uncletoxa/assignment-booking-analysis.git
   cd assignment-booking-analysis
   ```

2. Prepare the input data:
   - Place the booking data JSON files in a directory accessible by the Spark cluster (local or HDFS)
   - Place the airport reference data CSV file in a directory accessible by the Spark cluster

3. Update the `docker-compose.yml` file if necessary:
   - Modify the volume mappings to point to your input data file directories

4. Set required permissions for application file access:
   ```bash
   chmod 777 data/* scripts/*
   ```

5. Run the application:
   - Using Docker Compose (use `docker-compose` for versions below 2.0.0):
     ```bash
     # Start Spark cluster
     docker compose up -d
     
     # Execute analysis
     docker exec -it $(docker ps -q -f name=spark-master) \
       spark-submit \
       --master spark://spark-master:7077 \
       /opt/bitnami/spark/scripts/flight_analysis.py \
       --start-date 2019-01-01 \
       --end-date 2019-12-31 \
       --csv --parquet --no-print
     
     # Shut down cluster when finished
     docker compose down
     ```
   
   - Without Docker:
     ```bash
     spark-submit \
       --master spark://spark-master:7077 \
       /opt/bitnami/spark/scripts/flight_analysis.py \
       --start-date 2019-01-01 \
       --end-date 2019-12-31 \
       --csv --parquet --no-print
     ```

6. The application will process the data and save the analysis results in the specified output format(s) to the provided output paths.

## Configuration

The application accepts the following command-line arguments:

- `--booking-path`: Path to the booking data JSON files (local directory or HDFS URI). Default: `data/bookings/booking.json`
- `--airport-path`: Path to the airport reference data CSV file. Default: `data/airports/airports.dat`
- `--start-date`: Start date of the analysis period (YYYY-MM-DD)
- `--end-date`: End date of the analysis period (YYYY-MM-DD)
- `--csv`: Save results as CSV files in the `data` folder
- `--parquet`: Save results as Parquet files in the `data` folder
- `--no-print`: Suppress printing of the final dataframe to stdout

## Known Issues
- Age data may sometimes be invalid (e.g., `-16`) or missing (`None`). These values are currently included in the output as-is and not filtered out.
```
