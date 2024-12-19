import argparse
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from datetime import datetime


def validate_date(date_str):
    if not date_str:
        return None
    try:
        datetime.strptime(date_str, '%Y-%m-%d')
        return date_str
    except ValueError:
        raise argparse.ArgumentTypeError(f"Invalid date format: {date_str}. Use YYYY-MM-DD")


def load_airport_data(spark, csv_path):
    """
    Load airport reference data using timezone offset
    """
    return spark.read.option("header", "false") \
        .option("delimiter", ",") \
        .csv(csv_path) \
        .select(
        col("_c0").cast("integer").alias("id"),
        col("_c1").alias("name"),
        col("_c2").alias("city"),
        col("_c3").alias("country"),
        col("_c4").alias("iata"),
        col("_c9").cast("integer").alias("timezone_offset")
    ) \
        .filter(col("iata").isNotNull())


def load_booking_data(spark, json_path, start_date=None, end_date=None):
    """
    Load and filter booking data by date range, extracting passenger info including age
    """
    # Initial explode for flights
    df = spark.read.option("mode", "PERMISSIVE").json(json_path) \
        .withColumn(
        "flights",
        explode(col("event.DataElement.travelrecord.productsList"))
    )

    # Filter out rows with corrupt records
    if '_corrupt_record' in df.columns:
        df = df.filter(col("_corrupt_record").isNull())

    # Explode the passengers array to get individual passenger records
    df = df.select(
        col("flights.flight.operatingAirline").alias("airline"),
        col("flights.flight.departureDate").cast("timestamp").alias("departure_date"),
        col("flights.flight.originAirport").alias("origin"),
        col("flights.flight.destinationAirport").alias("destination"),
        col("flights.bookingStatus").alias("status"),
        explode(col("event.DataElement.travelrecord.passengersList")).alias("passenger")
    )

    # Extract passenger age
    df = df.select(
        "airline",
        "departure_date",
        "origin",
        "destination",
        "status",
        col("passenger.age").cast("integer").alias("age")
    )

    # Filter for confirmed bookings
    df = df.filter(col("status") == "CONFIRMED")

    # Apply date range
    if start_date:
        df = df.filter(col("departure_date") >= start_date)
    if end_date:
        df = df.filter(col("departure_date") <= end_date)

    # Filter for KLM flights
    df = df.filter(col("airline") == "KL")

    return df


def analyze_bookings(bookings_df, airports_df):
    """
    Analyze booking data including passenger age analysis
    """
    # Join with airports to get timezone offset information
    enriched_df = bookings_df.alias("bookings").join(
        airports_df.alias("airports"),
        col("bookings.destination") == col("airports.iata"),
        "inner"
    )

    # Convert UTC departure time to local time using timezone offset
    enriched_df = enriched_df.withColumn(
        "minutes_to_add",
        col("timezone_offset") * 60
    ).withColumn(
        "local_departure",
        from_unixtime(
            unix_timestamp(col("departure_date")) + col("minutes_to_add") * 60
        )
    )

    # Add season and day of week
    enriched_df = enriched_df.withColumn(
        "season",
        when((month(col("departure_date")).isin(12, 1, 2)), "Winter")
        .when((month(col("departure_date")).isin(3, 4, 5)), "Spring")
        .when((month(col("departure_date")).isin(6, 7, 8)), "Summer")
        .when((month(col("departure_date")).isin(9, 10, 11)), "Fall")
    ).withColumn(
        "day_of_week",
        date_format(col("local_departure"), "EEEE")
    )

    # Group and aggregate the data with age statistics
    popularity = enriched_df.groupBy(
        col("airports.country"),
        "season",
        "day_of_week"
    ).agg(
        count("*").alias("number_of_passengers"),
        round(avg("age"), 1).alias("average_age"),
        min("age").alias("youngest_age"),
        max("age").alias("oldest_age")
    ).orderBy(
        desc("number_of_passengers")
    )

    return popularity


def parse_arguments():
    """
    Parse command line arguments
    """
    parser = argparse.ArgumentParser(description='Analyze KLM flight bookings')
    parser.add_argument('--booking-path',
                        default='/opt/bitnami/spark/data/bookings/booking.json',
                        help='Path to booking data JSON file')
    parser.add_argument('--airport-path',
                        default='/opt/bitnami/spark/data/airports/airports.dat',
                        help='Path to airport data CSV file')
    parser.add_argument('--start-date',
                        type=validate_date,
                        help='Start date (YYYY-MM-DD) - optional, defaults to earliest date in data')
    parser.add_argument('--end-date',
                        type=validate_date,
                        help='End date (YYYY-MM-DD) - optional, defaults to latest date in data')
    parser.add_argument('--no-print', action='store_true', help='Do not print results in terminal')
    parser.add_argument('--csv', action='store_true', help='Save results as CSV')
    parser.add_argument('--parquet', action='store_true', help='Save results as Parquet')
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_arguments()

    SPARK = SparkSession.builder \
        .appName("KLM Flight Analysis") \
        .master("spark://spark-master:7077") \
        .config("spark.driver.host", "spark-master") \
        .getOrCreate()

    BOOKING_PATH = args.booking_path
    AIRPORTS_PATH = args.airport_path
    START_DATE = args.start_date
    END_DATE = args.end_date
    OUTPUT_PATH = '/opt/bitnami/spark/data/'

    # Validate dates if both are provided
    if START_DATE and END_DATE and START_DATE > END_DATE:
        raise ValueError("Start date must be before end date")

    airports_df = load_airport_data(SPARK, AIRPORTS_PATH)
    nl_airports_df = airports_df.filter(col("country") == "Netherlands")
    bookings_df = load_booking_data(SPARK, BOOKING_PATH, START_DATE, END_DATE)

    bookings_df = bookings_df.alias("bookings").join(
        nl_airports_df.alias("nl_airports").select("iata"),
        col("bookings.origin") == col("nl_airports.iata"),
        "inner"
    )

    results = analyze_bookings(bookings_df, airports_df)

    if args.csv:
        csv_path = f'{OUTPUT_PATH}/output.csv'
        results.write.mode("overwrite").csv(csv_path)
        logging.info(f"Results saved as CSV at: {csv_path}")

    if args.parquet:
        parquet_path = f'{OUTPUT_PATH}/output.parquet'
        results.write.mode("overwrite").parquet(parquet_path)
        logging.info(f"Results saved as Parquet at: {parquet_path}")

    if not args.no_print:
        results.show(results.count(), False)
