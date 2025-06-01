from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import from_json, col, count, expr
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, BooleanType # Added BooleanType
from pyspark.sql.avro.functions import from_avro

# --- Configuration Placeholders (Ideally, externalize these) ---
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
POSTGRES_JDBC_URL = "jdbc:postgresql://localhost:5432/voting"
POSTGRES_USER = "postgres" # Externalize this
POSTGRES_PASSWORD = "postgres" # Externalize this
POSTGRES_DRIVER = "org.postgresql.Driver"

# IMPORTANT: Update this path to actual PostgreSQL JDBC JAR location
# Alternatively, provide it via spark-submit --jars
POSTGRES_JAR_PATH = "/Users/nicholasklos/PycharmProjects/VotingDataEng/postgresql-42.7.1.jar" # <--- UPDATE THIS PATH

# IMPORTANT: Update these paths to actual writable directories for checkpoints
CHECKPOINT_BASE_DIR = "/Users/nicholasklos/PycharmProjects/VotingDataEng/spark_checkpoints" # <--- UPDATE THIS PATH

# Define schemas for Kafka topics and PostgreSQL tables
# Schema for messages from 'votes_topic' (produced by voting.py)
vote_kafka_schema = StructType([
    StructField("voter_id", StringType(), True),
    StructField("voter_name", StringType(), True), # Included for context if needed directly from vote
    StructField("candidate_id", StringType(), True),
    StructField("candidate_name", StringType(), True), # Included for context
    StructField("party_affiliation", StringType(), True), # Included for context
    StructField("voting_time", StringType(), True)  # Will be cast to TimestampType
])

# Schema for 'voters' table in PostgreSQL (matching what main.py creates)
voter_pg_schema = StructType([
    StructField("voter_id", StringType(), True),
    StructField("voter_name", StringType(), True),
    StructField("date_of_birth", StringType(), True), # Keep as String or parse if needed
    StructField("gender", StringType(), True),
    StructField("nationality", StringType(), True),
    StructField("registration_number", StringType(), True),
    StructField("address_street", StringType(), True),
    StructField("address_city", StringType(), True),
    StructField("address_state", StringType(), True),
    StructField("address_country", StringType(), True),
    StructField("address_postcode", StringType(), True),
    StructField("email", StringType(), True),
    StructField("phone_number", StringType(), True),
    StructField("cell_number", StringType(), True),
    StructField("picture", StringType(), True),
    StructField("registered_age", IntegerType(), True)
])

# Schema for 'candidates' table in PostgreSQL (matching what main.py creates)
candidate_pg_schema = StructType([
    StructField("candidate_id", StringType(), True),
    StructField("candidate_name", StringType(), True),
    StructField("party_affiliation", StringType(), True),
    StructField("biography", StringType(), True),
    StructField("campaign_platform", StringType(), True),
    StructField("photo_url", StringType(), True)
])


def write_stream_to_kafka(df_to_write: DataFrame, topic_name: str, checkpoint_location: str):
    """Helper function to write a streaming DataFrame to a Kafka topic."""
    return (df_to_write
            .selectExpr("to_json(struct(*)) AS value")
            .writeStream
            .format("kafka")
            .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
            .option("topic", topic_name)
            .option("checkpointLocation", checkpoint_location)
            .outputMode("update") # Use "complete" for non-windowed aggregations if all groups always output
            .start())


if __name__ == "__main__":
    spark = (SparkSession.builder
             .appName("RealTimeElectionAnalysis")
             .master("local[*]")
             .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:3.5.0") # Ensure version matches Spark
             .config("spark.jars", POSTGRES_JAR_PATH) # PostgreSQL driver
             .config("spark.sql.streaming.forceDeleteCheckpointLocation", "true") # For dev: auto-delete checkpoint if query fails
             # .config("spark.sql.adaptive.enabled", "false") # Usually not needed to disable for newer Spark versions
             .getOrCreate())

    spark.sparkContext.setLogLevel("WARN") # Reduce verbosity
    logger = spark.sparkContext._jvm.org.apache.log4j.LogManager.getLogger(__name__)
    logger.info("Spark Session Initialized for Election Analysis")

    # 1. Read static/batch data from PostgreSQL
    logger.info("Reading candidates data from PostgreSQL...")
    candidates_df_pg = spark.read \
        .format("jdbc") \
        .option("url", POSTGRES_JDBC_URL) \
        .option("dbtable", "candidates") \
        .option("user", POSTGRES_USER) \
        .option("password", POSTGRES_PASSWORD) \
        .option("driver", POSTGRES_DRIVER) \
        .load()
    candidates_df_pg.persist() # Cache for reuse in joins
    logger.info(f"Loaded {candidates_df_pg.count()} candidates. Schema:")
    candidates_df_pg.printSchema()

    logger.info("Reading voters data from PostgreSQL...")
    voters_df_pg = spark.read \
        .format("jdbc") \
        .option("url", POSTGRES_JDBC_URL) \
        .option("dbtable", "voters") \
        .option("user", POSTGRES_USER) \
        .option("password", POSTGRES_PASSWORD) \
        .option("driver", POSTGRES_DRIVER) \
        .load()
    voters_df_pg.persist() # Cache for reuse in joins
    logger.info(f"Loaded {voters_df_pg.count()} voters. Schema:")
    voters_df_pg.printSchema()


    # 2. Read streaming data from Kafka 'votes_topic'
    logger.info(f"Reading stream from Kafka topic: votes_topic")
    votes_stream_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", "votes_topic") \
        .option("startingOffsets", "earliest") \
        .load() \
        .selectExpr("CAST(value AS STRING) as json_value") \
        .select(from_json(col("json_value"), vote_kafka_schema).alias("data")) \
        .select("data.*") \
        .withColumn("voting_time_ts", col("voting_time").cast(TimestampType())) \
        .withWatermark("voting_time_ts", "1 minute") # Watermark for potential stateful operations

    logger.info("votes_topic stream schema after parsing:")
    votes_stream_df.printSchema()

    # 3. Enrich vote stream with voter and candidate details (stream-batch joins)
    # Alias DataFrames to avoid ambiguous column names after join
    enriched_votes_df = votes_stream_df.alias("v") \
        .join(voters_df_pg.alias("vr"), expr("v.voter_id == vr.voter_id"), "inner") \
        .join(candidates_df_pg.alias("c"), expr("v.candidate_id == c.candidate_id"), "inner") \
        .select(
        col("v.voter_id").alias("vote_voter_id"), # Keep original voter_id from vote event
        col("v.candidate_id").alias("vote_candidate_id"), # Keep original candidate_id from vote event
        col("v.voting_time_ts"),
        # Voter details from PostgreSQL voters table
        col("vr.voter_name"),
        col("vr.gender").alias("voter_gender"),
        col("vr.registered_age").alias("voter_registered_age"),
        col("vr.address_city").alias("voter_city"),
        col("vr.address_state").alias("voter_state"),
        col("vr.address_country").alias("voter_country"),
        # Candidate details from PostgreSQL candidates table
        col("c.candidate_name"), # This was also in vote_kafka_schema, prefer source of truth
        col("c.party_affiliation"), # This was also in vote_kafka_schema
        col("c.photo_url")
    )

    logger.info("Enriched votes stream schema:")
    enriched_votes_df.printSchema()

    # 4. Perform Aggregations

    # Aggregation 1: Votes per candidate
    votes_per_candidate_df = enriched_votes_df \
        .groupBy(
        col("vote_candidate_id"), # Use the ID from the vote event
        col("candidate_name"),    # Use the name from the joined candidates table
        col("party_affiliation"),
        col("photo_url")
    ) \
        .agg(count("vote_voter_id").alias("total_votes"))

    # Aggregation 2: Turnout by voter's state
    turnout_by_state_df = enriched_votes_df \
        .groupBy(col("voter_state")) \
        .agg(count("vote_voter_id").alias("total_votes"))

    # Aggregation 3: Turnout by voter's age
    turnout_by_age_df = enriched_votes_df \
        .groupBy(col("voter_registered_age")) \
        .agg(count("vote_voter_id").alias("total_votes"))

    # Aggregation 4: Turnout by voter's gender
    turnout_by_gender_df = enriched_votes_df \
        .groupBy(col("voter_gender")) \
        .agg(count("vote_voter_id").alias("total_votes"))

    # Aggregation 5: Total votes per party
    party_total_votes_df = enriched_votes_df \
        .groupBy(col("party_affiliation")) \
        .agg(count("vote_voter_id").alias("total_votes"))

    # 5. Write aggregated results to Kafka topics (or other sinks like console for debugging)

    # Sink 1: Votes per candidate
    votes_per_candidate_query = write_stream_to_kafka(
        votes_per_candidate_df,
        "aggregated_votes_per_candidate",
        f"{CHECKPOINT_BASE_DIR}/checkpoint_votes_per_candidate"
    )
    # For debugging, write to console:
    # votes_per_candidate_df.writeStream \
    #     .outputMode("update") \
    #     .format("console") \
    #     .option("truncate", "false") \
    #     .start()


    # Sink 2: Turnout by state
    turnout_by_state_query = write_stream_to_kafka(
        turnout_by_state_df,
        "aggregated_turnout_by_state",
        f"{CHECKPOINT_BASE_DIR}/checkpoint_turnout_by_state"
    )

    # Sink 3: Turnout by age
    turnout_by_age_query = write_stream_to_kafka(
        turnout_by_age_df,
        "aggregated_turnout_by_age",
        f"{CHECKPOINT_BASE_DIR}/checkpoint_turnout_by_age"
    )

    # Sink 4: Turnout by gender
    turnout_by_gender_query = write_stream_to_kafka(
        turnout_by_gender_df,
        "aggregated_turnout_by_gender",
        f"{CHECKPOINT_BASE_DIR}/checkpoint_turnout_by_gender"
    )

    # Sink 5: Total votes per party
    party_total_votes_query = write_stream_to_kafka(
        party_total_votes_df,
        "aggregated_party_total_votes",
        f"{CHECKPOINT_BASE_DIR}/checkpoint_party_total_votes"
    )

    logger.info("Streaming queries started. Awaiting termination...")
    # Await termination for all queries
    # spark.streams.awaitAnyTermination() # Use this if for app to run until any stream fails or is stopped

    # Or await individual one though awaitAnyTermination is common
    try:
        votes_per_candidate_query.awaitTermination()
        turnout_by_state_query.awaitTermination()
        turnout_by_age_query.awaitTermination()
        turnout_by_gender_query.awaitTermination()
        party_total_votes_query.awaitTermination()
    except Exception as e:
        logger.error(f"A streaming query failed: {e}", exc_info=True)
    finally:
        logger.info("Attempting to stop Spark session...")
        spark.stop()
        logger.info("Spark session stopped.")