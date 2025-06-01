import random
import time
from datetime import datetime
import psycopg2
import simplejson as json
from confluent_kafka import Consumer, KafkaException, KafkaError, SerializingProducer
import os
import sys
from dotenv import load_dotenv

# --- Load Environment Variables ---
load_dotenv()

# --- Configuration ---
try:
    DB_HOST = os.environ['DB_HOST']
    DB_NAME = os.environ['DB_NAME']
    DB_USER = os.environ['DB_USER']
    DB_PASSWORD = os.environ['DB_PASSWORD']
except KeyError as e:
    print(f"CRITICAL ERROR: Required environment variable {e} not set.")
    print(f"Please ensure '{e.strerror if hasattr(e, 'strerror') else e}' is defined in your .env file or system environment.")
    sys.exit(1)

KAFKA_BOOTSTRAP_SERVERS = os.environ.get('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
VOTERS_TOPIC = 'voters_topic'
VOTES_TOPIC = 'votes_topic' # New topic for produced vote events

# --- Logging (can be more sophisticated if needed) ---
import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- Kafka Delivery Report Callback (can be imported if main.py is a module) ---
def delivery_report(err, msg):
    if err is not None:
        logging.error(f'Message delivery failed for topic {msg.topic()}: {err}')
    else:
        logging.info(f'Message delivered to {msg.topic()} [{msg.partition()}] @ {msg.offset()}')

# --- Main Execution Block ---
if __name__ == "__main__":
    consumer_conf = {
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'group.id': 'voting-group-processor', # Unique group ID for this consumer
        'auto.offset.reset': 'earliest',      # Start from the beginning if no offset
        'enable.auto.commit': False           # We will commit offsets manually
    }
    producer_conf = {'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS}

    consumer = Consumer(consumer_conf)
    producer = SerializingProducer(producer_conf)

    conn = None  # Initialize conn to None for the finally block

    try:
        conn = psycopg2.connect(host=DB_HOST, dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD)
        logging.info(f"Successfully connected to database: {DB_NAME}")

        # Fetch candidate data once at the start
        with conn.cursor() as cur:
            cur.execute("SELECT candidate_id, candidate_name, party_affiliation FROM candidates")
            candidate_rows = cur.fetchall()
            if not candidate_rows:
                logging.error("No candidates found in the database. Exiting.")
                sys.exit(1)

            # Store candidates in a more usable format, e.g., a list of dicts
            candidates = [
                {"candidate_id": row[0], "candidate_name": row[1], "party_affiliation": row[2]}
                for row in candidate_rows
            ]
            logging.info(f"Loaded {len(candidates)} candidates from the database.")

        consumer.subscribe([VOTERS_TOPIC])
        logging.info(f"Subscribed to Kafka topic: {VOTERS_TOPIC}")

        while True:
            msg = consumer.poll(timeout=1.0)  # Poll for new messages

            if msg is None:
                continue  # No message received, continue polling
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # End of partition event, not an error
                    logging.info(f"Reached end of partition for {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")
                    continue
                else:
                    logging.error(f"Kafka consumer error: {msg.error()}")
                    break  # Fatal error, break the loop

            # Successfully received a message
            try:
                voter = json.loads(msg.value().decode('utf-8'))
                logging.info(f"Received voter: {voter.get('voter_id')} - {voter.get('voter_name')}")

                # Simulate a vote
                chosen_candidate = random.choice(candidates)

                vote_data = {
                    "voter_id": voter.get('voter_id'),
                    "voter_name": voter.get('voter_name'), # For context in the vote event
                    "candidate_id": chosen_candidate.get('candidate_id'),
                    "candidate_name": chosen_candidate.get('candidate_name'), # For context
                    "party_affiliation": chosen_candidate.get('party_affiliation'), # For context
                    "voting_time": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
                    # "vote" field can be implicit by the event itself, or can add a count
                }

                logging.info(f"Voter {vote_data['voter_id']} is voting for candidate: {vote_data['candidate_id']} ({vote_data['candidate_name']})")

                # Start a new transaction for this message's DB operations
                with conn.cursor() as cur:
                    cur.execute("""
                                INSERT INTO votes (voter_id, candidate_id, voting_time)
                                VALUES (%s, %s, %s)
                                    ON CONFLICT (voter_id) DO NOTHING; -- Assuming one vote per voter, requires UNIQUE constraint on voter_id
                                """, (vote_data['voter_id'], vote_data['candidate_id'], vote_data['voting_time']))

                # Produce vote event to Kafka
                producer.produce(
                    VOTES_TOPIC, # Use the new VOTES_TOPIC
                    key=str(vote_data["voter_id"]),
                    value=json.dumps(vote_data),
                    on_delivery=delivery_report
                )
                producer.flush(timeout=5) # Block until message is delivered or timeout

                # If both DB insert and Kafka produce were successful for this message
                conn.commit()  # Commit database changes for this specific vote
                consumer.commit(message=msg) # Commit Kafka offset for this specific message
                logging.info(f"Successfully processed and committed vote for voter {vote_data['voter_id']}.")

            except psycopg2.Error as db_e:
                if conn: # conn might be None if initial connection failed
                    conn.rollback() # Rollback DB changes for this specific vote
                logging.error(f"DB Error processing vote for voter {voter.get('voter_id', 'N/A')}: {db_e}. Vote rolled back. Message will be reprocessed if script restarts or another consumer takes over.")
                # Do not commit Kafka offset, so message will be re-delivered
            except KafkaException as kafka_e: # For producer errors
                if conn:
                    conn.rollback() # Rollback DB changes as Kafka produce failed for this vote
                logging.error(f"Kafka Produce Error for vote from voter {voter.get('voter_id', 'N/A')}: {kafka_e}. Vote rolled back. Message will be reprocessed if script restarts or another consumer takes over.")
                # Do not commit Kafka offset
            except Exception as e:
                if conn:
                    conn.rollback() # General rollback for other errors
                logging.error(f"General Error processing vote for voter {voter.get('voter_id', 'N/A')}: {e}. Vote rolled back. Message will be reprocessed if script restarts or another consumer takes over.", exc_info=True)
                # Do not commit Kafka offset

            time.sleep(0.2) # Small delay between processing messages

    except KeyboardInterrupt:
        logging.info("Process interrupted by user (Ctrl+C). Shutting down...")
    except Exception as e:
        logging.error(f"An uncaught exception occurred: {e}", exc_info=True)
    finally:
        logging.info("Closing Kafka consumer.")
        if 'consumer' in locals() and consumer: # Check if consumer was defined
            consumer.close()

        logging.info("Flushing any outstanding Kafka producer messages...")
        if 'producer' in locals() and producer: # Check if producer was defined
            producer.flush(timeout=10)

        if conn:
            logging.info("Closing database connection.")
            conn.close()
        logging.info("Voting script finished.")