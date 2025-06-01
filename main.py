import random
import psycopg2
import requests
import simplejson as json
from confluent_kafka import SerializingProducer, KafkaException
import logging
import os
import sys
import time
from dotenv import load_dotenv

# --- Load Environment Variables ---
load_dotenv()
# Ensure required environment variables are set
try:
    DB_HOST = os.environ['DB_HOST']
    DB_NAME = os.environ['DB_NAME']
    DB_USER = os.environ['DB_USER']
    DB_PASSWORD = os.environ['DB_PASSWORD']
except KeyError as e:
    logging.error(
        f"CRITICAL ERROR: Required environment variable {e} not set. Please define it in your .env file or system environment.")
    sys.exit(1)

DEFAULT_KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'
DEFAULT_RANDOMUSER_API_URL = 'https://randomuser.me/api/?nat=gb'
KAFKA_BOOTSTRAP_SERVERS = os.environ.get('KAFKA_BOOTSTRAP_SERVERS', DEFAULT_KAFKA_BOOTSTRAP_SERVERS)
BASE_URL = os.environ.get('RANDOMUSER_API_URL', DEFAULT_RANDOMUSER_API_URL)
PARTIES = ["Management Party", "Savior Party", "Tech Republic Party"]
VOTERS_TOPIC = 'voters_topic'
# CANDIDATES_TOPIC = 'candidates_topic' # Still defined but not actively used for producing

# --- Logging Setup ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
random.seed(42)


# --- generate_voter_data (ensure it checks for empty results) ---
def generate_voter_data():
    try:
        response = requests.get(BASE_URL)
        response.raise_for_status()
        data = response.json()
        if data.get('results'):
            user_data = data['results'][0]
            return {
                "voter_id": user_data['login']['uuid'],
                "voter_name": f"{user_data['name']['first']} {user_data['name']['last']}",
                "date_of_birth": user_data['dob']['date'],
                "gender": user_data['gender'],
                "nationality": user_data['nat'],
                "registration_number": user_data['login']['username'],
                "address": {
                    "street": f"{user_data['location']['street']['number']} {user_data['location']['street']['name']}",
                    "city": user_data['location']['city'],
                    "state": user_data['location']['state'],
                    "country": user_data['location']['country'],
                    "postcode": str(user_data['location']['postcode'])
                },
                "email": user_data['email'],
                "phone_number": user_data['phone'],
                "cell_number": user_data['cell'],
                "picture": user_data['picture']['large'],
                "registered_age": user_data['registered']['age']
            }
        else:
            logging.warning(f"API returned no results for voter data. Response: {data}")
            return None
    except requests.exceptions.RequestException as e:
        logging.error(f"API request failed for voter data: {e}")
        return None
    except (KeyError, IndexError, TypeError) as e:
        logging.error(
            f"Error parsing voter data from API response: {e}. Response: {response.text if 'response' in locals() else 'N/A'}")
        return None


# --- generate_candidate_data (ensure it checks for empty results) ---
def generate_candidate_data(candidate_number, total_parties):
    gender_param = 'female' if candidate_number % 2 == 1 else 'male'
    try:
        url_to_fetch = f"{BASE_URL}{'&' if '?' in BASE_URL else '?'}gender={gender_param}"
        response = requests.get(url_to_fetch)
        response.raise_for_status()
        data = response.json()
        if data.get('results'):
            user_data = data['results'][0]
            return {
                "candidate_id": user_data['login']['uuid'],
                "candidate_name": f"{user_data['name']['first']} {user_data['name']['last']}",
                "party_affiliation": PARTIES[candidate_number % total_parties],
                "biography": "A brief bio of the candidate.",
                "campaign_platform": "Key campaign promises.",
                "photo_url": user_data['picture']['large']
            }
        else:
            logging.warning(f"API returned no results for candidate data. Response: {data}")
            return None
    except requests.exceptions.RequestException as e:
        logging.error(f"API request failed for candidate data: {e}")
        return None
    except (KeyError, IndexError, TypeError) as e:
        logging.error(
            f"Error parsing candidate data from API response: {e}. Response: {response.text if 'response' in locals() else 'N/A'}")
        return None


# --- delivery_report, create_tables, insert_voter_data, insert_candidate_data (remain mostly the same) ---
def delivery_report(err, msg):
    if err is not None:
        logging.error(f'Message delivery failed: {err}')
    else:
        logging.info(
            f'Message delivered to {msg.topic()} [{msg.partition()}] - Key: {msg.key().decode("utf-8") if msg.key() else "N/A"}')


def create_tables(cur):
    """Creates database tables if they don't already exist."""
    # Note: VARCHAR length for postcode might need adjustment based on data
    cur.execute("""
        CREATE TABLE IF NOT EXISTS candidates (
            candidate_id VARCHAR(255) PRIMARY KEY,
            candidate_name VARCHAR(255),
            party_affiliation VARCHAR(255),
            biography TEXT,
            campaign_platform TEXT,
            photo_url TEXT
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS voters (
            voter_id VARCHAR(255) PRIMARY KEY,
            voter_name VARCHAR(255),
            date_of_birth VARCHAR(255),
            gender VARCHAR(255),
            nationality VARCHAR(255),
            registration_number VARCHAR(255),
            address_street VARCHAR(255),
            address_city VARCHAR(255),
            address_state VARCHAR(255),
            address_country VARCHAR(255),
            address_postcode VARCHAR(255),
            email VARCHAR(255),
            phone_number VARCHAR(255),
            cell_number VARCHAR(255),
            picture TEXT,
            registered_age INTEGER
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS votes (
            vote_id SERIAL PRIMARY KEY,
            voter_id VARCHAR(255) UNIQUE NOT NULL,
            candidate_id VARCHAR(255),
            voting_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (voter_id) REFERENCES voters(voter_id) ON DELETE CASCADE,
            FOREIGN KEY (candidate_id) REFERENCES candidates(candidate_id) ON DELETE CASCADE
        )
    """)
    logging.info("Database tables ensured to exist.")



def insert_voter_data(cur, voter):
    cur.execute("""
                INSERT INTO voters (voter_id, voter_name, date_of_birth, gender, nationality, registration_number,
                                    address_street, address_city, address_state, address_country, address_postcode,
                                    email, phone_number, cell_number, picture, registered_age)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s) ON CONFLICT (voter_id) DO NOTHING;""",
                (voter["voter_id"], voter['voter_name'], voter['date_of_birth'], voter['gender'], voter['nationality'],
                 voter['registration_number'], voter['address']['street'], voter['address']['city'],
                 voter['address']['state'], voter['address']['country'], str(voter['address']['postcode']),
                 voter['email'], voter['phone_number'], voter['cell_number'], voter['picture'],
                 voter['registered_age']))


def insert_candidate_data(cur, candidate):
    cur.execute("""
                INSERT INTO candidates (candidate_id, candidate_name, party_affiliation, biography, campaign_platform,
                                        photo_url)
                VALUES (%s, %s, %s, %s, %s, %s) ON CONFLICT (candidate_id) DO NOTHING;""",
                (candidate['candidate_id'], candidate['candidate_name'], candidate['party_affiliation'],
                 candidate['biography'], candidate['campaign_platform'], candidate['photo_url']))

# --- Main Execution Block ---
if __name__ == "__main__":
    logging.info(f"Script starting. Connecting to DB: {DB_HOST}/{DB_NAME} as {DB_USER}")
    logging.info(f"Kafka brokers: {KAFKA_BOOTSTRAP_SERVERS}")
    logging.info(f"Using API Base URL: {BASE_URL}")

    producer_conf = {'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS}
    producer = None # Initialize to None for the finally block
    conn = None     # Initialize to None for the finally block

    try:
        producer = SerializingProducer(producer_conf)
    except Exception as e:
        logging.error(f"Failed to create Kafka producer: {e}")
        sys.exit(1)

    try:
        conn = psycopg2.connect(host=DB_HOST, dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD)
        with conn.cursor() as cur:
            create_tables(cur) # Ensures voters, candidates, and votes tables exist

            # --- Candidate Generation/Verification ---
            # This part still ensures candidates are in the DB for voting.py to use.
            cur.execute("SELECT candidate_id FROM candidates")
            existing_candidate_ids_rows = cur.fetchall()
            # We are not using candidate_ids_in_db directly in main.py for voting anymore,
            # but it's good to know how many exist.
            num_existing_candidates = len(existing_candidate_ids_rows)
            num_parties = len(PARTIES)

            if num_existing_candidates < num_parties:
                logging.info(f"Found {num_existing_candidates} candidates. Generating up to {num_parties} total.")
                # Temp set to keep track of candidates added in this run to avoid re-querying DB in loop
                current_run_candidate_ids = {row[0] for row in existing_candidate_ids_rows}
                for i in range(num_parties):
                    if len(current_run_candidate_ids) >= num_parties:
                        break # Already have enough distinct candidates

                    candidate_data = generate_candidate_data(i, num_parties)
                    time.sleep(0.2) # Small delay for candidate API calls
                    if candidate_data:
                        if candidate_data['candidate_id'] not in current_run_candidate_ids:
                            insert_candidate_data(cur, candidate_data) # Defined above
                            logging.info(f"Inserted candidate: {candidate_data['candidate_name']} ({candidate_data['party_affiliation']})")
                            current_run_candidate_ids.add(candidate_data['candidate_id'])
                    else:
                        logging.warning(f"Could not generate data for candidate slot {i + 1}.")
            else:
                logging.info(f"Sufficient candidates ({num_existing_candidates}) already exist in the database for {num_parties} parties.")

            conn.commit() # Commit candidate insertions if any

            # --- Voter Generation and Kafka Production ---
            num_voters_to_generate = 100  # Number of voters to generate in this run
            logging.info(f"Generating {num_voters_to_generate} voters...")
            voters_processed_successfully = 0
            for i in range(num_voters_to_generate):
                voter_data = generate_voter_data() # Defined above
                if voter_data:
                    try:
                        # Start a new transaction for this voter
                        with conn.cursor() as voter_cur: # Use a new cursor for per-voter transaction
                            insert_voter_data(voter_cur, voter_data) # Defined above

                        producer.produce(
                            VOTERS_TOPIC, # Defined above
                            key=str(voter_data["voter_id"]),
                            value=json.dumps(voter_data), # json is imported simplejson
                            on_delivery=delivery_report # Defined above
                        )
                        # No direct vote insertion in main.py anymore
                        conn.commit() # Commit after successful DB insert and Kafka produce initiated
                        voters_processed_successfully += 1
                        logging.info(f"Processed voter {voters_processed_successfully}/{num_voters_to_generate}: {voter_data['voter_id']}")

                    except psycopg2.Error as db_err:
                        logging.error(f"Database error inserting voter {voter_data.get('voter_id', 'N/A')}: {db_err}")
                        conn.rollback() # Rollback this specific voter's transaction
                    except KafkaException as kafka_err: # Catch Kafka errors during produce
                        logging.error(f"Kafka error producing voter {voter_data.get('voter_id', 'N/A')}: {kafka_err}")
                        # Decide if to rollback the DB insert if Kafka fails.
                        # For now, we've committed before this specific catch, but for a single voter,
                        # the previous transaction could be rolled back.
                        # A more robust solution might produce to Kafka first or use an outbox pattern.
                        # For simplicity here, if Kafka fails, the voter is in DB but not on Kafka topic for this attempt.
                        # The main 'try...except Exception' below would catch broader producer issues.
                    except Exception as e:
                        logging.error(f"Error processing voter {voter_data.get('voter_id', 'N/A')}: {e}")
                        conn.rollback() # Rollback this specific voter's transaction
                else:
                    logging.warning(f"Skipping voter generation for iteration {i + 1} due to data API error.")

                # Poll for Kafka delivery reports periodically
                if i % 20 == 0 or i == num_voters_to_generate - 1 :
                    if producer:
                        producer.poll(0.1) # Timeout in seconds

                time.sleep(1) # Sleep between voter API calls

            logging.info(f"Successfully processed and initiated Kafka production for {voters_processed_successfully}/{num_voters_to_generate} voters.")

    except psycopg2.OperationalError as e:
        logging.error(f"DATABASE CONNECTION FAILED: Could not connect to PostgreSQL.")
        logging.error(f"Connection details: host={DB_HOST}, dbname={DB_NAME}, user={DB_USER}")
        logging.error(f"Error details: {e}")
        logging.error("Please check your database server, network connection, and .env file credentials.")
    except psycopg2.Error as e: # Catch other general psycopg2 errors
        logging.error(f"A general database error occurred: {e}")
        if conn:
            conn.rollback() # Rollback any pending transaction
    except KafkaException as e: # Catch Kafka producer instantiation or critical errors
        logging.error(f"A critical Kafka error occurred: {e}")
    except Exception as e:
        logging.error(f"An unexpected error occurred in the main execution block: {e}", exc_info=True)
        if conn:
            conn.rollback() # Rollback any pending transaction
    finally:
        logging.info("Flushing final Kafka messages (if any)...")
        if producer:
            try:
                producer.flush(timeout=10)  # Wait up to 10 seconds
                logging.info("Kafka messages flushed.")
            except KafkaException as e: # Catch Kafka errors during flush
                logging.error(f"Kafka error during flush: {e}")
            except Exception as e: # Catch other errors during flush
                logging.error(f"Error flushing Kafka messages: {e}")

        if conn:
            logging.info("Closing database connection.")
            conn.close()
        logging.info("Script finished.")