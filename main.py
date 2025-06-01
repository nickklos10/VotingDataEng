# main.py

import random
import psycopg2
import requests
import simplejson as json
from confluent_kafka import SerializingProducer
import logging
import os
import sys  # For sys.exit()
from dotenv import load_dotenv

# --- Load Environment Variables ---
load_dotenv()

# --- Configuration ---
# Attempt to get critical configurations; exit if not found.
try:
    DB_HOST = os.environ['DB_HOST']
    DB_NAME = os.environ['DB_NAME']
    DB_USER = os.environ['DB_USER']
    DB_PASSWORD = os.environ['DB_PASSWORD']
except KeyError as e:
    print(f"CRITICAL ERROR: Required environment variable {e} not set.")
    print(f"Please ensure '{e.strerror if hasattr(e, 'strerror') else e}' is defined in your .env file or system environment.")
    print("The .env file should be in the same directory as the script and NOT committed to Git.")
    sys.exit(1)  # Exit the script with an error code

# For other configurations, we can use .get() if defaults are acceptable
# or if they are less critical to be missing from the .env file.
DEFAULT_KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'
DEFAULT_RANDOMUSER_API_URL = 'https://randomuser.me/api/?nat=gb'

KAFKA_BOOTSTRAP_SERVERS = os.environ.get('KAFKA_BOOTSTRAP_SERVERS', DEFAULT_KAFKA_BOOTSTRAP_SERVERS)
BASE_URL = os.environ.get('RANDOMUSER_API_URL', DEFAULT_RANDOMUSER_API_URL) # Script uses BASE_URL

# Application-specific constants (can also be moved to .env if they change per environment)
PARTIES = ["Management Party", "Savior Party", "Tech Republic Party"]
VOTERS_TOPIC = 'voters_topic'
CANDIDATES_TOPIC = 'candidates_topic' # Defined but not used for producing in this script

# --- Logging Setup ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- Random Seed for Reproducibility ---
random.seed(42)


def generate_voter_data():
    """Fetches and formats voter data from the randomuser.me API (using BASE_URL)."""
    try:
        response = requests.get(BASE_URL)
        response.raise_for_status()  # Raises an HTTPError for bad responses (4XX or 5XX)
        user_data = response.json()['results'][0]
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
                "postcode": str(user_data['location']['postcode']) # Ensure postcode is string
            },
            "email": user_data['email'],
            "phone_number": user_data['phone'],
            "cell_number": user_data['cell'],
            "picture": user_data['picture']['large'],
            "registered_age": user_data['registered']['age']
        }
    except requests.exceptions.RequestException as e:
        logging.error(f"API request failed for voter data: {e}")
        return None
    except (KeyError, IndexError, TypeError) as e:
        logging.error(f"Error parsing voter data from API response: {e}. Response: {response.text if 'response' in locals() else 'N/A'}")
        return None


def generate_candidate_data(candidate_number, total_parties):
    """Fetches and formats candidate data from the randomuser.me API (using BASE_URL)."""
    gender_param = 'female' if candidate_number % 2 == 1 else 'male'
    try:
        # Append gender to BASE_URL correctly, checking if BASE_URL already has query params
        url_to_fetch = f"{BASE_URL}{'&' if '?' in BASE_URL else '?'}gender={gender_param}"
        response = requests.get(url_to_fetch)
        response.raise_for_status()
        user_data = response.json()['results'][0]
        return {
            "candidate_id": user_data['login']['uuid'],
            "candidate_name": f"{user_data['name']['first']} {user_data['name']['last']}",
            "party_affiliation": PARTIES[candidate_number % total_parties],
            "biography": "A brief bio of the candidate, generated for demonstration.",
            "campaign_platform": "Key campaign promises or platform details for this candidate.",
            "photo_url": user_data['picture']['large']
        }
    except requests.exceptions.RequestException as e:
        logging.error(f"API request failed for candidate data: {e}")
        return None
    except (KeyError, IndexError, TypeError) as e:
        logging.error(f"Error parsing candidate data from API response: {e}. Response: {response.text if 'response' in locals() else 'N/A'}")
        return None


def delivery_report(err, msg):
    """Callback for Kafka message delivery reports."""
    if err is not None:
        logging.error(f'Message delivery failed: {err}')
    else:
        logging.info(f'Message delivered to {msg.topic()} [{msg.partition()}] - Key: {msg.key().decode("utf-8") if msg.key() else "N/A"}')


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
            voter_id VARCHAR(255),
            candidate_id VARCHAR(255),
            voting_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            vote INT DEFAULT 1,
            PRIMARY KEY (voter_id, candidate_id),
            FOREIGN KEY (voter_id) REFERENCES voters(voter_id) ON DELETE CASCADE,
            FOREIGN KEY (candidate_id) REFERENCES candidates(candidate_id) ON DELETE CASCADE
        )
    """)
    logging.info("Database tables ensured to exist.")


def insert_voter_data(cur, voter):
    """Inserts a single voter's data into the voters table."""
    cur.execute("""
        INSERT INTO voters (
            voter_id, voter_name, date_of_birth, gender, nationality,
            registration_number, address_street, address_city, address_state,
            address_country, address_postcode, email, phone_number,
            cell_number, picture, registered_age
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (voter_id) DO NOTHING;
        """,
        (
            voter["voter_id"], voter['voter_name'], voter['date_of_birth'], voter['gender'],
            voter['nationality'], voter['registration_number'], voter['address']['street'],
            voter['address']['city'], voter['address']['state'], voter['address']['country'],
            str(voter['address']['postcode']), voter['email'], voter['phone_number'], # Ensure postcode is string
            voter['cell_number'], voter['picture'], voter['registered_age']
        )
    )

def insert_candidate_data(cur, candidate):
    """Inserts a single candidate's data into the candidates table."""
    cur.execute("""
        INSERT INTO candidates (
            candidate_id, candidate_name, party_affiliation,
            biography, campaign_platform, photo_url
        )
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (candidate_id) DO NOTHING;
        """,
        (
            candidate['candidate_id'], candidate['candidate_name'], candidate['party_affiliation'],
            candidate['biography'], candidate['campaign_platform'], candidate['photo_url']
        )
    )

# --- Main Execution Block ---
if __name__ == "__main__":
    logging.info(f"Script starting. Connecting to DB: {DB_HOST}/{DB_NAME} as {DB_USER}")
    logging.info(f"Kafka brokers: {KAFKA_BOOTSTRAP_SERVERS}")
    logging.info(f"Using API Base URL: {BASE_URL}")

    producer_conf = {'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS}
    # Consider adding error handling for producer creation if Kafka is unavailable
    try:
        producer = SerializingProducer(producer_conf)
    except Exception as e:
        logging.error(f"Failed to create Kafka producer: {e}")
        sys.exit(1)


    try:
        # Using 'with' ensures the connection and cursor are closed automatically
        # and transactions are committed (or rolled back on error).
        with psycopg2.connect(host=DB_HOST, dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD) as conn:
            with conn.cursor() as cur:
                create_tables(cur)

                # --- Candidate Generation ---
                cur.execute("SELECT candidate_id FROM candidates")
                existing_candidates_rows = cur.fetchall()
                existing_candidate_ids = {row[0] for row in existing_candidates_rows}
                num_parties = len(PARTIES)

                if len(existing_candidate_ids) < num_parties:
                    logging.info(f"Found {len(existing_candidate_ids)} candidates. Generating up to {num_parties} total.")
                    # This simple loop generates one candidate per party index.
                    # More sophisticated logic might be needed if candidates can leave/join parties.
                    for i in range(num_parties):
                        candidate = generate_candidate_data(i, num_parties)
                        if candidate:
                            if candidate['candidate_id'] not in existing_candidate_ids:
                                insert_candidate_data(cur, candidate)
                                logging.info(f"Inserted candidate: {candidate['candidate_name']} ({candidate['party_affiliation']})")
                                existing_candidate_ids.add(candidate['candidate_id'])
                            else:
                                logging.info(f"Candidate {candidate['candidate_id']} already exists or was just generated by another run.")
                        else:
                            logging.warning(f"Could not generate data for candidate slot {i + 1}.")
                else:
                    logging.info(f"Sufficient unique candidates ({len(existing_candidate_ids)}) already exist in the database for {num_parties} parties.")

                # --- Voter Generation and Kafka Production ---
                num_voters_to_generate = 100  # Number of voters to generate in this run
                logging.info(f"Generating {num_voters_to_generate} voters...")
                voters_processed_successfully = 0
                for i in range(num_voters_to_generate):
                    voter_data = generate_voter_data()
                    if voter_data:
                        try:
                            insert_voter_data(cur, voter_data)
                            producer.produce(
                                VOTERS_TOPIC,
                                key=str(voter_data["voter_id"]),
                                value=json.dumps(voter_data),
                                on_delivery=delivery_report
                            )
                            voters_processed_successfully += 1
                        except psycopg2.Error as db_err:
                            logging.error(f"Database error inserting voter {voter_data['voter_id']}: {db_err}")
                            conn.rollback() # Rollback this specific voter insert if atomic operations are needed
                        except Exception as e: # Catch other errors during processing a single voter
                            logging.error(f"Error processing voter {voter_data['voter_id']}: {e}")
                    else:
                        logging.warning(f"Skipping voter generation for iteration {i + 1} due to data API error.")

                    # Poll for Kafka delivery reports periodically
                    if i % 20 == 0:  # Adjust frequency as needed
                        producer.poll(0.1) # Timeout in seconds

                logging.info(f"Successfully processed and produced {voters_processed_successfully}/{num_voters_to_generate} voters.")

    except psycopg2.OperationalError as e:
        logging.error(f"DATABASE CONNECTION FAILED: Could not connect to PostgreSQL.")
        logging.error(f"Connection details: host={DB_HOST}, dbname={DB_NAME}, user={DB_USER}")
        logging.error(f"Error details: {e}")
        logging.error("Please check your database server, network connection, and .env file credentials.")
    except psycopg2.Error as e:
        logging.error(f"A general database error occurred: {e}")
    except Exception as e:
        logging.error(f"An unexpected error occurred in the main execution block: {e}", exc_info=True)
    finally:
        logging.info("Flushing final Kafka messages (if any)...")
        if 'producer' in locals() and producer:
            try:
                producer.flush(timeout=10)  # Wait up to 10 seconds for messages to be delivered
                logging.info("Kafka messages flushed.")
            except Exception as e:
                logging.error(f"Error flushing Kafka messages: {e}")
        logging.info("Script finished.")