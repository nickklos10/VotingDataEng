import random
import psycopg2
import requests
import simplejson as json
from confluent_kafka import SerializingProducer
import logging
import os  # For potential environment variable usage

# --- Configuration ---
# Consider moving these to environment variables or a configuration file for better practice
BASE_URL = os.environ.get('RANDOMUSER_API_URL', 'https://randomuser.me/api/?nat=gb')
PARTIES = ["Management Party", "Savior Party", "Tech Republic Party"]
KAFKA_BOOTSTRAP_SERVERS = os.environ.get('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
DB_HOST = os.environ.get('DB_HOST', 'localhost')
DB_NAME = os.environ.get('DB_NAME', 'voting')
DB_USER = os.environ.get('DB_USER', 'postgres')
DB_PASSWORD = os.environ.get('DB_PASSWORD', 'postgres')

VOTERS_TOPIC = 'voters_topic'
CANDIDATES_TOPIC = 'candidates_topic'  # Defined but not used for producing in this script

# --- Logging Setup ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

random.seed(42)


def generate_voter_data():
    """Fetches and formats voter data from the randomuser.me API."""
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
                "postcode": user_data['location']['postcode']
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
        logging.error(f"Error parsing voter data from API response: {e}")
        return None


def generate_candidate_data(candidate_number, total_parties):
    """Fetches and formats candidate data from the randomuser.me API."""
    gender_param = 'female' if candidate_number % 2 == 1 else 'male'
    try:
        response = requests.get(f"{BASE_URL}&gender={gender_param}")
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
        logging.error(f"Error parsing candidate data from API response: {e}")
        return None


def delivery_report(err, msg):
    """Callback for Kafka message delivery reports."""
    if err is not None:
        logging.error(f'Message delivery failed: {err}')
    else:
        logging.info(
            f'Message delivered to {msg.topic()} [{msg.partition()}] - Key: {msg.key().decode("utf-8") if msg.key() else "N/A"}')


def create_tables(cur):
    """Creates database tables if they don't already exist."""
    cur.execute("""
                CREATE TABLE IF NOT EXISTS candidates (
                    candidate_id VARCHAR(255) PRIMARY KEY,
                    candidate_name VARCHAR(255),
                    party_affiliation VARCHAR
                (
                    255
                ),
                    biography TEXT,
                    campaign_platform TEXT,
                    photo_url TEXT
                    )
                """)
    cur.execute("""
                CREATE TABLE IF NOT EXISTS voters
                (
                    voter_id
                    VARCHAR
                (
                    255
                ) PRIMARY KEY,
                    voter_name VARCHAR
                (
                    255
                ),
                    date_of_birth VARCHAR
                (
                    255
                ),
                    gender VARCHAR
                (
                    255
                ),
                    nationality VARCHAR
                (
                    255
                ),
                    registration_number VARCHAR
                (
                    255
                ),
                    address_street VARCHAR
                (
                    255
                ),
                    address_city VARCHAR
                (
                    255
                ),
                    address_state VARCHAR
                (
                    255
                ),
                    address_country VARCHAR
                (
                    255
                ),
                    address_postcode VARCHAR
                (
                    255
                ),
                    email VARCHAR
                (
                    255
                ),
                    phone_number VARCHAR
                (
                    255
                ),
                    cell_number VARCHAR
                (
                    255
                ),
                    picture TEXT,
                    registered_age INTEGER
                    )
                """)
    cur.execute("""
                CREATE TABLE IF NOT EXISTS votes
                (
                    voter_id
                    VARCHAR
                (
                    255
                ) UNIQUE,
                    candidate_id VARCHAR
                (
                    255
                ),
                    voting_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    vote INT DEFAULT 1,
                    PRIMARY KEY
                (
                    voter_id,
                    candidate_id
                ),
                    FOREIGN KEY
                (
                    voter_id
                ) REFERENCES voters
                (
                    voter_id
                ),
                    FOREIGN KEY
                (
                    candidate_id
                ) REFERENCES candidates
                (
                    candidate_id
                )
                    )
                """)
    logging.info("Database tables ensured to exist.")


def insert_voter_data(cur, voter):
    """Inserts a single voter's data into the voters table."""
    # For inserting many voters, consider using cursor.executemany() for better performance.
    cur.execute("""
                INSERT INTO voters (voter_id, voter_name, date_of_birth, gender, nationality,
                                    registration_number, address_street, address_city, address_state,
                                    address_country, address_postcode, email, phone_number,
                                    cell_number, picture, registered_age)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s) ON CONFLICT (voter_id) DO NOTHING; -- Optional: handle conflicts, e.g., ignore or update
                """,
                (
                    voter["voter_id"], voter['voter_name'], voter['date_of_birth'], voter['gender'],
                    voter['nationality'], voter['registration_number'], voter['address']['street'],
                    voter['address']['city'], voter['address']['state'], voter['address']['country'],
                    voter['address']['postcode'], voter['email'], voter['phone_number'],
                    voter['cell_number'], voter['picture'], voter['registered_age']
                )
                )


def insert_candidate_data(cur, candidate):
    """Inserts a single candidate's data into the candidates table."""
    cur.execute("""
                INSERT INTO candidates (candidate_id, candidate_name, party_affiliation,
                                        biography, campaign_platform, photo_url)
                VALUES (%s, %s, %s, %s, %s, %s) ON CONFLICT (candidate_id) DO NOTHING; -- Optional: handle conflicts
                """,
                (
                    candidate['candidate_id'], candidate['candidate_name'], candidate['party_affiliation'],
                    candidate['biography'], candidate['campaign_platform'], candidate['photo_url']
                )
                )


if __name__ == "__main__":
    producer_conf = {'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS}
    producer = SerializingProducer(producer_conf)

    try:
        # Using 'with' ensures the connection and cursor are closed automatically
        # and transactions are committed (or rolled back on error).
        with psycopg2.connect(host=DB_HOST, dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD) as conn:
            with conn.cursor() as cur:
                create_tables(cur)

                # --- Candidate Generation ---
                cur.execute("SELECT candidate_id FROM candidates")
                existing_candidates = cur.fetchall()
                num_candidates_to_generate = len(PARTIES)

                if len(existing_candidates) < num_candidates_to_generate:
                    logging.info(
                        f"Found {len(existing_candidates)} candidates. Generating {num_candidates_to_generate - len(existing_candidates)} more.")
                    for i in range(num_candidates_to_generate):
                        # Simple check: if we assume candidate IDs are unique and we need N candidates for N parties
                        # This logic might need refinement for more complex scenarios (e.g. specific candidate check)
                        candidate = generate_candidate_data(i, num_candidates_to_generate)
                        if candidate:
                            insert_candidate_data(cur, candidate)
                            logging.info(
                                f"Inserted candidate: {candidate['candidate_name']} ({candidate['party_affiliation']})")
                            # Optionally produce candidate data to Kafka if needed
                            # producer.produce(CANDIDATES_TOPIC, key=candidate['candidate_id'], value=json.dumps(candidate), on_delivery=delivery_report)
                        else:
                            logging.warning(f"Could not generate data for candidate {i + 1}.")
                else:
                    logging.info(f"Sufficient candidates ({len(existing_candidates)}) already exist in the database.")

                # --- Voter Generation and Kafka Production ---
                num_voters = 100  # Number of voters to generate
                logging.info(f"Generating {num_voters} voters...")
                for i in range(num_voters):
                    voter_data = generate_voter_data()
                    if voter_data:
                        insert_voter_data(cur, voter_data)

                        producer.produce(
                            VOTERS_TOPIC,
                            key=str(voter_data["voter_id"]),  # Ensure key is string
                            value=json.dumps(voter_data),  # Ensure value is serializable (simplejson handles this)
                            on_delivery=delivery_report
                        )
                        # producer.poll(0) # Call poll() at regular intervals or before flush
                        logging.info(f'Processed voter {i + 1}/{num_voters}: {voter_data["voter_id"]}')
                    else:
                        logging.warning(f"Skipping voter {i + 1} due to data generation error.")

                    if i % 100 == 0:  # Poll every 100 messages, or choose a different interval
                        producer.poll(0.1)  # timeout in seconds

    except psycopg2.Error as e:
        logging.error(f"Database connection or operation error: {e}")
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}", exc_info=True)
    finally:
        logging.info("Flushing final Kafka messages...")
        if 'producer' in locals() and producer:
            producer.flush(timeout=10)  # Wait up to 10 seconds for messages to be delivered
        logging.info("Script finished.")