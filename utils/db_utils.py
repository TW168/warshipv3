import configparser
from sqlalchemy import create_engine
import logging
from datetime import datetime

# Configure logging
logging.basicConfig(filename='database_connection.log', level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

def connect_to_database(section='DB', dbms='mysql'):
    """
    Connect to a database using connection information from a config file.

    Parameters:
        section (str): The name of the section in the config file that contains the connection information.
        dbms (str): The name of the database management system (DBMS) to use (default is 'mysql').

    Returns:
        sqlalchemy.engine.Connection: A connection object that can be used to execute SQL queries.

    Raises:
        ValueError: If the specified section is not found in the config file.
    """
    config = configparser.ConfigParser()
    config.read("D:\\dev\\shipping\\shipping\\config.ini")
    
    if section not in config.sections():
        error_message = f"Section {section} not found in config.ini"
        logging.error(error_message)
        raise ValueError(error_message)

    # Get the connection information from the specified section
    user = config[section]["user"]
    password = config[section]["password"]
    host = config[section]["host"]
    port = int(config[section]["port"])
    database = config[section]["database"]

    # Create a connection string using sqlalchemy and the specified DBMS
    connection_string = f"{dbms}+mysqlconnector://{user}:{password}@{host}:{port}/{database}"
    # Create an engine object with the connection string
    engine = create_engine(connection_string)
    
    try:
        # Test the connection by executing a simple query
        conn = engine.connect()
        logging.info(f"Successfully connected to {dbms.upper()} database: {database}")
    except Exception as e:
        logging.error(f"Failed to connect to the database. Error: {e}")
        raise

    return conn
