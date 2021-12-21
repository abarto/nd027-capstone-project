"""
Redshift/PostgreSQL related functions for "Project: Capstone Project"
"""
import psycopg2

def get_connection_string(config):
    """Creates a psycopg2 connection string from the config"""
    return f"postgresql://{config['CLUSTER']['DB_USER']}:{config['CLUSTER']['DB_PASSWORD']}@{config['CLUSTER']['HOST']}:{config['CLUSTER']['DB_PORT']}/{config['CLUSTER']['DB_NAME']}"


def get_connection(config):
    """Create a psycopg2 connection"""
    connection = psycopg2.connect(get_connection_string(config))
    return connection
