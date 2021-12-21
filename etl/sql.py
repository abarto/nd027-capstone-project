"""
SQL related functions for "Project: Capstone Project"
"""
from string import Template


dim_airport_table_drop = 'DROP TABLE IF EXISTS "dimAirport"'
dim_country_table_drop = 'DROP TABLE IF EXISTS "dimDate"'
dim_date_table_drop = 'DROP TABLE IF EXISTS "dimDate"'
fact_ingress_table_drop = 'DROP TABLE IF EXISTS "factIngress"'


dim_airport_table_truncate = 'TRUNCATE TABLE "dimAirport"'
dim_country_table_truncate = 'TRUNCATE TABLE "dimCountry"'
dim_date_table_truncate = 'TRUNCATE TABLE "dimDate"'


dim_airport_table_create= """\
CREATE TABLE IF NOT EXISTS "dimAirport" (
    airport_id CHAR(4) PRIMARY KEY,
    name VARCHAR(128),
    municipality VARCHAR(64) NOT NULL,
    state CHAR(2) NOT NULL
)
DISTSTYLE ALL
"""

dim_country_table_create= """\
CREATE TABLE IF NOT EXISTS "dimCountry" (
    country_id CHAR(3) PRIMARY KEY,
    name VARCHAR(128),
    languages VARCHAR(128) NOT NULL,
    gdp_per_capita FLOAT
)
DISTSTYLE ALL
"""

dim_date_table_create = """\
CREATE TABLE IF NOT EXISTS "dimDate" (
    date_id INTEGER PRIMARY KEY,
    year INTEGER NOT NULL,
    month INTEGER NOT NULL,
    day INTEGER NOT NULL,
    day_of_week INTEGER NOT NULL,
    week_of_year INTEGER NOT NULL
)
DISTSTYLE ALL
"""

fact_ingress_table_create = """
CREATE TABLE IF NOT EXISTS "factIngress" (
    ingress_id CHAR(36) PRIMARY KEY,
    date_id INTEGER NOT NULL SORTKEY DISTKEY REFERENCES "dimDate",
    country_id CHAR(3) NOT NULL REFERENCES "dimCountry",
    airport_id CHAR(4) NOT NULL REFERENCES "dimAirport",
    gender CHAR(1) NOT NULL,
    age_bucket CHAR(2) NOT NULL,
    temperature_bucket CHAR(2) NOT NULL
)
DISTSTYLE KEY
"""

table_copy_sql_template = Template("""\
    COPY "${table}"
    FROM '${uri}'
    CREDENTIALS 'aws_iam_role=${iam_role}'
    FORMAT AS PARQUET
    COMPUPDATE OFF;
""")
