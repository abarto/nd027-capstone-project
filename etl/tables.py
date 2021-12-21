"""
Fact and dimension tables related functions for "Project: Capstone Project"
"""
import uuid

from datetime import datetime, timedelta
from pyspark.sql import functions as F

from .common import _simple_validation
from .sql import (
    dim_airport_table_create,
    dim_airport_table_drop,
    dim_airport_table_truncate,
    dim_country_table_create,
    dim_country_table_drop,
    dim_country_table_truncate,
    dim_date_table_create,
    dim_date_table_drop,
    dim_date_table_truncate,
    fact_ingress_table_create,
    fact_ingress_table_drop,
    table_copy_sql_template
)


def _get_dim_date_df(
    spark
):
    """
    Create a PySpark DataFrame to be loaded as dimDate
    
    | Column       | Type | PK | Description                                                                 |
    |--------------|------|:--:|-----------------------------------------------------------------------------|
    | date_id      | int  | Y  | An integer with format `YYYYMMDD` that identifies a record of the dimension |
    | year         | int  |    | Year                                                                        |
    | month        | int  |    | Month                                                                       |
    | day          | int  |    | Day of month                                                                |
    | day_of_week  | int  |    | An integer between 0 (Sunday) and 6 that represents the day of the week     |
    | week_of_year | int  |    | Week of the year                                                            |
    """
    end = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=30)
    start = end - timedelta(days=3660)

    dim_date_df = spark.sql(
        f"SELECT SEQUENCE({int(start.timestamp())}, {int(end.timestamp())}, 86400) AS date_timestamp")\
        .withColumn("date_timestamp", F.explode("date_timestamp"))\
        .select(F.to_date(F.col("date_timestamp").cast("timestamp")).alias("date"))\
        .withColumn("date_id", F.date_format(F.col("date"), "YYYYMMDD").cast("int"))\
        .withColumn("year", F.year(F.col("date")).cast("short"))\
        .withColumn("month", F.month(F.col("date")).cast("short"))\
        .withColumn("day", F.dayofmonth(F.col("date")).cast("short"))\
        .withColumn("day_of_week", F.dayofweek(F.col("date")).cast("short"))\
        .withColumn("week_of_year", F.weekofyear(F.col("date")).cast("short"))\
        .drop("date")\
        .orderBy("date_id")

    return dim_date_df


def generate_dim_date_parquet(
    spark,
    output_dir="parquet_files"
):
    """Generates dimCountry parquet files"""
    dim_date_df = _get_dim_date_df(spark)
    # We don't need to validate as this is a generated table
    dim_date_df.write.mode("overwrite").parquet(f"{output_dir}/dim_date")


def _get_dim_country_df(
    countries_df,
    languages_df,
    national_accounts_df
):
    """
    Create a PySpark DataFrame to be loaded as dimCountry
    
    | Column         | Type      | PK | Description                       |
    |----------------|-----------|----|-----------------------------------|
    | country_id     | char      | Y  | The ISO identifier of the country |
    | name           | varchar   |    | Name of the country               |
    | languages      | varchar   |    | A list of languages spoken        |
    | gdp_per_capita | double    |    | GDP per capita                    |
    """
    dim_country_df = countries_df.select(["alpha3", "name", "languages"])\
        .withColumnRenamed("alpha3", "country_id")\
        .withColumn("languages", F.explode(F.col("languages")))\
        .join(
            languages_df,
            F.col("languages") == languages_df.alpha3,
            "left"
        ).select(
            "country_id",
            countries_df.name,
            languages_df.name.alias("languages")
        ).groupBy("country_id").agg(
            F.first("name").alias("name"),
            F.collect_list("languages").alias("languages")
        ).join(
            national_accounts_df,
            F.lower(F.col("name")) == F.lower(national_accounts_df.country),
            "left"
        ).withColumn("gdp_per_capita", F.coalesce("gdp_per_capita", F.lit(0)))\
        .withColumn("languages", F.udf(lambda languages: ",".join(languages) if languages else "Unknown")(F.col("languages")))\
        .drop("country")

    return dim_country_df


def _validate_dim_country_df(df):
    """Performs simple validations on the dimCountry DataFrame"""
    
    _simple_validation(df, "dim_country_df", ["country_id", "name", "languages", "gdp_per_capita"], 100)


def generate_dim_country_parquet(
    countries_df,
    languages_df,
    national_accounts_df,
    output_dir="parquet_files"
):
    """Generates dimCountry parquet files"""
    dim_country_df = _get_dim_country_df(
        countries_df,
        languages_df,
        national_accounts_df
    )
    _validate_dim_country_df(dim_country_df)
    dim_country_df.write.mode("overwrite").parquet(f"{output_dir}/dim_country")


def _get_dim_airport_df(airports_df):
    """
    Create a PySpark DataFrame to be loaded as dimAirport

    | Column       | Type      | PK | Description                        |
    |--------------|-----------|:--:|------------------------------------|
    | airport_id   | char      | Y  | The 4 character airport identifier |
    | name         | varchar   |    | Name of the airport                |
    | municipality | varchar   |    | Municipality/city                  |
    | state        | char[2]   |    | State                              |
    """
    return airports_df


def _validate_dim_airport_df(df):
    """Performs simple validations on the dimAirport DataFrame"""
    
    _simple_validation(df, "dim_airport_df", ["airport_id", "name", "municipality", "state"], 100)


def generate_dim_airport_parquet(
    airports_df,
    output_dir="parquet_files"
):
    """Generates dimCountry parquet files"""
    dim_airport_df = _get_dim_airport_df(
        airports_df
    )
    _validate_dim_airport_df(dim_airport_df)
    dim_airport_df.write.mode("overwrite").parquet(f"{output_dir}/dim_airport")


def _get_fact_ingress_df(
    i94_data_df,
    airports_df,
    cbp_codes_df,
    countries_df,
    i94cntyl_df,
    temperatures_df,
    output_dir="parquet_files"
):
    """
    Create a PySpark DataFrame to be loaded as factIngress

    | Column             | Type     | PK | Description                                        |
    |--------------------|----------|:--:|----------------------------------------------------|
    | ingress_id         | char[36] | Y  | Generated identifier                               |
    | date_id            | int      |    | Date of ingress                                    |
    | year               | int      |    | Year of ingress (used for partitioning)            |
    | country_id         | char[2]  |    | Country of origin                                  |
    | airport_id         | char[4]  |    | Airport                                            |
    | gender             | char     |    | Gender.                                            |
    | age_bucket         | char[2]  |    | Age bucket.                                        |
    | temperature_bucket | char[2]  |    | Average weekly temperature bucket.                 |
    """
    fact_ingress_df = i94_data_df.join(
        cbp_codes_df,
        F.col("i94port") == cbp_codes_df.code
    ).join(
        airports_df,
        (cbp_codes_df.state == airports_df.state) & (cbp_codes_df.municipality == airports_df.municipality) 
    ).join(
        temperatures_df,
        (F.lower(cbp_codes_df.municipality) == F.lower(temperatures_df.city))
        & (F.weekofyear(F.col("arrdate")) == temperatures_df.week_of_year),
        "left"
    ).withColumn("temperature_bucket", F.coalesce(temperatures_df.temperature_bucket, F.lit("UN")))\
    .join(
        i94cntyl_df,
        F.col("i94cit") == i94cntyl_df.value
    ).join(
        countries_df,
        F.lower(i94cntyl_df.description) == F.lower(countries_df.name)
    ).withColumn("ingress_id", F.udf(lambda: str(uuid.uuid4()))())\
    .select(
        "ingress_id",
        "date_id",
        countries_df.alpha3.alias("country_id"),
        airports_df.airport_id.alias("airport_id"),
        "gender",
        "age_bucket",
        "temperature_bucket"
    )

    return fact_ingress_df


def _validate_fact_ingress_df(df):
    """Performs simple validations on the factIngress DataFrame"""
    
    _simple_validation(
        df,
        "fact_airport_df", [
            "ingress_id",
            "date_id",
            "country_id",
            "airport_id",
            "gender",
            "age_bucket",
            "temperature_bucket"
        ],
        1000000
    )


def generate_fact_ingress_parquet(
    i94_data_df,
    airports_df,
    cbp_codes_df,
    countries_df,
    i94cntyl_df,
    temperatures_df,
    output_dir="parquet_files"
):
    """Generates factIngress parquet files"""
    fact_ingress_df = _get_fact_ingress_df(
        i94_data_df,
        airports_df,
        cbp_codes_df,
        countries_df,
        i94cntyl_df,
        temperatures_df
    )
    _validate_fact_ingress_df(fact_ingress_df)
    fact_ingress_df.write.mode("overwrite").parquet(
        f"{output_dir}/fact_ingress")


def create_dim_fact_tables(connection):
    """Creates all the dimension and fact tables (if they don't exist)"""

    for query in (dim_airport_table_create, dim_country_table_create, dim_date_table_create, fact_ingress_table_create):
        with connection.cursor() as cursor:
            cursor.execute(query)
            connection.commit()


def truncate_dim_tables(connection):
    """Truncates all the dimension tables"""

    for query in (dim_airport_table_truncate, dim_country_table_truncate, dim_date_table_truncate):
        with connection.cursor() as cursor:
            cursor.execute(query)
            connection.commit()


def drop_dim_fact_tables(connection):
    """Drop all the dimension and fact tables"""

    for query in (dim_airport_table_drop, dim_country_table_drop, dim_date_table_drop, fact_ingress_table_drop):
        with connection.cursor() as cursor:
            cursor.execute(query)
            connection.commit()


def copy_table(config, connection, table, parquet_dir):
    sql = table_copy_sql_template.substitute(
        table=table,
        uri=f"s3://{config.get('S3', 'OUTPUT_BUCKET')}/{parquet_dir}",
        iam_role=config.get("IAM_ROLE", "ARN")
    )

    with connection.cursor() as cursor:
        cursor.execute(sql)
        connection.commit()


def load_dim_tables(config, connection):
    """Loads data into dim tables"""
    copy_table(config, connection, "dimAirport", "dim_airport")
    copy_table(config, connection, "dimCountry", "dim_country")
    copy_table(config, connection, "dimDate", "dim_date")


def validate_dim_tables(connection):
    """Performs validation on the dimension tables"""
    with connection.cursor() as cursor:
        # Count rows on dimAirport
        cursor.execute("""\
        SELECT COUNT(*) AS count
        FROM dimAirport
        """)
        
        if cursor.fetchone()[0] < 100:
            raise ValueError("Low record count on dimAirport")

        # Count rows on dimCountry
        cursor.execute("""\
        SELECT COUNT(*) AS count
        FROM dimCountry
        """)
        
        if cursor.fetchone()[0] < 100:
            raise ValueError("Low record count on dimCountry")

        # Count rows on dimDate
        cursor.execute("""\
        SELECT COUNT(*) AS count
        FROM dimDate
        """)
        
        if cursor.fetchone()[0] < 100:
            raise ValueError("Low record count on dimCountry")



def load_fact_tables(config, connection):
    """Loads data into fact tables"""
    copy_table(config, connection, "factIngress", "fact_ingress")


def validate_fact_tables(connection):
    """Performs validation on the fact tables"""
    with connection.cursor() as cursor:
        # Count rows on factIngress
        cursor.execute("""\
        SELECT COUNT(*) AS count
        FROM factIngress
        """)
        
        if cursor.fetchone()[0] < 100000:
            raise ValueError("Low record count on factIngress")
