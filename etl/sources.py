"""
Data sources related functions for "Project: Capstone Project"
"""
import re

from functools import reduce
from itertools import islice, chain, zip_longest

from bs4 import BeautifulSoup
import pandas as pd
import requests

from pyspark.sql import functions as F
from pyspark.sql import SparkSession, Row
from pyspark.sql.types import ArrayType, IntegerType, StringType
from pyspark.sql.window import Window
from pyspark import SparkFiles

from .common import _simple_validation

I94_SAS_LABELS_DESCRIPTIONS_FILE = "/home/workspace/I94_SAS_Labels_Descriptions.SAS"

 # I'm limiting the number of files read due to restrictions on S3 usage
I94_DATA_FILES = [
    "/data/18-83510-I94-Data-2016/i94_apr16_sub.sas7bdat",
]

GLOBAL_LAND_TEMPERATURES_BY_CITY_FILE = "/data2/GlobalLandTemperaturesByCity.csv"

I94_CNTYL_FILE = "i94cntyl.csv"

AIRPORT_CODES_FILE = "airport-codes_csv.csv"


def convert_i94_sas_labels_descriptions():
    """Converts the information from I94_SAS_Labels_Descriptions.SAS"""
    value_re = re.compile(r"^\s*('(?P<s_value>.*)'|(?P<n_value>\d*))\s*=\s*'(?P<description>.*)'.*$")

    i94cntyl_rows = []
    in_i94cntyl_value = False

    with open(I94_SAS_LABELS_DESCRIPTIONS_FILE, "r") as f:
        for line in f:
            if re.search("value i94cntyl", line):
                in_i94cntyl_value = True
            elif ";" in line:
                in_i94cntyl_value = None
            elif in_i94cntyl_value:
                match = value_re.match(line)
                if match is not None:
                    i94cntyl_rows.append({
                        'value': int(match.group("n_value").strip()) if match.group("n_value") is not None else match.group("s_value").strip(),
                        'description': match.group("description").strip()
                    })

    # Country codes
    i94cntyl_df = pd.DataFrame(i94cntyl_rows, columns=("value", "description"))
    i94cntyl_df.to_csv(I94_CNTYL_FILE, index=False)


def get_i94_data_df(spark):
    """Creates a PySpark DataFrame from the i94 immigration data"""

    def age_bucket_udf(age):
        if age is None:
            return None

        bucket = None

        if age < 12:
            bucket = "CH"
        elif 12 <= age < 18:
            bucket = "TE"
        elif 18 <= age < 65:
            bucket = "AD"
        elif 65 < age:
            bucket = "OA"

        return bucket

    # Filter only rows for air travel
    # Convert the arrdate to a date
    # Add the age_bucket column
    # Add an year column for partitioning
    # Partition by year and i94port
    i94_data_df = reduce(
        lambda accum, df: accum.union(df),
        map(
            lambda f: spark.read.format("com.github.saurfang.sas.spark").load(f).select(
                ["i94port", "i94cit", "arrdate", "i94mode", "biryear", "gender"]
            ),
            I94_DATA_FILES
        )
    )\
    .filter(F.col("i94mode") == 1)\
    .drop("i94mode")\
    .withColumn("arrdate", F.expr("date_add(to_date('1960-01-01'), arrdate)"))\
    .withColumn("year", F.expr("year(arrdate)"))\
    .withColumn("age", F.expr("year - biryear"))\
    .withColumn("age_bucket", F.udf(age_bucket_udf)(F.col("age")))\
    .withColumn("date_id", F.udf(lambda d: int(d.strftime("%Y%m%d")), IntegerType())(F.col("arrdate")))\
    .withColumn("gender", F.coalesce(F.col("gender"), F.lit("U")))\
    .drop("age", "biryear")\
    .repartition("year", "i94port")

    return i94_data_df


def validate_i94_data_df(df):
    """Performs simple validations on the I94 immigration data DataFrame"""
    
    _simple_validation(df, "i94_data_df", ["i94port", "i94cit", "arrdate", "year", "gender", "age_category"], 1000000)


def get_i94cntyl_df(spark):
    """Creates a PySpark dataframe with country codes from I94_SAS_Labels_Descriptions.SAS"""
    i94cntyl_df = spark.read.csv(I94_CNTYL_FILE, header=True)

    return i94cntyl_df


def validate_i94cntyl_df(df):
    """Performs simple validations on the country codes DataFrame"""

    _simple_validation(df, "i94cntyl_df", ["value", "description"], 10)


def get_cbp_codes_df(spark):
    """
    Creates a PySpark DataFrame of CPB codes from https://redbus2us.com/travel/usa/us-customs-and-border-protection-cbp-codes-port-of-entry-stamp/
    """

    # We need to use this header to fool the server into actually serving the data
    headers = {"User-Agent": "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:95.0) Gecko/20100101 Firefox/95.0"}
    response = requests.get(
        "https://redbus2us.com/travel/usa/us-customs-and-border-protection-cbp-codes-port-of-entry-stamp/",
        headers=headers)

    doc = BeautifulSoup(response.content, "html.parser")

    cbp_codes_df = spark.createDataFrame(
        Row(code=t[0], location=t[1])  # Build the frame with the code and location values
        for t in zip_longest(
            *[
                chain.from_iterable(
                    filter(
                        lambda t: re.match("^[A-Z]{3}$", t[0]),  # The first column looks like a code
                        (
                            tuple(map(lambda e: e.text, tr_elem.find_all("td")))
                            for tr_elem in doc.find_all("tr")  # Find all tr elements
                        )
                    )
                )
            ]*2  # Split the list in twos
        )
    ).withColumn(
        "municipality", F.udf(lambda s: s.split(",")[0].strip() if "," in s else None)(F.col("location"))
    ).withColumn(
        "state", F.udf(lambda s: s.split(",")[1].strip() if "," in s else None)(F.col("location"))
    ).drop("location")

    return cbp_codes_df


def validate_cbp_codes_df(df):
    """Performs simple validations on the CBP codes DataFrame"""

    _simple_validation(df, "cbp_codes_df", ['code', 'municipality', 'state'], 10)


def get_temperatures_df(spark):
    """Creates a PySpark DataFrame of average temperatures in US cities."""
    temperatures_df = spark.read.csv(GLOBAL_LAND_TEMPERATURES_BY_CITY_FILE, header=True)\
        .where(F.col("Country") == "United States")

    max_year = temperatures_df.select([
        F.max(
            F.year(F.col("dt"))
        ).alias('max_year')
    ]).collect()[0].max_year

    def temperature_bucket_udf(average_temperature):
        if average_temperature is None:
            return None

        bucket = None

        if average_temperature < 5:
            bucket = "VC"
        elif 5 <= average_temperature < 15:
            bucket = "CO"
        elif 15 <= average_temperature < 25:
            bucket = "MI"
        elif 25 <= average_temperature < 35:
            bucket = "HO"
        else:
            bucket = "VH"

        return bucket

    temperatures_df = temperatures_df\
        .where((F.year(F.col("dt")) > max_year - 5) & (F.year(F.col("dt")) <= max_year))\
        .groupBy([F.col("City"), F.weekofyear(F.col("dt")).alias("week_of_year")]).agg(F.mean("AverageTemperature").alias("AverageTemperature"))\
        .withColumn(
           "temperature_bucket",
           F.udf(temperature_bucket_udf)(F.col("AverageTemperature"))
        )\
        .drop("AverageTemperature")\
        .select(
            F.col("City").alias("city"),
            F.col("week_of_year"),
            F.col("temperature_bucket")
        ).orderBy("city", "week_of_year")

    return temperatures_df


def validate_temperatures_df(df):
    """Performs simple validations on the average temperatures DataFrame"""

    _simple_validation(df, "temperatures_df", ['City', 'WeekOfYear', 'TemperatureCategory'], 100)


def get_national_accounts_df(spark):
    """Creates a PySpark DataFrame with economic country information from http://data.un.org/_Docs/SYB/CSV/SYB64_230_202110_GDP%20and%20GDP%20Per%20Capita.csv."""

    response = requests.get("http://data.un.org/_Docs/SYB/CSV/SYB64_230_202110_GDP%20and%20GDP%20Per%20Capita.csv", stream=True)

    with open('SYB64_230_202110_GDP_and_GDP_Per_Capita.csv', "w") as f:
        f.writelines(map(lambda l: f"{l.decode('latin1')}\n", islice(response.iter_lines(), 1, None)))  # We need to drop the first extra line

    # We remove the totalizing rows and select only the GDP per capita series
    national_accounts_df = spark.read.csv("SYB64_230_202110_GDP_and_GDP_Per_Capita.csv", header=True)\
        .where(~F.col("Region/Country/Area").isin(1, 2, 15, 202, 14, 17, 18, 11, 19, 21, 419, 29, 13, 5, 142, 143, 30, 35, 34, 145, 150, 151, 154, 39, 155, 9, 53, 54, 57, 61))\
        .where(F.col("Series") == "GDP per capita (US dollars)")\
        .where(F.col("Year") == 2017)\
        .select([
            F.col("_c1").alias("country"),
            F.udf(lambda s: s.replace(",", ""))(F.col("Value")).cast("double").alias("gdp_per_capita")
        ])

    return national_accounts_df


def validate_national_accounts_df(df):
    """Performs simple validations on the UN data DataFrame"""

    _simple_validation(df, "national_accounts_df", ['country', 'gdp_per_capita'], 100)


def get_i94cntyl_df(spark):
    """Create a PySpark DataFrame from the country codes extracted from I94_SAS_Labels_Descriptions.SAS"""

    i94cntyl_df = spark.read.csv("i94cntyl.csv", header=True)
    return i94cntyl_df


def validate_i94cntyl_df(df):
    """Performs simple validations on the country codes DataFrame"""

    _simple_validation(df, "i94cntyl_df", ['value', 'description'], 10)


def get_airports_df(spark):
    """Create a PySpark DataFrame from the airports code CSV file"""

    municipality_state_window = Window.partitionBy("state", "municipality").orderBy(F.asc("ident"))

    # We're only interested in large, international US airports. There might be duplicate airports per
    # state, so we only keep the first one.
    airports_df = spark.read.csv(AIRPORT_CODES_FILE, header=True)\
        .select(["ident", "name", "iso_country", "iso_region", "municipality", "type", "iata_code"])\
        .filter(F.col("iso_country") == "US")\
        .filter(F.col("type") == "large_airport")\
        .filter(F.col("iata_code").isNotNull())\
        .withColumn("state", F.udf(lambda s: re.sub(r"US-", "", s))(F.col("iso_region")))\
        .withColumn("row_number", F.row_number().over(municipality_state_window))\
        .filter(F.col("row_number") == 1)\
        .drop("iata_code", "iso_country", "iso_region", "row_number", "type")

    return airports_df


def validate_airports_df(df):
    """Performs simple validations on the airport codes DataFrame"""
    
    _simple_validation(df, "airports_df", ["ident", "municipality", "state", "name"], 10)


def get_countries_df(spark):
    """Create a PySpark DataFrame with country information from https://raw.githubusercontent.com/OpenBookPrices/country-data/master/data/countries.csv"""

    spark.sparkContext.addFile("https://raw.githubusercontent.com/OpenBookPrices/country-data/master/data/countries.csv")

    countries_df = spark.read.csv(f"file://{SparkFiles.get('countries.csv')}", header=True)\
        .withColumn("languages", F.udf(lambda s: s and s.split(","), ArrayType(StringType()))(F.col("languages")))\
        .select(["name", "alpha2", "alpha3", "languages"])

    return countries_df


def validate_countries_df(df):
    """Performs simple validations on the countries information DataFrame"""
    
    _simple_validation(df, "countries_df", ["name", "alpha2", "alpha3", "languages"], 10)


def get_languages_df(spark):
    """Create a PySpark DataFrame with country information from https://raw.githubusercontent.com/OpenBookPrices/country-data/master/data/languages.csv"""

    spark.sparkContext.addFile("https://raw.githubusercontent.com/OpenBookPrices/country-data/master/data/languages.csv")

    languages_df = spark.read.csv(f"file://{SparkFiles.get('languages.csv')}", header=True)\
        .select(["name", "alpha2", "alpha3"])

    return languages_df


def validate_languages_df(df):
    """Performs simple validations on the languages information DataFrame"""
    
    _simple_validation(df, "languages_df", ["name", "alpha2", "alpha3"], 10)
