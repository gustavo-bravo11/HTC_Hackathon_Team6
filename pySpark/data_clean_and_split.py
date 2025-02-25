from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf
from pyspark.sql.types import StringType, DoubleType
import os

# Initialize Spark session
spark = SparkSession.builder.appName("DataProcessing").getOrCreate()

# Load data
data = spark.read.csv("../data_sets/Geoscout Facility Data Dump.csv", header=True, inferSchema=False)

# Rename primary key column
data = data.withColumnRenamed("Unique Facility ID", "id")

# Dictionary to hold tables
data_dict = {
    "facilities": data.select(data.columns[:13]).dropDuplicates(),
    "facility_capacity": data.select(["id"] + data.columns[13:35]).dropDuplicates(),
    "facility_total_production": data.select(["id"] + data.columns[35:45]).dropDuplicates(),
    "facility_monthly_production": data.select(["id"] + data.columns[45:76]).dropDuplicates(),
}

# Standardize column names
def clean_column_names(df):
    for col_name in df.columns:
        new_col_name = col_name.lower().replace(' ', '_').replace('(', '_').replace(')', '')
        df = df.withColumnRenamed(col_name, new_col_name)
    return df

data_dict = {name: clean_column_names(df) for name, df in data_dict.items()}

# Convert coordinates from DMS to decimal degrees
@udf(DoubleType())
def dms_to_dd(dms):
    if dms is None:
        return None
    try:
        degrees, direction = dms.split(' deg ')
        degrees = float(degrees)
        if direction in ['S', 'W']:
            degrees = -degrees
        return degrees
    except:
        return None

data_dict["facilities"] = data_dict["facilities"].withColumn("latitude", dms_to_dd(col("latitude")))
data_dict["facilities"] = data_dict["facilities"].withColumn("longitude", dms_to_dd(col("longitude")))

# Drop columns with all NaN values
def drop_empty_columns(df):
    return df.dropna(how='all')

data_dict = {name: drop_empty_columns(df) for name, df in data_dict.items()}

# Standardize operator names and format dates
data_dict["facilities"] = data_dict["facilities"].withColumn("current_operator", col("current_operator").cast(StringType()))
data_dict["facilities"] = data_dict["facilities"].withColumn("co-owner", col("co-owner").cast(StringType()))
data_dict["facilities"] = data_dict["facilities"].withColumn("constructed_date", col("constructed_date").cast(StringType()))
data_dict["facility_monthly_production"] = data_dict["facility_monthly_production"].withColumn("date", col("date").cast(StringType()))

# Save cleaned data
output_dir = "data_cleaned"
if not os.path.exists(output_dir):
    os.mkdir(output_dir)

for name, df in data_dict.items():
    df.write.csv(os.path.join(output_dir, f"{name}"), header=True, mode="overwrite")