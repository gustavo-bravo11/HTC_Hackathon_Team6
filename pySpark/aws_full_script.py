import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrameCollection
from awsgluedq.transforms import EvaluateDataQuality
from awsglue.dynamicframe import DynamicFrame

# Script generated for node Change Schema
def MyTransform(glueContext, dfc) -> DynamicFrameCollection:
    # Retrieve the single DynamicFrame from the collection
    raw_dyf = list(dfc.values())[0]
    df = raw_dyf.toDF()

    from pyspark.sql.functions import col, to_date

    # Loop over each column to adjust its type based on column name
    for c in df.columns:
        lower = c.lower()
        if lower == "date":
            df = df.withColumn(c, to_date(col(c), "yyyy/MM"))
        elif "date" in lower:
            df = df.withColumn(c, to_date(col(c), "M/d/yyyy"))
        # If the column name includes parentheses (indicating units) cast to double.
        elif ("(" in c and ")" in c):
            df = df.withColumn(c, col(c).cast("double"))
        else:
            df = df.withColumn(c, col(c).cast("string"))

    # Change the column names 
    df = df.withColumnRenamed("Unique Facility ID", "id")

    # Change the column names to be snake_case
    new_columns = [
        col.lower().replace(' ', '_')\
            .replace('(', '_').replace(')', '') for col in df.columns
        ]
    df = df.toDF(*new_columns)

    schema_update_dyf = DynamicFrame.fromDF(df, glueContext, "schema_update_dyf")

    # Return the result as a DynamicFrameCollection
    return DynamicFrameCollection({"schema_update_dyf": schema_update_dyf}, glueContext)
# Script generated for node Split Data
def MyTransform(glueContext, dfc) -> DynamicFrameCollection:
    data = dfc.select(list(dfc.keys())[0]).toDF()

    # Facility data: first 13 columns
    facility = data.select(data.columns[:13]).dropDuplicates()

    # Capacity data: first column plus columns 13 to 34
    capacity_cols = [data.columns[0]] + data.columns[13:35]
    facility_capacity = data.select(capacity_cols).dropDuplicates()

    # Total production data: first column plus columns 35 to 44
    total_prod_cols = [data.columns[0]] + data.columns[35:45]
    facility_total_production = data.select(total_prod_cols).dropDuplicates()

    # Monthly production data: first column plus columns 45 to 75
    monthly_prod_cols = [data.columns[0]] + data.columns[45:76]
    facility_monthly_production = data.select(monthly_prod_cols).dropDuplicates()

    # Inventory and metering data: first column plus columns 76 to 93
    inventory_meter_cols = [data.columns[0]] + data.columns[76:94]
    facility_inventory_meter = data.select(inventory_meter_cols).dropDuplicates()

    # Convert back to dynamic frames and create dictionary to output

    dyf_facility = DynamicFrame.fromDF(facility, glueContext, "facility")
    dyf_facility_capacity = DynamicFrame.fromDF(facility_capacity, glueContext, "facility_capacity")
    dyf_facility_total_production = DynamicFrame.fromDF(facility_total_production, glueContext, "facility_total_production")
    dyf_facility_monthly_production = DynamicFrame.fromDF(facility_monthly_production, glueContext, "facility_monthly_production")
    dyf_facility_inventory_meter = DynamicFrame.fromDF(facility_inventory_meter, glueContext, "facility_inventory_meter")

    result = {
        "facility": dyf_facility,
        "facility_capacity": dyf_facility_capacity,
        "facility_total_production": dyf_facility_total_production,
        "facility_monthly_production": dyf_facility_monthly_production,
        "facility_inventory_meter": dyf_facility_inventory_meter
    }

    return DynamicFrameCollection(result, glueContext)
# Script generated for node Change Lat Long
def MyTransform(glueContext, dfc) -> DynamicFrameCollection:
    """
    A custom transform that:
      - Retrieves the first DynamicFrame in the collection
      - Converts 'latitude' and 'longitude' columns from a format like "52.14108 deg N"
        into decimal degrees using an inline lambda UDF
      - Returns a new DynamicFrameCollection
    """
    from pyspark.sql.functions import udf, col
    from pyspark.sql.types import DoubleType

    # 1) Extract the single DynamicFrame from the collection and convert to Spark DataFrame
    input_dyf = list(dfc.values())[0]
    df = input_dyf.toDF()

    # 2) Define a UDF inline (no separate helper function)
    #    - If the string is empty, None, or missing " deg ", returns None
    #    - Otherwise, parses the portion before " deg " as float
    #      and negates it if direction is S or W
    dms_to_dd_udf = udf(
        lambda s: None
        if s is None
        else (
            float(s)  # If already numeric, just cast and return
            if isinstance(s, (int, float))
            else (
                None
                if not isinstance(s, str) or " deg " not in s
                else (
                    float(s.split(" deg ")[0])
                    * (
                        -1
                        if s.split(" deg ")[1].strip().upper() in ["S", "W"]
                        else 1
                    )
                )
            )
        ),
        DoubleType()
    )

    # 3) Apply the UDF to 'latitude' and 'longitude' columns
    #    (Adjust these column names if yours differ.)
    df = df.withColumn("latitude", dms_to_dd_udf(col("latitude"))) \
           .withColumn("longitude", dms_to_dd_udf(col("longitude")))

    # 4) Convert back to a DynamicFrame
    output_dyf = DynamicFrame.fromDF(df, glueContext, "facility_cleaned")

    # 5) Return the result in a DynamicFrameCollection
    return DynamicFrameCollection({"facility": output_dyf}, glueContext)
# Script generated for node blank = "N/A"
def MyTransform(glueContext, dfc) -> DynamicFrameCollection:
    # Get the initial DynamicFrame
    input_dyf = dfc.select(list(dfc.keys())[0])

    # Get all expected columns
    expected_columns = input_dyf.toDF().columns  # Extract column names from the original schema

    # Define a function that will be applied to each record
    def fill_blanks(rec):
        # Ensure all expected keys exist in the record
        new_rec = {key: rec.get(key, None) for key in expected_columns}  # Preserve all columns

        # Replace empty strings and "N/A" with None
        for key, value in new_rec.items():
            if isinstance(value, str) and (value == "" or value == "N/A"):
                new_rec[key] = None
        
        return new_rec

    # Apply the transformation
    output_dyf = Map.apply(frame=input_dyf, f=fill_blanks, transformation_ctx="fill_blanks")

    # Return the transformed DynamicFrame wrapped in a DynamicFrameCollection
    return DynamicFrameCollection({"output": output_dyf}, glueContext)
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Default ruleset used by all target nodes with data quality enabled
DEFAULT_DATA_QUALITY_RULESET = """
    Rules = [
        ColumnCount > 0
    ]
"""

# Script generated for node Amazon S3
AmazonS3_node1740371150658 = glueContext.create_dynamic_frame.from_options(format_options={"quoteChar": "\"", "withHeader": True, "separator": ",", "optimizePerformance": False}, connection_type="s3", format="csv", connection_options={"paths": ["s3://test-bucket-htc-20230223/Geoscout Facility Data Dump.csv"]}, transformation_ctx="AmazonS3_node1740371150658")

# Script generated for node Change Schema
ChangeSchema_node1740380280959 = MyTransform(glueContext, DynamicFrameCollection({"AmazonS3_node1740371150658": AmazonS3_node1740371150658}, glueContext))

# Script generated for node Select Changed Schema
SelectChangedSchema_node1740383649488 = SelectFromCollection.apply(dfc=ChangeSchema_node1740380280959, key=list(ChangeSchema_node1740380280959.keys())[0], transformation_ctx="SelectChangedSchema_node1740383649488")

# Script generated for node Change Schema (Node)
ChangeSchemaNode_node1740389241110 = ApplyMapping.apply(frame=SelectChangedSchema_node1740383649488, mappings=[("id", "string", "id", "string"), ("status", "string", "status", "string"), ("name", "string", "name", "string"), ("type", "string", "type", "string"), ("plant_name", "string", "plant_name", "string"), ("current_operator", "string", "current_operator", "string"), ("original_operator", "string", "original_operator", "string"), ("co-owner", "string", "co-owner", "string"), ("constructed_date", "date", "constructed_date", "date"), ("activated_date", "date", "activated_date", "date"), ("field_name", "string", "field_name", "string"), ("latitude", "string", "latitude", "string"), ("longitude", "string", "longitude", "string"), ("licensed_capacity_of_raw_gas_mcf/d", "double", "licensed_capacity_of_raw_gas_mcf/d", "double"), ("licensed_capacity_of_sales_gas_mcf/d", "double", "licensed_capacity_of_sales_gas_mcf/d", "double"), ("licensed_capacity_of_oil_bbl/d", "double", "licensed_capacity_of_oil_bbl/d", "double"), ("licensed_capacity_of_ethane_c2_bbl/d", "double", "licensed_capacity_of_ethane_c2_bbl/d", "double"), ("licensed_capacity_of_ethane_plus_c2+_bbl/d", "double", "licensed_capacity_of_ethane_plus_c2+_bbl/d", "double"), ("licensed_capacity_of_propane_c3_bbl/d", "double", "licensed_capacity_of_propane_c3_bbl/d", "double"), ("licensed_capacity_of_butane_c4_bbl/d", "double", "licensed_capacity_of_butane_c4_bbl/d", "double"), ("licensed_capacity_of_pentane_plus_c5+_bbl/d", "double", "licensed_capacity_of_pentane_plus_c5+_bbl/d", "double"), ("licensed_capacity_of_ngl_bbl/d", "double", "licensed_capacity_of_ngl_bbl/d", "double"), ("licensed_capacity_of_lpg_bbl/d", "double", "licensed_capacity_of_lpg_bbl/d", "double"), ("licensed_capacity_of_sulphur_ton/d", "double", "licensed_capacity_of_sulphur_ton/d", "double"), ("design_capacity_of_raw_gas_mcf/d", "double", "design_capacity_of_raw_gas_mcf/d", "double"), ("design_capacity_of_sales_gas_mcf/d", "double", "design_capacity_of_sales_gas_mcf/d", "double"), ("design_capacity_of_oil_bbl/d", "double", "design_capacity_of_oil_bbl/d", "double"), ("design_capacity_of_ethane_c2_bbl/d", "double", "design_capacity_of_ethane_c2_bbl/d", "double"), ("design_capacity_of_ethane_plus_c2+_bbl/d", "double", "design_capacity_of_ethane_plus_c2+_bbl/d", "double"), ("design_capacity_of_propane_c3_bbl/d", "double", "design_capacity_of_propane_c3_bbl/d", "double"), ("design_capacity_of_butane_c4_bbl/d", "double", "design_capacity_of_butane_c4_bbl/d", "double"), ("design_capacity_of_pentane_plus_c5+_bbl/d", "double", "design_capacity_of_pentane_plus_c5+_bbl/d", "double"), ("design_capacity_of_ngl_bbl/d", "double", "design_capacity_of_ngl_bbl/d", "double"), ("design_capacity_of_lpg_bbl/d", "double", "design_capacity_of_lpg_bbl/d", "double"), ("design_capacity_of_sulphur_ton/d", "double", "design_capacity_of_sulphur_ton/d", "double"), ("raw_gas_mcf/d", "double", "raw_gas_mcf/d", "double"), ("sales_gas_mcf/d", "double", "sales_gas_mcf/d", "double"), ("ethane_c2_bbl/d", "double", "ethane_c2_bbl/d", "double"), ("ethane_plus_c2+_bbl/d", "double", "ethane_plus_c2+_bbl/d", "double"), ("propane_c3_bbl/d", "double", "propane_c3_bbl/d", "double"), ("butane_c4_bbl/d", "double", "butane_c4_bbl/d", "double"), ("pentane_plus_c5+_bbl/d", "double", "pentane_plus_c5+_bbl/d", "double"), ("natural_gas_liquids_bbl/d", "double", "natural_gas_liquids_bbl/d", "double"), ("liquefied_petroleum_gas_bbl/d", "double", "liquefied_petroleum_gas_bbl/d", "double"), ("sulphur_ton/d", "double", "sulphur_ton/d", "double"), ("sub_type", "string", "sub_type", "string"), ("location", "string", "location", "string"), ("date", "date", "date", "date"), ("injection_mcf/d", "double", "injection_mcf/d", "double"), ("shrinkage_mcf/d", "double", "shrinkage_mcf/d", "double"), ("fuel_mcf/d", "double", "fuel_mcf/d", "double"), ("flared_mcf/d", "double", "flared_mcf/d", "double"), ("other_mcf/d", "double", "other_mcf/d", "double"), ("oil_sk_bbl/d", "double", "oil_sk_bbl/d", "double"), ("methane_plus_c1+_sk_bbl/d", "double", "methane_plus_c1+_sk_bbl/d", "double"), ("propane_plus_c3+_sk_bbl/d", "double", "propane_plus_c3+_sk_bbl/d", "double"), ("butane_plus_c4+_sk_bbl/d", "double", "butane_plus_c4+_sk_bbl/d", "double"), ("pentane_c5_sk_bbl/d", "double", "pentane_c5_sk_bbl/d", "double"), ("receipts_of_oil_sk_bbl/d", "double", "receipts_of_oil_sk_bbl/d", "double"), ("receipts_of_ethane_c2_sk_bbl/d", "double", "receipts_of_ethane_c2_sk_bbl/d", "double"), ("receipts_of_ethane_plus_c2+_sk_bbl/d", "double", "receipts_of_ethane_plus_c2+_sk_bbl/d", "double"), ("receipts_of_propane_c3_sk_bbl/d", "double", "receipts_of_propane_c3_sk_bbl/d", "double"), ("receipts_of_propane_plus_c3+_sk_bbl/d", "double", "receipts_of_propane_plus_c3+_sk_bbl/d", "double"), ("receipts_of_butane_c4_sk_bbl/d", "double", "receipts_of_butane_c4_sk_bbl/d", "double"), ("receipts_of_butane_plus_c4+_sk_bbl/d", "double", "receipts_of_butane_plus_c4+_sk_bbl/d", "double"), ("receipts_of_pentane_c5_sk_bbl/d", "double", "receipts_of_pentane_c5_sk_bbl/d", "double"), ("receipts_of_pentane_plus_c5+_sk_bbl/d", "double", "receipts_of_pentane_plus_c5+_sk_bbl/d", "double"), ("oil_production_bbl/d", "double", "oil_production_bbl/d", "double"), ("oil_received_bbl/d", "double", "oil_received_bbl/d", "double"), ("oil_open_inventory_bbl/d", "double", "oil_open_inventory_bbl/d", "double"), ("oil_close_inventory_bbl/d", "double", "oil_close_inventory_bbl/d", "double"), ("oil_delivered_bbl/d", "double", "oil_delivered_bbl/d", "double"), ("gas_production_mcf/d", "double", "gas_production_mcf/d", "double"), ("gas_received_mcf/d", "double", "gas_received_mcf/d", "double"), ("lease_fuel_mcf/d", "double", "lease_fuel_mcf/d", "double"), ("gas_flared_mcf/d", "double", "gas_flared_mcf/d", "double"), ("gas_vented_mcf/d", "double", "gas_vented_mcf/d", "double"), ("gas_meter_diff_mcf/d", "double", "gas_meter_diff_mcf/d", "double"), ("gas_delivered_mcf/d", "double", "gas_delivered_mcf/d", "double"), ("water_production_bbl/d", "double", "water_production_bbl/d", "double"), ("water_received_bbl/d", "double", "water_received_bbl/d", "double"), ("water_open_inventory_bbl/d", "double", "water_open_inventory_bbl/d", "double"), ("water_close_inventory_bbl/d", "double", "water_close_inventory_bbl/d", "double"), ("water_meter_diff_bbl/d", "double", "water_meter_diff_bbl/d", "double"), ("water_delivered_bbl/d", "double", "water_delivered_bbl/d", "double")], transformation_ctx="ChangeSchemaNode_node1740389241110")

# Script generated for node blank = "N/A"
blankNA_node1740385483066 = MyTransform(glueContext, DynamicFrameCollection({"ChangeSchemaNode_node1740389241110": ChangeSchemaNode_node1740389241110}, glueContext))

# Script generated for node Select Filled Null
SelectFilledNull_node1740385763885 = SelectFromCollection.apply(dfc=blankNA_node1740385483066, key=list(blankNA_node1740385483066.keys())[0], transformation_ctx="SelectFilledNull_node1740385763885")

# Script generated for node Split Data
SplitData_node1740371468880 = MyTransform(glueContext, DynamicFrameCollection({"SelectFilledNull_node1740385763885": SelectFilledNull_node1740385763885}, glueContext))

# Script generated for node Select Capacity
SelectCapacity_node1740376214623 = SelectFromCollection.apply(dfc=SplitData_node1740371468880, key=list(SplitData_node1740371468880.keys())[1], transformation_ctx="SelectCapacity_node1740376214623")

# Script generated for node Select Facility
SelectFacility_node1740376057134 = SelectFromCollection.apply(dfc=SplitData_node1740371468880, key=list(SplitData_node1740371468880.keys())[0], transformation_ctx="SelectFacility_node1740376057134")

# Script generated for node Select Total Production
SelectTotalProduction_node1740376224634 = SelectFromCollection.apply(dfc=SplitData_node1740371468880, key=list(SplitData_node1740371468880.keys())[2], transformation_ctx="SelectTotalProduction_node1740376224634")

# Script generated for node Select Monthly Production
SelectMonthlyProduction_node1740376421331 = SelectFromCollection.apply(dfc=SplitData_node1740371468880, key=list(SplitData_node1740371468880.keys())[3], transformation_ctx="SelectMonthlyProduction_node1740376421331")

# Script generated for node Select Inventory and Meter
SelectInventoryandMeter_node1740376448479 = SelectFromCollection.apply(dfc=SplitData_node1740371468880, key=list(SplitData_node1740371468880.keys())[4], transformation_ctx="SelectInventoryandMeter_node1740376448479")

# Script generated for node Change Lat Long
ChangeLatLong_node1740376557189 = MyTransform(glueContext, DynamicFrameCollection({"SelectFacility_node1740376057134": SelectFacility_node1740376057134}, glueContext))

# Script generated for node Select Facility (2)
SelectFacility2_node1740387181443 = SelectFromCollection.apply(dfc=ChangeLatLong_node1740376557189, key=list(ChangeLatLong_node1740376557189.keys())[0], transformation_ctx="SelectFacility2_node1740387181443")

# Script generated for node Amazon S3 (Capacity)
EvaluateDataQuality().process_rows(frame=SelectCapacity_node1740376214623, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1740386421756", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3Capacity_node1740387270890 = glueContext.write_dynamic_frame.from_options(frame=SelectCapacity_node1740376214623, connection_type="s3", format="glueparquet", connection_options={"path": "s3://test-bucket-htc-20230223/tables/", "partitionKeys": []}, format_options={"compression": "snappy"}, transformation_ctx="AmazonS3Capacity_node1740387270890")

# Script generated for node Amazon S3 (Total Production)
EvaluateDataQuality().process_rows(frame=SelectTotalProduction_node1740376224634, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1740386421756", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3TotalProduction_node1740387320914 = glueContext.write_dynamic_frame.from_options(frame=SelectTotalProduction_node1740376224634, connection_type="s3", format="glueparquet", connection_options={"path": "s3://test-bucket-htc-20230223/tables/", "partitionKeys": []}, format_options={"compression": "snappy"}, transformation_ctx="AmazonS3TotalProduction_node1740387320914")

# Script generated for node Amazon S3 (Monthly Production)
EvaluateDataQuality().process_rows(frame=SelectMonthlyProduction_node1740376421331, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1740386421756", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3MonthlyProduction_node1740387374250 = glueContext.write_dynamic_frame.from_options(frame=SelectMonthlyProduction_node1740376421331, connection_type="s3", format="glueparquet", connection_options={"path": "s3://test-bucket-htc-20230223/tables/", "partitionKeys": []}, format_options={"compression": "snappy"}, transformation_ctx="AmazonS3MonthlyProduction_node1740387374250")

# Script generated for node Amazon S3 (Inventory and Meter)
AmazonS3InventoryandMeter_node1740387406450 = glueContext.write_dynamic_frame.from_options(frame=SelectInventoryandMeter_node1740376448479, connection_type="s3", format="glueparquet", connection_options={"path": "s3://test-bucket-htc-20230223/tables/", "partitionKeys": []}, format_options={"compression": "snappy"}, transformation_ctx="AmazonS3InventoryandMeter_node1740387406450")

# Script generated for node Amazon S3 (Facility)
AmazonS3Facility_node1740387110710 = glueContext.write_dynamic_frame.from_options(frame=SelectFacility2_node1740387181443, connection_type="s3", format="glueparquet", connection_options={"path": "s3://test-bucket-htc-20230223/tables/", "partitionKeys": []}, format_options={"compression": "snappy"}, transformation_ctx="AmazonS3Facility_node1740387110710")

job.commit()