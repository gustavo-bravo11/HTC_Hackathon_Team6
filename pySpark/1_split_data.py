def MyTransform (glueContext, dfc) -> DynamicFrameCollection:
    # 1
    # Change the schema and update fields
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
    
    # 2 Fill na's
    # For all values that are empty string or N/A make them null
    from pyspark.sql.functions import when, lit, col
    default_value = None
    
    for column_name, dtype in df.dtypes:
        if dtype == "string":
            df = df.withColumn(
                column_name,
                when(
                    (col(column_name) == "")|(col(column_name) == "N/A"), 
                    lit(default_value)
                ).otherwise(col(column_name))
            )
    # result = {"result": DynamicFrame.fromDF(df, glueContext, "df")}
    
    # 3 Split the dataset into three
    
    # Facility data: first 13 columns
    facility = df.select(df.columns[:13]).dropDuplicates()
    
    # Capacity data: first column plus columns 13 to 34
    capacity_cols = [df.columns[0]] + df.columns[13:35]
    facility_capacity = df.select(capacity_cols).dropDuplicates()
    
    # Total production data: first column plus columns 35 to 44
    total_prod_cols = [df.columns[0]] + df.columns[35:45]
    facility_total_production = df.select(total_prod_cols).dropDuplicates()
    
    # Monthly production data: first column plus columns 45 to 75
    monthly_prod_cols = [df.columns[0]] + df.columns[45:76]
    facility_monthly_production = df.select(monthly_prod_cols).dropDuplicates()
    
    # Inventory and metering data: first column plus columns 76 to 93
    inventory_meter_cols = [df.columns[0]] + df.columns[76:94]
    facility_inventory_meter = df.select(inventory_meter_cols).dropDuplicates()
    
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