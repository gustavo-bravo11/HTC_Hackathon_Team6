import sys
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

BUCKET_NAME = "htc-hackathon-team-6"
PATH = BUCKET_NAME + "/spatial_matching"

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

df = spark.read.csv(f"s3://{PATH}/ab_emissions.csv", header=True, inferSchema=True)

import geopandas as gpd
twp_grid = gpd.read_file(f"s3://{PATH}/ATS_Polygons_SHP_Geographic/V4-1_TWP.shp")
lsd_grid = gpd.read_file(f"s3://{PATH}/ATS_Polygons_SHP_Geographic/V4-1_LSD.shp")

# Ensure the shapefiles are in EPSG:4326
if twp_grid.crs.to_epsg() != 4326:
    twp_grid = twp_grid.to_crs(epsg=4326)
if lsd_grid.crs.to_epsg() != 4326:
    lsd_grid = lsd_grid.to_crs(epsg=4326)

# Broadcast the shapefile data to the executors
broadcast_twp = sc.broadcast(twp_grid)
broadcast_lsd = sc.broadcast(lsd_grid)

def find_dls_from_polygons(lat, lon):
    from shapely.geometry import Point
    import geopandas as gpd

    # Retrieve the broadcast shapefiles
    twp_grid = broadcast_twp.value
    lsd_grid = broadcast_lsd.value

    point = Point(lon, lat)
    matching = twp_grid[twp_grid.contains(point)]
    if matching.empty:
        print(f"No matching township for {lat, lon}")
        return None

    twp_polygon = matching.union_all()

    # Filter LSD grid polygons that intersect the township polygon and contain the point
    shape_filtered = lsd_grid[lsd_grid.intersects(twp_polygon)]
    result = shape_filtered[lsd_grid.contains(point)]
    if result.empty:
        print(f"No matching lsd for {lat, lon}")
        return None

    lsd = str(result['LS'].iloc[0]).zfill(2)
    sec = str(result['SEC'].iloc[0]).zfill(2)
    twp = str(result['TWP'].iloc[0]).zfill(3)
    rge = str(result['RGE'].iloc[0]).zfill(2)
    mer = "W" + str(result['M'].iloc[0]) + "M"

    dls_string = f"{lsd}-{sec}-{twp}-{rge}{mer}"

    print(f"Found DLS location {dls_string} for ({lat}, {lon})")

    return f"{lsd}-{sec}-{twp}-{rge}{mer}"

# Register the UDF with Spark.
find_dls_udf = udf(find_dls_from_polygons, StringType())

df_with_dls = df.withColumn("DLS", find_dls_udf(df["Latitude"], df["Longitude"]))

# Write the result back to S3 
df_with_dls.write.mode("overwrite").csv(f"s3://{PATH}/ab_emissions_with_lsd.csv", header=True)
