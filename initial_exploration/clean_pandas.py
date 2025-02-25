import pandas as pd
import os

# Load data
data = pd.read_csv("../data_sets/Geoscout Facility Data Dump.csv", low_memory=False)

# Rename primary key column
data.columns = data.columns.str.replace('Unique Facility ID', 'id')

# Extract and clean tables
facilities = data.iloc[:, :13].drop_duplicates().reset_index(drop=True)
facility_capacity = data.iloc[:, [0] + list(range(13, 35))].drop_duplicates().reset_index(drop=True)
facility_total_production = data.iloc[:, [0] + list(range(35, 45))].drop_duplicates().reset_index(drop=True)
facility_monthly_production = data.iloc[:, [0] + list(range(45, 76))].drop_duplicates().reset_index(drop=True)

# Dictionary to hold tables
data_dict = {
    "facilities": facilities,
    "facility_capacity": facility_capacity,
    "facility_total_production": facility_total_production,
    "facility_monthly_production": facility_monthly_production,
}

# Standardize column names
for df in data_dict.values():
    df.columns = df.columns.str.lower().str.replace(' ', '_').str.replace('(', '_').str.replace(')', '')

# Convert coordinates from DMS to decimal degrees
def dms_to_dd(dms):
    if pd.isna(dms):
        return None
    degrees, direction = dms.split(' deg ')
    degrees = float(degrees)
    if direction in ['S', 'W']:
        degrees = -degrees
    return degrees

facilities["latitude"] = facilities["latitude"].apply(dms_to_dd)
facilities["longitude"] = facilities["longitude"].apply(dms_to_dd)

# Drop columns with all NaN values
for df in data_dict.values():
    df.dropna(axis=1, how='all', inplace=True)

# Remove columns where all values are zero (Skip the strings)
for df in [facility_total_production.iloc[:, 1:], facility_monthly_production.iloc[:, 4:]]:
    for col in df.columns:
        try:
            if df[col].sum() == 0.0:
                df.drop(columns=[col], inplace=True)
        except TypeError:
            print(f"col {col} does not have numeric values")
            continue

# Standardize operator names and format dates
facilities['current_operator'] = facilities['current_operator'].str.title()
facilities['co-owner'] = facilities['co-owner'].str.title()
facilities['constructed_date'] = pd.to_datetime(facilities['constructed_date'], format='%m/%d/%Y')
facility_monthly_production['date'] = pd.to_datetime(facility_monthly_production['date'], format='%Y/%m')

# Save cleaned data
if not os.path.exists("data_cleaned"):
    os.mkdir("data_cleaned")

facilities.to_csv("data_cleaned/facilities.csv", index=False)
facility_capacity.to_csv("data_cleaned/facility_capacity.csv", index=False)
facility_total_production.to_csv("data_cleaned/facility_total_production.csv", index=False)
facility_monthly_production.to_csv("data_cleaned/facility_monthly_production.csv", index=False)
