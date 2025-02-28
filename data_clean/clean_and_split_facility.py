import pandas as pd
import os

# Load data
data = pd.read_csv("../data_sets/Geoscout Facility Data Dump.csv", low_memory=False)

# Rename primary key column
data.columns = data.columns.str.replace('Unique Facility ID', 'id')

# Normalize colum names
data.columns = data.columns.str.lower().str.replace(' ', '_').str.replace('(', '_').str.replace(')', '')

# Drop columns with no id name or operator
data.dropna(subset=['id', 'current_operator', 'name'], inplace=True)

# Split the data into seperate tables
facilities = data.iloc[:, :13].drop_duplicates().reset_index(drop=True)
facility_capacity = data.iloc[:, [0] + list(range(13, 35))].drop_duplicates().reset_index(drop=True)
facility_total_production = data.iloc[:, [0] + list(range(35, 45))].drop_duplicates().reset_index(drop=True)
facility_monthly_production = data.iloc[:, [0] + list(range(45, 76))].drop_duplicates().reset_index(drop=True)

# Add a prefix and make the date name more appropriate
facility_monthly_production.columns = facility_monthly_production\
    .columns.map(lambda x: 'monthly_' + x if x not in ['id', 'location', 'date', 'sub_type'] else x)
facility_monthly_production.rename(columns={'date': 'production_month'}, inplace=True)

# # This table contains no data
# facility_inventory_meter = data.iloc[:, [0] + list(range(76, 94))].drop_duplicates().reset_index(drop=True)

# Create dictionary of dataframes for easier manipulation
data_dict = {
    "facilities": facilities,
    "facility_capacity": facility_capacity,
    "facility_total_production": facility_total_production,
    "facility_monthly_production": facility_monthly_production,
}

# Function to convert DMS coordinates to decimal degrees
def dms_to_dd(dms):
    if pd.isna(dms):
        return
    degrees, direction = dms.split(' deg ')
    degrees = float(degrees)
    if direction in ['S', 'W']:
        degrees = -degrees
    return degrees

# Apply coordinate conversion
data_dict["facilities"]["latitude"] = data_dict["facilities"]["latitude"].apply(dms_to_dd)
data_dict["facilities"]["longitude"] = data_dict["facilities"]["longitude"].apply(dms_to_dd)

# Drop columns with all NaN values
facilities.dropna(axis=1, how='all', inplace=True)
facility_capacity.dropna(axis=1, how='all', inplace=True)
facility_total_production.dropna(axis=1, how='all', inplace=True)
facility_monthly_production.dropna(axis=1, how='all', inplace=True)

# Standardize operator names
facilities['current_operator'] = facilities['current_operator'].replace({
    'Altagas Holdings Inc.': 'Altagas Ltd.',
    'Atco Pipelines (North Tn8263923)': 'Atco Energy Solutions Ltd.',
    'Canlin Resources Partnership': 'Canlin Energy Corporation',
    'Conocophillips Canada Resources Corp.': 'Conocophillips Canada',
    'Conocophillips Canada Energy Partnership': 'Conocophillips Canada',
    'Encana Corporation': 'Ovintiv Canada Ulc.',
    'Encana Western Resources Ltd.': 'Ovintiv Canada Ulc.',
    'Exxonmobil Canada Energy': 'Exxonmobil',
    'Inter Pipeline Extraction Ltd.': 'Inter Pipeline',
    'Inter Pipeline Offgas Ltd.': 'Inter Pipeline',
    'Northriver Midstream Energy Holdings Limited': 'Northriver Midstream',
    'Northriver Midstream Energy Limited': 'Northriver Midstream',
    'Northriver Midstream G And P Canada Inc.': 'Northriver Midstream',
    'Northriver Midstream Inc.': 'Northriver Midstream',
    'Northriver Midstream Operations Gp Inc.': 'Northriver Midstream',
    'Pembina Gas Services Ltd.': 'Pembina Pipeline',
    'Pembina Ngl Corporation': 'Pembina Pipeline',
    'Shell Canada Energy': 'Shell Canada Limited',
    'Xto Energy Canada': 'Xto Energy Canada Ulc'
})

# Lastly, I want to add the LSD column from the monthly production data to the facility table
lsd = facility_monthly_production[['id', 'location']].drop_duplicates().reset_index(drop=True)
data_dict['facilities'] = facilities.merge(lsd, on='id', how='left')

# Convert text fields to title case
for name, df in data_dict.items():
    for col in df.select_dtypes(include='object').columns:
        if col not in ['id', 'type', 'location']:
            df[col] = df[col].apply(lambda x: x.title() if pd.notnull(x) else x)

# Convert date fields
data_dict["facilities"]["constructed_date"] = pd.to_datetime(data_dict["facilities"]["constructed_date"], format='%m/%d/%Y')
data_dict["facility_monthly_production"]["production_month"] = pd.to_datetime(data_dict["facility_monthly_production"]["production_month"], format='%Y/%m')

# Ensure output directory exists
if not os.path.exists("data_cleaned"):
    os.mkdir("data_cleaned")

# Save cleaned datasets
for name, df in data_dict.items():
    if not os.path.exists(f"data_cleaned/{name}"):
        os.mkdir(f"data_cleaned/{name}")
    df.to_csv(f"data_cleaned/{name}/{name}.csv", index=False)

# for df in data_dict.values():
#     print(df.head(10))