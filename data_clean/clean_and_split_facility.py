import pandas as pd
import os

# Load data
data = pd.read_csv("../data_sets/Facility_Data Pull.csv", low_memory=False)

# Rename primary key column
data.columns = data.columns.str.replace('Unique Facility ID', 'id')

# Normalize colum names
data.columns = data.columns.str.lower().str.replace(' ', '_').str.replace('(', '_').str.replace(')', '')

# Drop columns with no id name or operator
data.dropna(subset=['id', 'current_operator', 'name'], inplace=True)

# Split the data into seperate tables
data.columns = data.columns.str.replace('Unique Facility ID', 'id')
facilities = data.iloc[:, list(range(13)) + [45, 46]].drop_duplicates().reset_index(drop=True)
facility_capacity = data.iloc[:, [0] + list(range(13, 35))].drop_duplicates().reset_index(drop=True)
facility_total_production = data.iloc[:, [0] + list(range(35, 80))].drop_duplicates().reset_index(drop=True)
facility_monthly_production = data.iloc[:, [0] + list(range(80, 127))].drop_duplicates().reset_index(drop=True)

# Add all data to a dictionary to loop over it and do high level analysis
data_dict = {
    "facilities": facilities,
    "facility_capacity": facility_capacity,
    "facility_total_production": facility_total_production,
    "facility_monthly_production": facility_monthly_production
}

# Add a prefix and make the date name more appropriate
facility_monthly_production.columns = facility_monthly_production\
    .columns.map(lambda x: 'monthly_' + x if x not in ['id', 'location', 'date', 'sub_type'] else x)
facility_monthly_production.rename(columns={'date': 'production_month'}, inplace=True)

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

# Drop columns that contain only (NaN or 0)
for df in data_dict.values():
    df.drop(columns=df.columns[(df.eq(0) | df.isna()).all()], inplace=True)

# Standardize operator names
data_dict['facilities'].replace({
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
}, inplace=True)

print(sorted(data_dict['facilities']['current_operator'].unique()))

data_dict['facilities']['name'] = facilities['name'].str.lstrip('* ')

# Convert text fields to title case
for name, df in data_dict.items():
    for col in df.select_dtypes(include='object').columns:
        if col not in ['id', 'type', 'location']:
            df[col] = df[col].apply(lambda x: x.title() if pd.notnull(x) else x)

# Convert date fields
data_dict["facilities"]["constructed_date"] = pd.to_datetime(data_dict["facilities"]["constructed_date"], format='%Y-%m-%d')
data_dict["facility_monthly_production"]["production_month"] = pd.to_datetime(data_dict["facility_monthly_production"]["production_month"], format='%b-%y')

monthly_prod_melted = pd.melt(
    data_dict['facility_monthly_production'],
    id_vars=['id', 'production_month'],
    var_name='product',
    value_name='mcf/d'
)
data_dict['facility_monthly_production_pivoted'] = monthly_prod_melted

# Ensure output directory exists
if not os.path.exists("data_cleaned"):
    os.mkdir("data_cleaned")

# Save cleaned datasets
for name, df in data_dict.items():
    if not os.path.exists(f"data_cleaned/{name}"):
        os.mkdir(f"data_cleaned/{name}")
    df.to_csv(f"data_cleaned/{name}/{name}.csv", index=False)



# print(data_dict['facilities'].head(30))