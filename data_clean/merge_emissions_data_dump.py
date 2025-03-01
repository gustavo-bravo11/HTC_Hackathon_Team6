import pandas as pd
import os

# Load datasets
ab_emissions = pd.read_csv("../lat_long_to_lsd/ab_emissions_with_lsd.csv")

# Clean column names
ab_emissions.columns = [
    col.split('/')[0].strip().lower().replace(' ', '_').replace('(', '_').replace(')', '')
    for col in ab_emissions.columns
]

# Rename column
ab_emissions.rename(columns={'ghgrp_id_no.': 'ghgrp_id'}, inplace=True)

# Extract relevant facility data
ghg_facilities = ab_emissions[[
    'ghgrp_id', 'reference_year', 'facility_name', 'dls', 'total_emissions__tonnes_co2e'
]].drop_duplicates().reset_index(drop=True)

# Load facility data
facilities_data = pd.read_csv("data_cleaned/facilities/facilities.csv")

# Merge with facilities data
merged = facilities_data.merge(ghg_facilities, left_on="location", right_on="dls", how="inner")

# Summarize emissions data
facilities_total_emissions = merged.groupby(['id', 'reference_year'])[['total_emissions__tonnes_co2e']].sum().reset_index()

# Save results
path = "data_cleaned/yearly_total_emissions_co2"
os.makedirs(path, exist_ok=True)
merged.to_csv(f"{path}/yearly_total_emissions_co2.csv", index=False)
