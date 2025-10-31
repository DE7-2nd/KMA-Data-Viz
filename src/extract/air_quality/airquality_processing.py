import pandas as pd
import numpy as np
import glob
import os

# 1️. Paths
raw_path = "src/data/air_quality/raw_data/"
processed_path = "src/data/air_quality/processed_air_quality.csv"

# 2️. Read all raw CSVs and add city column
files = glob.glob(os.path.join(raw_path, "*.csv"))
dfs = []

for file in files:
    city = os.path.basename(file).replace(".csv", "")
    df = pd.read_csv(file)
    df["city"] = city
    
    # Convert to long format
    df_long = df.melt(id_vars=["date", "city"], var_name="pollutant", value_name="value")
    dfs.append(df_long)

# 3️. Combine all cities
all_cities_df = pd.concat(dfs, ignore_index=True)

# 4️. Handle missing values
# Convert 'date' to datetime
all_cities_df['date'] = pd.to_datetime(all_cities_df['date'])

# Convert empty strings to NaN
all_cities_df['value'].replace('', np.nan, inplace=True)

# Drop rows with missing values
all_cities_df = all_cities_df.dropna()

# 5️. Filter data up to 2025-10-24
all_cities_df = all_cities_df[all_cities_df['date'] <= '2025-10-24']

# 6️. Sort (optional)
all_cities_df = all_cities_df.sort_values(['city', 'pollutant', 'date'])

# 7️. Save processed data
all_cities_df.to_csv(processed_path, index=False)

print(f"Processed air quality data saved to: {processed_path}")
