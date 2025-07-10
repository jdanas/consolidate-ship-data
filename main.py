import pandas as pd
import json

def create_consolidated_json():
    """
    This script reads the ship factors and AIS data from local CSV files
    and combines them into a single structured JSON file.
    """
    try:
        # --- Step 1: Load and Prepare Ship Factors Data ---
        # Read CSV with first row as header (skip the empty first header row)
        factors_df = pd.read_csv('4-21-day-result.csv', header=1)
        factors_df.columns = factors_df.columns.str.strip()
        
        print("📊 Column names after reading:", factors_df.columns.tolist())
        print("📊 First few rows:")
        print(factors_df.head())
        
        # Rename the unnamed columns to proper names
        factors_df = factors_df.rename(columns={
            'Unnamed: 0': 'Factor Category',
            'Unnamed: 1': 'Factor'
        })
        
        # Forward-fill the 'Factor Category' column to fill empty cells
        factors_df['Factor Category'] = factors_df['Factor Category'].ffill()
        
        # Remove any completely empty rows
        factors_df = factors_df.dropna(how='all')
        
        # Identify time columns (those that look like '0:00 - 1:00', etc.)
        id_vars = ['Factor Category', 'Factor']
        time_vars = [col for col in factors_df.columns if col not in id_vars and ':' in col and '-' in col]
        
        print("📊 Time columns found:", time_vars)
        
        # Melt the DataFrame
        factors_long_df = factors_df.melt(
            id_vars=id_vars,
            value_vars=time_vars,
            var_name='time_range',
            value_name='value'
        )
        
        # Remove rows with missing factor names or values
        factors_long_df = factors_long_df.dropna(subset=['Factor', 'value'])
        
        print(f"📊 Found {len(factors_long_df)} factor records")
        print("📊 Sample factor data:")
        print(factors_long_df.head())

        # Extract final score for each time bucket, normalize time_range keys
        def normalize_time_range(s):
            return str(s).replace('\u2013', '-').replace('\u2014', '-').replace('–', '-').replace('—', '-').replace(' ', '').strip()

        # Instead of using the melted DataFrame, we need to extract final scores directly from the original DataFrame
        # Find the row with "Final Score" in the original dataframe
        final_score_row = factors_df[factors_df['Factor Category'].str.strip().str.lower() == 'final score']
        
        if not final_score_row.empty:
            # Create a map of time ranges to final scores
            final_score_map = {}
            for col in time_vars:
                if col in final_score_row.columns:
                    score = final_score_row[col].iloc[0]
                    if pd.notna(score):
                        final_score_map[normalize_time_range(col)] = score
        else:
            print("⚠️ 'Final Score' row not found in the CSV")
            final_score_map = {}
            
        print('🟦 Final score map keys:', list(final_score_map.keys()))
        print('🟦 Final score map values:', list(final_score_map.values())[:5], "..." if len(final_score_map) > 5 else "")


        # --- Step 2: Load and Prepare AIS Data ---
        try:
            ais_df = pd.read_csv('AIS.csv')
            ais_df.columns = ais_df.columns.str.strip()
            
            # Convert 'Timestamp' to datetime objects
            ais_df['Timestamp'] = pd.to_datetime(ais_df['Timestamp'], dayfirst=True, errors='coerce')
            ais_df.dropna(subset=['Timestamp'], inplace=True)
            
            print(f"📊 Found {len(ais_df)} AIS records")
            print("📊 Sample AIS data:")
            print(ais_df.head())
        except FileNotFoundError:
            print("⚠️ AIS.csv not found, proceeding without AIS data")
            ais_df = pd.DataFrame()  # Empty DataFrame


        # --- Step 3: Combine Data and Structure JSON ---
        unique_time_ranges = sorted(factors_long_df['time_range'].unique())
        print(f"📊 Processing {len(unique_time_ranges)} time ranges: {unique_time_ranges}")
        
        final_json_data = []

        def format_category_name(name):
            return name.strip().lower().replace(' ', '_') if isinstance(name, str) else 'unknown'

        for time_range in unique_time_ranges:
            print(f"🕐 Processing time range: {time_range}")
            try:
                # Extract hour from time range (e.g., "0:00 - 1:00" -> 0)
                start_hour = int(time_range.split(':')[0])
            except (ValueError, IndexError):
                print(f"⚠️ Skipping invalid time range: {time_range}")
                continue

            norm_time_range = normalize_time_range(time_range)
            print(f"   Normalized time_range: {norm_time_range} | Final score: {final_score_map.get(norm_time_range, None)}")

            hourly_record = {
                "time_range": time_range,
                "external_environment_factors": {},
                "human_factors": {},
                "internal_environment_factors": {},
                "ship_factors": {},
                "ais_data": [],
                "final_score": final_score_map.get(norm_time_range, None)
            }

            # Filter AIS data for the current hour
            if not ais_df.empty:
                ais_in_hour = ais_df[ais_df['Timestamp'].dt.hour == start_hour].copy()
                # Convert Timestamp to string for JSON compatibility
                ais_in_hour['Timestamp'] = ais_in_hour['Timestamp'].astype(str)
                hourly_record['ais_data'] = ais_in_hour.to_dict(orient='records')

            # Filter factor data for the current hour
            factors_in_hour = factors_long_df[factors_long_df['time_range'] == time_range]
            print(f"   Found {len(factors_in_hour)} factors for this hour")

            for _, row in factors_in_hour.iterrows():
                category = format_category_name(row['Factor Category'])
                factor = row['Factor'].strip() if pd.notna(row['Factor']) else 'Unknown'
                value = row['value']

                if category in hourly_record:
                    hourly_record[category][factor] = value
                else:
                    print(f"⚠️ Unknown category: {category}")

            # Add the record regardless of whether it has factor data
            final_json_data.append(hourly_record)
            print(f"   Added record with {sum(len(hourly_record[cat]) for cat in hourly_record if isinstance(hourly_record[cat], dict))} factors")


        # --- Step 4: Write the JSON file ---
        output_filename = 'consolidated_ship_data.json'
        with open(output_filename, 'w') as f:
            json.dump(final_json_data, f, indent=4)
        
        print(f"✅ Successfully created '{output_filename}' with {len(final_json_data)} time periods!")
        if final_json_data:
            print(f"📊 Sample output structure for first time period:")
            first_record = final_json_data[0]
            for key, value in first_record.items():
                if isinstance(value, dict):
                    print(f"   {key}: {len(value)} items")
                elif isinstance(value, list):
                    print(f"   {key}: {len(value)} records")
                else:
                    print(f"   {key}: {value}")

    except FileNotFoundError as e:
        print(f"❌ Error: Could not find a required file.")
        print(f"Please make sure '{e.filename}' is in the same folder as the script.")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")

# Run the main function
if __name__ == "__main__":
    create_consolidated_json()