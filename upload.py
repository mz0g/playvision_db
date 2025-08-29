from supabase import create_client, Client
import pandas as pd
from dotenv import load_dotenv
import os
import numpy as np
from tqdm import tqdm

# Your Supabase project URL + anon/service key
load_dotenv()
url = os.getenv("SUPABASE_URL")
key = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
supabase: Client = create_client(url, key)

def clean_dataframe(df, table_name):
    """Clean dataframe to make it JSON compliant"""

    # Ensure percentages are divided by 100
    if table_name == "player_info":
        columns_to_divide = ["orb_pct", "drb_pct", "trb_pct", "stl_pct", "blk_pct", "min_pct", "ast_pct", "tov_pct", "usg_pct", "total_s_pct", "ftr"]
        # Use vectorized operations to divide all columns at once
        existing_columns = [col for col in columns_to_divide if col in df.columns]
        if existing_columns:
            df[existing_columns] = df[existing_columns] / 100
            
    # Replace infinite values with None
    df = df.replace([np.inf, -np.inf], None)
    
    # Replace NaN values with None
    df = df.replace([np.nan], None)
    
    # Handle specific table data type issues
    if table_name == "players":
        # Convert weight column to integer (remove .0 and convert to int)
        if 'weight' in df.columns:
            # First replace None/NaN with empty string, then remove .0, then convert to int
            df['weight'] = df['weight'].fillna('').astype(str).str.replace('.0', '').str.replace('nan', '')
            # Convert empty strings to None, then convert valid numbers to int
            df['weight'] = df['weight'].replace('', None)
            # Only convert non-None values to int
            df['weight'] = df['weight'].apply(lambda x: int(x) if x is not None and x != '' else None)

    if "zscore" in table_name:
        df = df.drop(columns=['conference_id', 'team_id'])
    
    int_columns = ['number', 'age', 'height_inches', 'weight', 'gp']
    for col in int_columns:
        if col in df.columns:
            df[col] = df[col].astype('Int64') 
    
    return df

def upload_table(table_name, csv_file, key_column=None, composite_keys=None, batch_size=1000, max_retries=3):
    
    print(f"🔄 Processing {table_name}...")
    
    # Load CSV
    df = pd.read_csv(os.path.join(os.getenv("CSV_FOLDER"), csv_file))
    print(f"   Loaded {len(df)} rows from {csv_file}")
    
    # Clean the data to make it JSON compliant
    df = clean_dataframe(df, table_name)
    
    # Insert data in batches
    total_rows = len(df)
    print(f"   Uploading {total_rows} rows in batches of {batch_size}...")
    
    for i in tqdm(range(0, total_rows, batch_size), desc=f"Uploading {table_name}"):
        batch_end = min(i + batch_size, total_rows)
        batch_df = df.iloc[i:batch_end]
        batch_data = batch_df.to_dict(orient="records")
        
        # Retry logic for network errors
        for attempt in range(max_retries):
            try:
                supabase.table(table_name).insert(batch_data).execute()
                break  # Success, exit retry loop
            except Exception as e:
                if "502" in str(e) or "Bad Gateway" in str(e) or "cloudflare" in str(e):
                    if attempt < max_retries - 1:
                        print(f"   ⚠️  Network error (attempt {attempt + 1}/{max_retries}), retrying in 5 seconds...")
                        import time
                        time.sleep(5)
                        continue
                    else:
                        print(f"   ❌ Network error after {max_retries} attempts: {e}")
                        raise e
                else:
                    print(f"   ❌ Error uploading batch {i//batch_size + 1}: {e}")
                    raise e
    
    print(f"✅ Successfully uploaded {total_rows} rows to {table_name}")
    return total_rows

def delete_all_tables():
    """Delete all tables in dependency order to avoid foreign key constraint violations"""
    print("🗑️  Deleting all existing tables to avoid foreign key constraints...")
    
    # Delete tables in dependency order (child tables first, then parent tables)
    # Tables with foreign key references must be deleted before their parent tables
    tables_to_delete = [
        "scores",                        # References players
        "player_zscores",                # References players
        "player_zscores_role",           # References players
        "player_zscores_role_season",    # References players
        "player_zscores_season",         # References players
        "player_info",                   # References players and teams
        "team_info",                     # References teams
        "players",                       # Parent table
        "teams",                         # Parent table  
        "conferences"                    # Parent table
    ]
    
    for table_name in tables_to_delete:
        try:
            print(f"   Deleting {table_name}...")
            
            # Try to delete all rows from the table
            # For Supabase, we need to use a WHERE clause, so use a condition that's always true
            if table_name in ["players", "teams", "conferences"]:
                # For master tables, delete with a condition that's always true
                # Use a valid column that exists in these tables
                if table_name == "players":
                    supabase.table(table_name).delete().gte("player_id", 0).execute()
                elif table_name == "teams":
                    supabase.table(table_name).delete().gte("team_id", 0).execute()
                elif table_name == "conferences":
                    supabase.table(table_name).delete().gte("conference_id", 0).execute()
            else:
                # For other tables, try to delete with a common column
                try:
                    # Try with player_id if it exists
                    supabase.table(table_name).delete().gte("player_id", 0).execute()
                except:
                    try:
                        # Try with team_id if it exists
                        supabase.table(table_name).delete().gte("team_id", 0).execute()
                    except:
                        # Fallback: delete with a condition that's always true
                        # Use a valid column that should exist in most tables
                        supabase.table(table_name).delete().gte("player_id", 0).execute()
            
            print(f"   ✅ {table_name} cleared")
                
        except Exception as e:
            if "does not exist" in str(e) or "42P01" in str(e):
                print(f"   ℹ️  Table {table_name} does not exist, skipping...")
            else:
                print(f"   ⚠️  Could not clear {table_name}: {e}")
            # Continue with other tables even if one fails
    
    print("✅ All tables cleared, ready for fresh upload")

def main():
    """Main upload function for all tables"""
    print("🚀 Starting database upload process...")
    
    # First, delete all existing tables to avoid foreign key constraints
    delete_all_tables()
    
    # Define table configurations
    tables = [
        {
            "table_name": "conferences",
            "csv_file": "conferences.csv",
            "key_column": "conference_id",
            "batch_size": 1000
        },
        {
            "table_name": "teams", 
            "csv_file": "teams.csv",
            "key_column": "team_id",
            "batch_size": 1000
        },
        { 
            "table_name": "players",
            "csv_file": "players.csv", 
            "key_column": "player_id",
            "batch_size": 1000
        },
        {
            "table_name": "team_info",
            "csv_file": "team_info.csv",
            "composite_keys": ["team_id", "season"],
            "batch_size": 500
        },
        {
            "table_name": "player_info", 
            "csv_file": "player_info.csv",
            "composite_keys": ["player_id", "season"],
            "batch_size": 250
        },
        {
            "table_name": "player_zscores",
            "csv_file": "player_zscores.csv",
            "composite_keys": ["player_id", "season"],
            "batch_size": 250
        },
        {
            "table_name": "player_zscores_season",
            "csv_file": "player_zscores_season.csv",
            "composite_keys": ["player_id", "season"],
            "batch_size": 250
        },
        {
            "table_name": "player_zscores_role",
            "csv_file": "player_zscores_role.csv",
            "composite_keys": ["player_id", "role", "season"],
            "batch_size": 250
        },
        {
            "table_name": "player_zscores_role_season",
            "csv_file": "player_zscores_role_season.csv",
            "composite_keys": ["player_id", "role", "season"],
            "batch_size": 250
        },
        {
            "table_name": "scores",
            "csv_file": "scores.csv",
            "composite_keys": ["player_id", "season"],
            "batch_size": 250
        }
    ]
    
    total_rows = 0
    
    # Upload each table
    for table_config in tables:
        try:
            rows = upload_table(
                table_name=table_config["table_name"],
                csv_file=table_config["csv_file"],
                key_column=table_config.get("key_column"),
                composite_keys=table_config.get("composite_keys"),
                batch_size=table_config.get("batch_size", 1000)  # Default to 1000 if not specified
            )
            total_rows += rows
        except Exception as e:
            print(f"❌ Error uploading {table_config['table_name']}: {e}")
            continue
    
    print(f"\n🎉 Upload complete! Total rows processed: {total_rows}")

if __name__ == "__main__":
    main()
