import pandas as pd
import os
from datetime import datetime

class DumpFileValidator:
    def __init__(self):
        # Initialize variables
        self.window_start = pd.Timestamp('2024-12-02 19:00:00.000000+00:00')
        self.window_end = pd.Timestamp('2024-12-03 19:00:00.000000+00:00')
        self.yyyymmdd = '20241204'
        self.database_names = ['online', 'offline']
        self.environment = ['PRD', 'QA', 'DEV']
        self.etl_mode = {'delta': 'daily', 'full': 'full'}
        
    def check_manifest_headers(self, df):
        required_headers = ['table_name', 'row_count', 'time_start', 'time_end']
        return all(header in df.columns for header in required_headers)
    
    def check_id_column(self, df):
        id_columns = ['ID', 'Id', 'DataID', 'ProductOptionDataID']
        found_cols = [col for col in id_columns if col in df.columns]
        return len(found_cols) > 0, found_cols[0] if found_cols else None

    def validate_daily_file(self, raw_df, metadata_row):
        validation_results = []
        
        # 4a. Check row count
        if len(raw_df) != metadata_row['row_count']:
            validation_results.append(f"Row count mismatch: expected {metadata_row['row_count']}, got {len(raw_df)}")
        
        # 4b. Check ID column
        has_id, id_col = self.check_id_column(raw_df)
        if not has_id:
            validation_results.append("No valid ID column found")
        
        # 4c. Check IsCreated values
        if 'IsCreated' in raw_df.columns and 'DateCreated' in raw_df.columns:
            created_mask = raw_df['DateCreated'] >= self.window_start
            invalid_created = raw_df[created_mask & (raw_df['IsCreated'] != 1)]
            if not invalid_created.empty:
                validation_results.append(f"Found {len(invalid_created)} invalid IsCreated values")
        
        # 4d. Check IsModified values
        if 'IsModified' in raw_df.columns and 'DateModified' in raw_df.columns:
            modified_mask = raw_df['DateModified'] >= self.window_start
            invalid_modified = raw_df[modified_mask & (raw_df['IsModified'] != 1)]
            if not invalid_modified.empty:
                validation_results.append(f"Found {len(invalid_modified)} invalid IsModified values")
        
        # 4e. Check date criteria
        if 'DateCreated' in raw_df.columns and 'DateModified' in raw_df.columns:
            date_criteria = (
                ((raw_df['DateCreated'] >= self.window_start) & 
                 (raw_df['DateCreated'] < self.window_end)) |
                ((raw_df['DateModified'] >= self.window_start) & 
                 (raw_df['DateModified'] < self.window_end))
            )
            invalid_dates = raw_df[~date_criteria]
            if not invalid_dates.empty:
                validation_results.append(f"Found {len(invalid_dates)} records outside time window")
        
        return len(validation_results) == 0, validation_results

    def validate_full_file(self, raw_df, metadata_row):
        validation_results = []
        
        # 4a. Check row count
        if len(raw_df) != metadata_row['row_count']:
            validation_results.append(f"Row count mismatch: expected {metadata_row['row_count']}, got {len(raw_df)}")
        
        # 4b. Check ID column
        has_id, id_col = self.check_id_column(raw_df)
        if not has_id:
            validation_results.append("No valid ID column found")
        
        # 4c. Check IsCreated values
        if 'IsCreated' in raw_df.columns:
            invalid_created = raw_df[raw_df['IsCreated'] != 1]
            if not invalid_created.empty:
                validation_results.append(f"Found {len(invalid_created)} rows with IsCreated != 1")
        
        # 4d. Check IsModified values
        if 'IsModified' in raw_df.columns:
            invalid_modified = raw_df[raw_df['IsModified'] != 0]
            if not invalid_modified.empty:
                validation_results.append(f"Found {len(invalid_modified)} rows with IsModified != 0")
        
        return len(validation_results) == 0, validation_results

    def compare_validation_files(self, raw_df, validation_df):
        # Check if validation file is empty, meaning no changes
        if len(validation_df.index) == 0:
            return True, "Validation file is empty, no changes made"
        
        # Check if validation and raw file only have 1 row
        if len(raw_df.index) == 1 and len(validation_df.index) == 1:
            single_row_match = raw_df.iloc[0].equals(validation_df.iloc[0])

            if single_row_match:
                return True, "Validation check passed"
            else:
                return False, "Validation and raw row mismatch"
        
        # Check that validation file has exactly 2 data rows (excluding header)
        if len(validation_df.index) != 2:
            return False, f"Validation file should contain exactly 2 data rows (first and last), found {len(validation_df.index)} rows"
        
        # Compare first row of raw file with last row of validation file
        first_row_match = raw_df.iloc[0].equals(validation_df.iloc[1])
        last_row_match = raw_df.iloc[-1].equals(validation_df.iloc[0])
        
        if not (first_row_match or last_row_match):
            # debugging
            if not first_row_match:
                print("\nFirst row mismatch:")
                print("Raw file first row:")
                print(raw_df.iloc[0])
                print("\nValidation file first row:")
                print(validation_df.iloc[0])
            
            if not last_row_match:
                print("\nLast row mismatch:")
                print("Raw file last row:")
                print(raw_df.iloc[-1])
                print("\nValidation file second row:")
                print(validation_df.iloc[1])

            if not single_row_match:
                print("\nSingle row mismatch:")
                print("Raw file single row:")
                print(raw_df.iloc[0])
                print("\nValidation file single row:")
                print(validation_df.iloc[0])
            
            return False, "First and/or last rows don't match between raw and validation files"
        
        return True, "Validation file check passed"

    def process_files(self, base_path, etl_mode):
        mode = self.etl_mode[etl_mode]
        
        for database in self.database_names:
            print(f"\n========== PROCESSING {database} DATABASE ==========")
            
            # Read manifest file
            manifest_path = f"{base_path}/{mode}/{self.yyyymmdd}_jti_vita-ploom-{database}_Manifest.csv"
            if not os.path.exists(manifest_path):
                print(f"Manifest file not found: {manifest_path}")
                continue
            
            manifest_df = pd.read_csv(manifest_path)
            if not self.check_manifest_headers(manifest_df):
                print(f"Invalid manifest headers for {database}")
                continue
            
            # Process each table
            for _, row in manifest_df.iterrows():
                table_name = row['table_name']
                print(f"\nValidating table: {table_name} - {database}")
                
                # Process raw file
                raw_file = f"{base_path}/{mode}/{self.yyyymmdd}_jti_vita-ploom-{database}_{table_name}_raw.csv"
                if not os.path.exists(raw_file):
                    print(f"Raw file not found: {raw_file}")
                    continue
                
                # Read CSV with date parsing
                try:
                    raw_df = pd.read_csv(raw_file)
                    # Convert date columns to datetime
                    date_columns = ['DateCreated', 'DateModified']
                    for col in date_columns:
                        if col in raw_df.columns:
                            raw_df[col] = pd.to_datetime(raw_df[col], utc=True)
                    
                    # Validate based on etlmode
                    if mode == 'daily':
                        is_valid, messages = self.validate_daily_file(raw_df, row)
                    else:
                        is_valid, messages = self.validate_full_file(raw_df, row)
                    
                    print(f"Raw file validation: {'PASSED' if is_valid else 'FAILED'}")
                    if not is_valid:
                        for msg in messages:
                            print(f"- {msg}")
                    
                    # Check validation file
                    validation_file = f"{base_path}/{mode}/{self.yyyymmdd}_jti_vita-ploom-{database}_{table_name}_validation.csv"
                    if os.path.exists(validation_file):
                        try:
                            # Read validation file with the same date parsing as raw file
                            validation_df = pd.read_csv(validation_file)
                            date_columns = ['DateCreated', 'DateModified']
                            for col in date_columns:
                                if col in validation_df.columns:
                                    validation_df[col] = pd.to_datetime(validation_df[col], utc=True)
                                    
                            is_valid, message = self.compare_validation_files(raw_df, validation_df)
                            print(f"Validation file check: {message}")
                        except Exception as e:
                            print(f"Error processing validation file: {str(e)}")
                    else:
                        print(f"Validation file not found: {validation_file}")
                        
                except Exception as e:
                    print(f"Error processing {table_name}: {str(e)}")
                    continue

def main():
    validator = DumpFileValidator()
    base_path = "./20241204"
    
    # Process delta (daily) files
    print("\n********** PROCESSING DELTA (DAILY) FILES **********")
    validator.process_files(base_path, 'delta')
    
    # Process full files
    print("\n********** PROCESSING FULL FILES **********")
    validator.process_files(base_path, 'full')

if __name__ == "__main__":
    main()