import pandas as pd

class DataProcessor:
    @staticmethod
    def validate_and_transform(file_path):
        """
        Validates and transform the dataset.

        :param file_path: Path to the CSV or Excel file.
        :return: Transformed Dataframe
        :raises ValueError: If the validation fails
        """
        try:
            # Load the file
            if file_path.endswith('.csv'):
                data = pd.read_csv(file_path)
            elif file_path.endswith('.xlsx'):
                data = pd.read_excel(file_path)
            else:
                raise ValueError("Unsupported file format. Only CSV and Excel files are allowed")
            
            # Validate required columns
            required_columns = ['TOWER', 'FLOOR', 'APARTMENT NO.', 'AREA', 'FACING', 'CORNER', 'TYPE']
            missing_columns = [col for col in required_columns if col not in data.columns]

            if missing_columns:
                raise ValueError(f"Missing required columns: {missing_columns}")
            
            # Transformation: Standardize column names
            data.columns = [col.strip().upper() for col in data.columns]

            # Validate Data Types
            data['AREA'] = pd.to_numeric(data['AREA'], errors='coerce')
            data['FLOOR'] = pd.to_numeric(data['FLOOR'], errors='coerce')
            data['APARTMENT No.'] = pd.to_numeric(data['APARTMENT NO.'], errors='coerce')

            if data[['AREA', 'FLOOR', 'APARTMENT No.']].isnull().any().any():
                raise ValueError("Invalid or missing values in numeric columns: AREA, FLOOR, APARTMENT NO.")
            
            # Validate Ranges (Sqft)
            if not data['AREA'].between(500, 5000).all():
                raise ValueError("AREA values must be between 500 and 5000.")

            if not data['FLOOR'].between(1, 100).all():  # Adjust based on max floors
                raise ValueError("FLOOR values must be between 1 and 100.")

            # Validate String Values
            valid_facing = ['EAST', 'WEST', 'NORTH', 'SOUTH']
            if not data['FACING'].str.upper().isin(valid_facing).all():
                raise ValueError(f"Invalid FACING values. Allowed values: {valid_facing}")

            valid_corner = ['YES', 'NO']
            if not data['CORNER'].str.upper().isin(valid_corner).all():
                raise ValueError(f"Invalid CORNER values. Allowed values: {valid_corner}")

            valid_types = ['3 BHK', '4 BHK']
            if not data['TYPE'].str.upper().isin(valid_types).all():
                raise ValueError(f"Invalid TYPE values. Allowed values: {valid_types}")

            # Remove Duplicates
            if data.duplicated(subset=['TOWER', 'FLOOR', 'APARTMENT NO.']).any():
                raise ValueError("Duplicate entries detected for TOWER, FLOOR, and APARTMENT NO.")

            # Handle Missing Values
            data['CORNER'] = data['CORNER'].fillna('NO')

            # Add Derived Columns 
            data['APARTMENT_TYPE'] = data['TYPE'].apply(lambda x: "Luxury" if "4 BHK" in x else "Standard")

            # Clean Strings (Trim spaces and standardize case)
            for col in ['TOWER', 'FACING', 'CORNER', 'TYPE']:
                data[col] = data[col].str.strip().str.upper()

            return data

        except Exception as e:
            raise ValueError(f"Validation/Transformation Error: {e}")            
            

