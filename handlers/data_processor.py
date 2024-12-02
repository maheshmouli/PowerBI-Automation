import pandas as pd

class DataProcessor:
    @staticmethod
    def validate_and_transform(file_path):
        """
        Validates and transforms the dataset.

        :param file_path: Path to the CSV or Excel file.
        :return: Transformed DataFrame
        :raises ValueError: If the validation fails
        """
        try:
            # Step 1: Load the file
            if file_path.endswith('.csv'):
                data = pd.read_csv(file_path)
            elif file_path.endswith('.xlsx'):
                data = pd.read_excel(file_path)
            else:
                raise ValueError("Unsupported file format. Only CSV and Excel files are allowed.")

            # Step 2: Validate required columns
            required_columns = ['TOWER', 'FLOOR', 'APARTMENT NO.', 'AREA', 'FACING', 'CORNER', 'TYPE']
            missing_columns = [col for col in required_columns if col not in data.columns]

            if missing_columns:
                raise ValueError(f"Missing required columns: {missing_columns}")

            # Step 3: Standardize column names to match database schema
            data.columns = [col.strip().lower().replace(" ", "_").rstrip(".") for col in data.columns]
            print(f"Transformed columns: {data.columns}")

            # Step 4: Validate numeric columns
            data['area'] = pd.to_numeric(data['area'], errors='coerce')
            data['floor'] = pd.to_numeric(data['floor'], errors='coerce')
            data['apartment_no'] = pd.to_numeric(data['apartment_no'], errors='coerce')

            if data[['area', 'floor', 'apartment_no']].isnull().any().any():
                raise ValueError("Invalid or missing values in numeric columns: area, floor, apartment_no.")

            # Step 5: Validate ranges (e.g., area and floor)
            if not data['area'].between(500, 5000).all():
                raise ValueError("area values must be between 500 and 5000.")

            if not data['floor'].between(1, 100).all():  # Adjust based on max floors
                raise ValueError("floor values must be between 1 and 100.")

            # Step 6: Validate categorical/string values
            valid_facing = ['EAST', 'WEST', 'NORTH', 'SOUTH']
            if not data['facing'].str.upper().isin(valid_facing).all():
                raise ValueError(f"Invalid facing values. Allowed values: {valid_facing}")

            valid_corner = ['YES', 'NO']
            if not data['corner'].str.upper().isin(valid_corner).all():
                raise ValueError(f"Invalid corner values. Allowed values: {valid_corner}")

            valid_types = ['3 BHK', '4 BHK']
            if not data['type'].str.upper().isin(valid_types).all():
                raise ValueError(f"Invalid type values. Allowed values: {valid_types}")

            # Step 7: Remove duplicates
            if data.duplicated(subset=['tower', 'floor', 'apartment_no']).any():
                raise ValueError("Duplicate entries detected for tower, floor, and apartment_no.")

            # Step 8: Handle missing values
            data['corner'] = data['corner'].fillna('NO')

            # Step 9: Clean strings (trim spaces and standardize case)
            for col in ['tower', 'facing', 'corner', 'type']:
                data[col] = data[col].str.strip().str.upper()

            return data

        except Exception as e:
            raise ValueError(f"Validation/Transformation Error: {e}")
