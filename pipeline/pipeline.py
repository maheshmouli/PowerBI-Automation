from handlers.file_handler import FileHandler
from handlers.data_processor import DataProcessor
from handlers.db_handler import DatabaseHandler

class Pipeline:
    def __init__(self, upload_folder='./uploads'):
        """
        Initialize the pipeline components
        """
        self.file_handler = FileHandler(upload_folder=upload_folder)
        self.data_processor = DataProcessor()
        self.db_handler = DatabaseHandler()
        
    #Added schema_name augement in function
    def process_file(self, file, schema_name, table_name):
        """
        Orchestractes the process of file handling, data validation, and database upload.

        :param file: Uploaded file object.
        :param table_name: Name of the database table to upload data to.
        :return: Result message.
        """
        try:
            # Save the uploaded file
            file_path = self.file_handler.save_file(file)
            print(f"File saved at {file_path}")

            # Validate and transform the data
            transformed_data = self.data_processor.validate_and_transform(file_path)
            print(f"Data validated and transformed successfully")

            # Connect to the database
            self.db_handler.connect()

            # Upload the new data to the database
            #Added schema_name 
            self.db_handler.upload_new_data(transformed_data, schema_name, table_name)
            print(f"Data uploaded successfully to table {table_name}.")

            return {"message": "File processed and data uploaded successfully"}
        except Exception as e:
            return {"error": str(e)}
        finally:
            self.db_handler.close()

            