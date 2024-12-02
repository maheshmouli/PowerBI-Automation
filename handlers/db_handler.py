import psycopg2
from psycopg2 import sql, extras
import pandas as pd
from config.db_config import DB_CONFIG

class DatabaseHandler:
    def __init__(self) -> None:
        """
        Initialize the DatabaseHandler using the database configuration from db_config.py.
        """
        self.connection = None
        self.db_config = DB_CONFIG

    def connect(self):
        """
        Establish a connection to the database.
        :rasie ValueError: If the connection fails.
        """
        try:
            self.connection = psycopg2.connect(
                host = self.db_config["host"],
                user = self.db_config["user"],
                password = self.db_config["password"],
                database = self.db_config["database"]
            )
            
            print("Database connection successful")
        except psycopg2.Error as e:
            raise ValueError(f"Database connection Error: {e}")
        
    def close(self):
        """
        Close the database connection.
        """
        if self.connection:
            self.connection.close()
            print("Database connection closed")

    def fetch_existing_data(self, table_name):
        """
        Fetch existing data from the specified table.
        :param table_name: Name of the database table.
        :return: A pandas DataFrame containing the exisitng data.
        """
        if self.connection is None:
            raise ValueError("No active database connection")
        
        cursor = self.connection.cursor(cursor_factory=extras.DictCursor)
        try:
            query = sql.SQL("SELECT * FROM {table_name}").format(table_name=sql.Identifier(table_name))
            cursor.execute(query)
            result = cursor.fetchall()

            columns = [desc[0] for desc in cursor.description]
            return pd.DataFrame(result, columns=columns)
        except psycopg2.Error as e:
            raise ValueError(f"Error fetcing data from {table_name}: {e}")
        finally:
            cursor.close()
        
    def upload_new_data(self, new_data, table_name):
        """
        Upload only new data to the database table by comparing it with existing data.

        :param new_data: Pandas DataFrame containing new data.
        :param table_name: Name of the target database table.
        :raise ValueError: If the upload fails.
        """
        if self.connection is None:
            raise ValueError("No active database connection")
        
        existing_data = self.fetch_existing_data(table_name)

        if not existing_data.empty:
            combined_data = pd.concat([existing_data, new_data], ignore_index=True)
            unique_data = combined_data.drop_duplicates(keep=False, ignore_index=True)
        else:
            unique_data = new_data

        if unique_data.empty:
            print("No new data to upload")
            return

        cursor = self.connection.cursor()
        try:
            columns = ", ".join(unique_data.columns)
            placeholders = ", ".join(["%s"] * len(unique_data.columns))
            query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
            records = [tuple(row) for row in unique_data.to_numpy()]

            extras.execute_batch(cursor, query, records)
            self.connection.commit()
            print(f"{cursor.rowcount} new rows inserted into {table_name}.")
        except psycopg2.Error as e:
            self.connection.rollback()
            raise ValueError(f"Error inserting new data into {table_name}: {e}")
        finally:
            cursor.close()
            