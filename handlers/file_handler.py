import os

class FileHandler:
    def __init__(self, upload_folder='./uploads'):
        """
        Initialize the file handler with the upload folder path.
        Creates the folder if it doesn't exist.
        """
        self.upload_folder = upload_folder
        os.makedirs(self.upload_folder, exist_ok=True)

    def save_file(self, file):
        """
        Save the uploaded file to the server's upload folder.

        :param file: File object form the HTTP request
        :return: File path where the file is saved
        :raises ValueError: If the file format is invalid
        """
        if not file.filename.endswith(('csv', 'xlsx')):
            raise ValueError("Invalid file format, Only CSV and Excel files are allowed")
        
        file_path = os.path.join(self.upload_folder, file.filename)
        file.save(file_path)
        return file_path