from flask import Flask, request, jsonify
from pipeline.pipeline import Pipeline

app = Flask(__name__)

pipeline = Pipeline(upload_folder="./uploads")

TABLE_NAME = "apartment_data"
SCHEMA_NAME = "Builders_data"

@app.route("/upload", methods=["POST"])
def upload_file():
    """
    API endpoint to handle file uploads and process them
    """
    try:
        if 'file' not in request.files:
            return jsonify({"error":"No file provided"}), 400
        
        file = request.files['file']
        if not file:
            return jsonify({"error": "File is empty"}), 400
        
        #Added schema_name below
        result = pipeline.process_file(file, SCHEMA_NAME, TABLE_NAME)

        if "error" in result:
            return jsonify(result), 500
        
        return jsonify(result), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    

if __name__=="__main__":
    app.run(debug=True)
    
