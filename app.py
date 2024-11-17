from flask import Flask, request

app = Flask(__name__)

# Database configurations
db_config = {
    "host":"localhost",
    "user":"your_username",
    "password":"password",
    "database":"database_name"

}

# Initialize the pipeline
pipeline = Pipeline(db_config)

@app.route('/process', methods=['POST'])
def process_file():
    file = request.files['file']
    if not file:
        return {"error":"No file uploaded"}, 400
    
    table_name = "table_name"
    result = pipeline.process(file, table_name)
    if "error" in result:
        return result, 500
    return result, 200

if __name__=="__main__":
    app.run(debug=True)