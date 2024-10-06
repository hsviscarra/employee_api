# Flask Data Migration API

This Flask-based API allows users to upload CSV files containing employee data, manage batch transactions in PostgreSQL, and retrieve analytical insights on the data.

## Table of Contents
- [Project Setup](#project-setup)
- [Environment Variables](#environment-variables)
- [Uploading Files](#uploading-files)
- [API Endpoints](#api-endpoints)
- [Expected Output](#expected-output)

---

## Project Setup

1. **Create a virtual environment**:
   ```bash
   python3 -m venv venv
   source venv/bin/activate  # macOS/Linux
   venv\Scripts\activate      # Windows

2. **Install requirements**
   ```bash 
    pip install -r requirements.txt

3. **Run the Flask app**
    ```bash 
    python3 app.py

## Environment variables
To configure the PostgreSQL database connection, create a .env file in the root directory with the following content:
USER_NAME=<your_db_username>
PASSWORD=<your_db_password>
SERVERNAME=<your_db_server>
PORT=<your_db_port>
DATABASE_NAME=<your_db_name>

## Upload files
To upload CSV files, use the /upload/<table_name> endpoint. 
You can upload data to three tables: departments, jobs, and hired_employees. 
The CSV should follow the schema for each table:

- departments: ID, department
- jobs: ID, job
- hired_employees: ID, name, hire_date, id_department, id_job

1. **File Format & Restrictions**
- Accepted file format: CSV
- Chunk size: 1000 rows per batch
- Schema validation: Ensure your CSV has the correct headers for each table.

2. **Example: cURL to Upload a File**
To upload a CSV to the hired_employees table:
        ```bash 
        curl -X POST http://localhost:5000/upload/hired_employees \ -F "file=@path/to/your/file.csv"

Replace hired_employees with departments or jobs depending on which table you're uploading data to.

## API Endpoints
The Flask app could also run on Azure apps. For testing use the enpoint:
https://employeetestapp-fwa8f7gebydje9e9.eastus2-01.azurewebsites.net/

1. **Upload CSV to Table**
- Endpoint: /upload/<table_name>
- Method: POST
- Description: Uploads CSV data to the specified table (departments, jobs, hired_employees).
- Payload: A CSV file.
- Response:
    - Success: {"message": "Data uploaded successfully to <table_name>"} (HTTP 200)
    - Error: {"error": "<error_message>"} (HTTP 400/500)

2. **Get Hired Employees by Quarter**
- Endpoint: /hired_employees_by_quarter
- Method: GET
- Description: Retrieves the number of hired employees by department and job for each quarter of 2021.

3. **Get Departments with Hires Above Average**
- Endpoint: /hired_employees_over_mean
- Method: GET
- Description: Retrieves departments that hired more employees than the average in 2021.