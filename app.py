from dotenv import load_dotenv
from flask import Flask, request, jsonify
from sqlalchemy import create_engine, text
from flask import render_template
import pandas as pd
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import SQLAlchemyError
import math
import os

app = Flask(__name__)

load_dotenv()

# Define your PostgreSQL database URI here
USER_NAME=os.getenv("USER_NAME")
PASSWORD=os.getenv("PASSWORD")
SERVERNAME=os.getenv("SERVERNAME")
PORT=os.getenv("PORT")
DATABASE_NAME=os.getenv("DATABASE_NAME")

print(USER_NAME, PASSWORD, SERVERNAME, PORT, DATABASE_NAME)

DATABASE_URI = f'postgresql://{USER_NAME}:{PASSWORD}@{SERVERNAME}:{PORT}/{DATABASE_NAME}'
print(DATABASE_URI)
engine = create_engine(DATABASE_URI)
Session = sessionmaker(bind=engine)

def create_tables(engine):
    with engine.connect() as connection:
        # Create the hired_employees table
        connection.execute(text("""
        CREATE TABLE IF NOT EXISTS departments (
            ID INTEGER PRIMARY KEY,
            department TEXT NOT NULL
        )
        """))

        connection.execute(text("""
        CREATE TABLE IF NOT EXISTS jobs (
            ID INTEGER PRIMARY KEY,
            job TEXT NOT NULL
        )
        """))


        connection.execute(text("""
        CREATE TABLE IF NOT EXISTS hired_employees (
            ID INTEGER PRIMARY KEY,
            name TEXT,
            hire_date TIMESTAMP,
            id_department INTEGER,
            id_job INTEGER
        )
        """))

create_tables(engine)

def chunck_dataframe(df, chunk_size):
    num_chunks = math.ceil(len(df) / chunk_size)
    return [df[i*chunk_size:(i+1) *chunk_size] for i in range(num_chunks)]

def upload_csv_to_table(file, table_name, column_names, chunk_size=1000):
    try:
        # Read the CSV file into a Dataframe
        df = pd.read_csv(file, header=None)
        df.columns = column_names

        # Split Dataframe in chunks
        chunks = chunck_dataframe(df, chunk_size)

        # Insert each chunk into the database
        for chunk in chunks:
            chunk.to_sql(table_name, con=engine, if_exists='append', index=False)
        return jsonify({"message": f"Data uploaded succesfully to {table_name}"}), 200
    
    except Exception as e:
        print(str(e))
        return jsonify({"error": str(e)}), 500

@app.route('/')
def home():
    return 'Bienvenido a la app de migración de datos!'

@app.route('/upload/<table_name>', methods=['POST'])
def upload_csv(table_name):
    if 'file' not in request.files:
        return jsonify({"error": "No file part"}), 400

    file = request.files['file']
    if file.filename == '':
        return jsonify({"error": "No selected file"}), 400

    if file and file.filename.endswith('.csv'):
        schema = {
            'departments': ['ID', 'department'],
            'jobs': ['ID', 'job'],
            'hired_employees': ['ID', 'name', 'hire_date', 'id_department', 'id_job']
        }
        if table_name not in schema:
            return jsonify({"error": f"Unknown table: {table_name}"}), 400
        
        return upload_csv_to_table(file, table_name, schema[table_name], chunk_size=1000)
    else:
        return jsonify({"error": "Invalid file format. Please upload a CSV file."}), 400
        

@app.route('/hired_employees_by_quarter', methods=['GET'])
def hired_employees_by_quarter():
    session = Session()
    try:
        query = text("""
        SELECT d.department, 
                j.job,
                EXTRACT(QUARTER FROM he.hire_date::timestamp) AS quarter,
                COUNT(he."ID") AS num_hired
        FROM hired_employees he
        JOIN departments d ON he.id_department = d."ID"
        JOIN jobs j ON he.id_job = j."ID"
        WHERE he.hire_date >= '2021-01-01' AND he.hire_date < '2022-01-01'
        GROUP BY d.department, j.job, quarter
        ORDER BY d.department, j.job, quarter;
        """)

        result = session.execute(query).fetchall()

        data = [
            {
                "department": row[0],
                "job": row[1],
                "quarter": int(row[2]),
                "num_hired": row[3]
            }
            for row in result
        ]

        #return jsonify(data), 200
        return render_template('hired_employees_by_quarter.html', data=data)

    except SQLAlchemyError as e:
        print(f"Error occurred: {str(e)}")
        return jsonify({"error": str(e)}), 500

    finally:
        session.close()
        

@app.route('/hired_employees_over_mean', methods=['GET'])
def hired_employees_over_mean():
    session = Session()
    try:
        query = text("""
        
        WITH department_hires AS (
        SELECT d."ID" AS department_id, 
               d.department,
               COUNT(he."ID") as num_hired
        FROM hired_employees he
        JOIN departments d ON he.id_department = d."ID"
        WHERE he.hire_date >= '2021-01-01' AND he.hire_date< '2022-01-01'
        GROUP BY d."ID", d.department
        ), mean_hires AS (
                SELECT AVG(num_hired) AS mean 
                FROM department_hires
        )
        SELECT dh.department_id, dh.department, dh.num_hired
        FROM department_hires as dh
        JOIN mean_hires mh ON dh.num_hired > mh.mean
        ORDER BY dh.num_hired DESC;
        """)

        result = session.execute(query).fetchall()

        data = [
            {
                "ID": row[0],
                "department": row[1],
                "hired": int(row[2])
            }
            for row in result
        ]

        return jsonify(data), 200

    except SQLAlchemyError as e:
        print(f"Error occurred: {str(e)}")
        return jsonify({"error": str(e)}), 500

    finally:
        session.close()


@app.route('/favicon.ico')
def favicon():
    return '', 204  # No content

if __name__ == '__main__':
    app.run(debug=True)


