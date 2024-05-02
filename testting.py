from flask import Flask, jsonify, request
import pymysql
import os
import pandas as pd
from dotenv import load_dotenv
from flask_cors import CORS

app = Flask(__name__)
CORS(app)  # Enable CORS for the entire app

load_dotenv()

app.config['DB_HOST'] = os.getenv('DB_HOST')
app.config['DB_USER'] = os.getenv('DB_USER')
app.config['DB_PASSWORD'] = os.getenv('DB_PASSWORD')
app.config['DB_NAME'] = os.getenv('DB_NAME')
app.config['DB_PORT'] = int(os.getenv('DB_PORT'))


def create_connection():
    return pymysql.connect(host=app.config['DB_HOST'], user=app.config['DB_USER'], password=app.config['DB_PASSWORD'],
                           database=app.config['DB_NAME'], port=app.config['DB_PORT'])


@app.route('/perform_search', methods=['GET'])
def search_results():
    connection = create_connection()
    to_search = request.form.get('search_input')
    try:
        with connection.cursor() as cursor:
            cursor.execute(f"""SELECT * FROM search WHERE search.state !='Deleted' AND search.query 
            LIKE '%{to_search}%' ORDER BY search.updated""")
            search_results_data = cursor.fetchall()
            search_dataframe = pd.DataFrame(search_results_data, columns=[x[0] for x in cursor.description])
            return search_dataframe.to_json(orient="records")
    except Exception as e:
        return jsonify({'error': str(e)})
    finally:
        connection.close()


@app.route('/single_remove_search', methods=['POST'])
def remove_searched_entry():
    connection = create_connection()
    to_remove = request.form.get('single_remove_search')
    connection.autocommit(True)
    try:
        with connection.cursor() as cursor:
            cursor.execute(f"""SET @search_id:=(SELECT search.id FROM search WHERE search.query={to_remove} 
            AND search.state!='Deleted')""")
            cursor.execute("UPDATE search SET state = 'Deleted' WHERE search.id=@search_id")
            total_row_count = cursor.rowcount
            if total_row_count > 0:
                return jsonify({'task': 'successful'})
            else:
                return jsonify({'task': 'nothing to update'})
    except Exception as e:
        return jsonify({'error': str(e)})
    finally:
        connection.close()


if __name__ == '__main__':
    app.run(debug=True)
    # app.run(host='0.0.0.0', port=6061)
