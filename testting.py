from flask import Flask, jsonify, request
import pymysql
import os
import pandas as pd
from dotenv import load_dotenv
from flask_cors import CORS
import logging

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
    to_search = request.args.get('search_input')
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


@app.route('/remove_search', methods=['POST'])
def remove_searched_entry():
    connection = create_connection()
    search_id = request.json.get('id')  # Get the id from the request body
    connection.autocommit(True)
    try:
        with connection.cursor() as cursor:
            cursor.execute(f"""UPDATE search SET state = 'Deleted' 
                            WHERE id = %s""", (search_id,))
            total_row_count = cursor.rowcount
            if total_row_count > 0:
                return jsonify({'success': True, 'message': 'Search removed successfully'})
            else:
                return jsonify({'success': False, 'message': 'Nothing to remove'})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})
    finally:
        connection.close()


@app.route('/multiple_remove_search', methods=['POST'])
def remove_multiple_searched_entry():
    connection = create_connection()
    remove_search = request.form.get('multiple_remove_search')
    remove_data = remove_search.replace(",", "','")
    remove_search_data = f"""'{remove_data}'"""
    connection.autocommit(True)
    try:
        with connection.cursor() as cursor:
            cursor.execute(f"""UPDATE search SET search.state = 'Deleted' WHERE search.id IN ({remove_search_data});""")
            total_row_count = cursor.rowcount
            if total_row_count > 0:
                return jsonify({'task': 'successful'})
            else:
                return jsonify({'task': 'nothing to update'})
    except Exception as e:
        return jsonify({'error': str(e)})
    finally:
        connection.close()


@app.route('/insert_single_row', methods=['POST'])
def insert_single_entry():
    connection = create_connection()
    data = request.get_json()
    row_insert_question = data.get('single_row_insert_question', '')
    row_insert_result = data.get('single_row_insert_result', '')
    connection.autocommit(True)

    try:
        with connection.cursor() as cursor:
            insert_search_query = "INSERT INTO search (query, result, state) VALUES (%s, %s, 'Added')"
            insert_search_history_query = "INSERT INTO search_history_new (query, results, state) VALUES (%s, %s, 'Added')"
            cursor.execute(insert_search_query, (row_insert_question, row_insert_result))
            search_inserted = cursor.rowcount == 1

            logging.info(f"Executing query: {insert_search_history_query}")
            cursor.execute(insert_search_history_query, (row_insert_question, row_insert_result))
            search_history_inserted = cursor.rowcount == 1

            if search_inserted and search_history_inserted:
                return jsonify({'task': 'successful'})
            else:
                return jsonify({'task': 'failed'})
    except Exception as e:
        logging.error(f"Error during insert: {str(e)}")
        return jsonify({'error': str(e)})
    finally:
        connection.close()


@app.route('/edit_single_row', methods=['POST'])
def update_single_row():
    connection = create_connection()
    question = request.form.get('selected_question')
    new_value = request.form.get('selected_result')
    connection.autocommit(True)
    try:
        with connection.cursor() as cursor:
            cursor.execute(f"""SET @search_id:=(SELECT search.id FROM search WHERE search.query={question} AND 
            search.state!='Deleted')""")
            id_fetched = cursor.fetchone()
            cursor.execute(f"""UPDATE search SET search.result = {new_value}, search.state = 'Updated' 
            WHERE search.id={id_fetched}""")
            cursor.execute(f"""INSERT INTO search_history_new (query, results, state) 
            VALUES ({question}, {new_value}, 'Added');""")
            total_row_count = cursor.rowcount
            if total_row_count == 2:
                return jsonify({'task': 'successful'})
            else:
                return jsonify({'task': 'failed'})
    except Exception as e:
        return jsonify({'error': str(e)})
    finally:
        connection.close()


if __name__ == '__main__':
    app.run(debug=True)
    # app.run(host='0.0.0.0', port=6061)
