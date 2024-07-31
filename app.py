from flask import Flask, render_template, request, send_file
from reserved_words_mapping import RESERVED_WORDS_MAPPING
import io
import re

app = Flask(__name__)

# Translate PostgreSQL to Redshift
def translateQuery(postgres_query):
    print("Translating... ")
    
    redshift_query = postgres_query
    for postgres_word, redshift_word in RESERVED_WORDS_MAPPING.items():
        redshift_query = re.sub(r'\b' + re.escape(postgres_word) + r'\b', 
                                redshift_word, 
                                redshift_query, 
                                flags=re.IGNORECASE)
        
    redshift_query = formatQuery(redshift_query)

    return redshift_query

def formatQuery(redshift_query):
    # Remove comment lines that start with --
    redshift_query = re.sub(r'--.*\n', '', redshift_query)
    # Replace lines that start with ',' with a tab then the line
    redshift_query = re.sub(r'^\s*,', r'\t,', redshift_query, flags=re.MULTILINE)
    # Add a tab to lines starting with And or Or
    redshift_query = re.sub(r'^\s*(AND|OR)\b', r'\t\1', redshift_query, flags=re.MULTILINE)
    # Replace occurrences of 'OR' in lines, not at the start, with line break, tab, and 'OR'
    redshift_query = re.sub(r'(?<!^)OR\b', '\n\tOR', redshift_query)
    
    return redshift_query

@app.route('/', methods=['GET', 'POST'])
def index():
    report_id = ""
    report_name = ""
    original_query = ""
    translated_query = ""

    if request.method == 'POST':
        report_id = request.form['report_id']
        report_name = request.form['report_name']
        original_query= request.form['query']
        translated_query = translateQuery(original_query)
    return render_template('index.html', 
                           translated_query=translated_query, 
                           original_query=original_query, 
                           report_id=report_id, 
                           report_name=report_name,
                           file_name=f"Redshift-{report_id}-{report_name}.sql"
                           )


@app.route('/download', methods=['POST'])
def downloadQuery():
    report_id = request.form['report_id']
    report_name = request.form['report_name']
    translated_query = request.form['translated_query']

    file_name = f"Redshift-{report_id}-{report_name}.sql"
    return send_file(
        io.BytesIO(translated_query.encode()),
        as_attachment=True,
        download_name=file_name,
        mimetype='text/sql'
    )


if __name__ == '__main__':
    app.run(debug=True)