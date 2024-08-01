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
    redshift_query = replaceIntervalFunctions(redshift_query)

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


def replaceIntervalFunctions(redshift_query):
    pattern = re.compile(r'^(.*\s)\+\s*INTERVAL\s*\'(\d+)\s(YEAR)\'(.*)$')
    lines = redshift_query.split('\n')

    for i, line in enumerate(lines):
        line_match = pattern.match(line)
        if line_match:
            before_plus = re.sub(r'[^a-zA-Z0-9.]', '', line_match.group(1)) 
            interval_num = line_match.group(2)
            interval_unit = line_match.group(3)
            after_interval = line_match.group(4)

            lines[i] = f"\t, dateadd('{interval_unit.lower()}', {interval_num}, '{before_plus}'){after_interval}"
            print(lines[i])

    return '\n'.join(lines)


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
                           file_name=f"{report_id}-{report_name}-RV.sql"
                           )


@app.route('/download', methods=['POST'])
def downloadQuery():
    report_id = request.form['report_id']
    report_name = request.form['report_name']
    translated_query = request.form['translated_query']
    file_name = f"{report_id}-{report_name}-RV.sql"

    return send_file(
        io.BytesIO(translated_query.encode()),
        as_attachment=True,
        download_name=file_name,
        mimetype='text/sql'
    )

@app.route('/download/original', methods=['POST'])
def downloadOriginalQuery():
    report_id = request.form['report_id']
    report_name = request.form['report_name']
    original_query = request.form['original_query']
    file_name = f"{report_id}-{report_name}-Original.sql"

    return send_file(
        io.BytesIO(original_query.encode()),
        as_attachment=True,
        download_name=file_name,
        mimetype='text/sql'
    )

if __name__ == '__main__':
    app.run(debug=True)