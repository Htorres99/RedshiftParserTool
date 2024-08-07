import shutil
from flask import Flask, json, render_template, request, send_file, jsonify
from werkzeug.utils import secure_filename
from reserved_words_mapping import RESERVED_WORDS_MAPPING
import io
import re
import os, zipfile, tempfile

app = Flask(__name__)
app.config['UPLOAD_FOLDER'] = 'uploaded_files'  # Temporary folder to store uploaded files
app.config['MAX_CONTENT_LENGTH'] = 512 * 1024 * 1024  # 16 MB max upload size, adjust as needed

# Translate PostgreSQL to Redshift
def translateQuery(postgres_query):
    print("Translating... ")
    
    redshift_query = postgres_query

    redshift_query = checkAssert(redshift_query)

    for postgres_word, redshift_word in RESERVED_WORDS_MAPPING.items():
        pattern = r'\b' + re.escape(postgres_word) + r'\b'
        redshift_query = re.sub(pattern, 
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
    redshift_query = re.sub(r'(^|\n)(\s{2,})?(\bOR\b)', '\n\tOR', redshift_query)
    # Replace occurrences CONCAT_WS
    redshift_query = replaceConcatWSFunctions(redshift_query)

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


def checkAssert(redshift_query):
    line1 = "do $$begin assert (select max(run_ts) from lake.celink_assignmenttracking) >= current_date,'celink_assignmenttracking refresh not complete'; end$$;"
    line2 = "do $$begin assert (select max(run_ts) from lake.celink_assignmentdocs) >= current_date,'celink_assignmentdocs refresh not complete'; end$$;"

    replace_line1 = "CALL check_celink_assignmenttracking_refresh();"
    replace_line2 = "CALL check_celink_assignmentdocs_refresh();"

    # Break the original query into lines
    lines = redshift_query.splitlines()
    
    if lines[0] == line1 and lines[1] == line2:
        lines[0] = replace_line1
        lines[1] = replace_line2
    
    return "\n".join(lines)

# TODO: Working on logic to modify Values to use UNION ALL. 
def replaceValuesFunctions(redshift_query):
    '''SELECT Z.*
    FROM (VALUES 
        (272, 'TransferLetter'), 
        (2796, 'AssignmentFileImaged'),
        (5063, 'PermExclude'),
        (3405, 'Exclude'),
        (3406, 'Include'),
        (538, 'Exclude'),
        (5111, 'Exclude')
    ) AS Z(StepNumber, StepType)'''

    select_pattern = re.compile(r"^(.*\s)SELECT\s+\w+\.\*\s+FROM\s+\(\s*VALUES\s+\((?:\s*\d+\s*,\s*'[^']*'\s*\)\s*,?)+\)\s*\)\s+AS\s+\w+\(\s*\w+\s*,\s*\w+\s*\)")
    return None


def replaceConcatWSFunctions(redshift_query):
    print("Replace Concat_WS")
    pattern = re.compile(
        r"CONCAT_WS\('([^']*)',\s*([^)]+)\)\s+AS\s+\"([^\"]+)\"",
        re.IGNORECASE
    )

    def replace_match(match):
        print("Replace Match Subtask")
        delimiter = match.group(1)
        columns = match.group(2).split(',')
        alias = match.group(3)
        
        print(columns)
        # Construct the replacement string
        transformed_parts = []
        for column in columns:
            column = column.strip()
            transformed_parts.append(f"COALESCE({column}, '')")
            print(transformed_parts)
        
        transformed_string = f" || '{delimiter}' || ".join(transformed_parts)
        print(f"{transformed_string} AS \"{alias}\"")
        return f"{transformed_string} AS \"{alias}\""
    
    
    # Substitute all occurrences of CONCAT_WS with the transformation
    return pattern.sub(replace_match, redshift_query)


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


@app.route('/bulk_translate', methods=['GET', 'POST'])
def bulkTranslate():
    if request.method == 'GET':
        return render_template('bulk_translate.html')

    if request.method == 'POST':
        # Retrieve uploaded files from the request
        uploaded_files = request.files.getlist('files')
        if not uploaded_files:
            return jsonify({'error': 'No files uploaded.'}), 400
        
        processed_files = []
        temp_dir = tempfile.mkdtemp()

        try:
            for file in uploaded_files:
                original_filename = file.filename
                if '/' in original_filename or '\\' in original_filename:
                    # Handle both Unix and Windows path separators
                    original_filename = original_filename.replace('\\', '/').split('/', 1)[-1]
                
                file_name, file_ext = os.path.splitext(original_filename)
                file_name = original_filename.split('/', 1)[-1]

                if '-RV' in file_name:
                    print(f"Skipping file: {file_name} (already processed)")
                    continue

                print(f"FILE: {file_name} \nEXT : {file_ext}")

                # Ensure directory structure is maintained
                relative_path = os.path.dirname(original_filename)
                temp_file_dir = os.path.join(temp_dir, relative_path)
                os.makedirs(temp_file_dir, exist_ok=True)
                print(f"REL PATH: {relative_path} \nTEMP DIR : {temp_file_dir}")

                # Read original file content
                content = file.read().decode('utf-8')
                
                # Process the content (translate the query)
                translated_content = translateQuery(content)

                # Create the translated filename with -RV suffix
                translated_filename = f"{file_name}-RV{file_ext}"
                translated_file_path = os.path.join(temp_file_dir, translated_filename)
                print(f"TRANS FILE PATH: {translated_file_path}")
                print(f"TRANS FILE NAME: {translated_filename}")

                # Write the translated content to a new temporary file
                with open(translated_file_path, 'w', encoding='utf-8') as translated_file:
                    translated_file.write(translated_content)

                # Keep track of the processed files
                processed_files.append(translated_file_path)

            # Create a zip file containing all processed files
            print("Creating ZIP file...")
            zip_filename = os.path.join(temp_dir, 'translated_files.zip')
            with zipfile.ZipFile(zip_filename, 'w') as zipf:
                for file_path in processed_files:
                    zipf.write(file_path, os.path.relpath(file_path, temp_dir))

            # Ensure all files are closed before sending the zip file
            if zipf.fp is not None:
                zipf.close()
            print(f"ZIP file closed successfully, sending from {zip_filename}")
            return send_file(zip_filename, as_attachment=True, download_name='translated_files.zip')
        finally:
            # Clean up the temporary directory and all its contents after response
            try:
                shutil.rmtree(temp_dir)
            except Exception as e:
                print(f"Error cleaning up temporary directory: {e}")
                
if __name__ == '__main__':
    app.run(debug=True)