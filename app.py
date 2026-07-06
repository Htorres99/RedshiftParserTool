import logging
import os
import re
import shutil
import tempfile
import zipfile
import io

from flask import Flask, render_template, request, send_file, jsonify
from werkzeug.utils import secure_filename

from reserved_words_mapping import RESERVED_WORDS_MAPPING

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

app = Flask(__name__)
app.config['UPLOAD_FOLDER'] = 'uploaded_files'  # Temporary folder to store uploaded files
app.config['MAX_CONTENT_LENGTH'] = 512 * 1024 * 1024  # 512 MB max upload size, adjust as needed

ALLOWED_EXTENSIONS = {'.sql'}


# ---------------------------------------------------------------------------
# Query translation logic
# ---------------------------------------------------------------------------

def translateQuery(postgres_query):
    logger.info("Translating query...")

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
            logger.debug(lines[i])

    return '\n'.join(lines)


def checkAssert(redshift_query):
    line1 = "do $$begin assert (select max(run_ts) from lake.celink_assignmenttracking) >= current_date,'celink_assignmenttracking refresh not complete'; end$$;"
    line2 = "do $$begin assert (select max(run_ts) from lake.celink_assignmentdocs) >= current_date,'celink_assignmentdocs refresh not complete'; end$$;"

    replace_line1 = "CALL check_celink_assignmenttracking_refresh();"
    replace_line2 = "CALL check_celink_assignmentdocs_refresh();"

    lines = redshift_query.splitlines()

    # Guard against files with fewer than 2 lines
    if len(lines) >= 2 and lines[0] == line1 and lines[1] == line2:
        lines[0] = replace_line1
        lines[1] = replace_line2

    return "\n".join(lines)


def replaceConcatWSFunctions(redshift_query):
    pattern = re.compile(
        r"CONCAT_WS\('([^']*)',\s*([^)]+)\)\s+AS\s+\"([^\"]+)\"",
        re.IGNORECASE
    )

    def replace_match(match):
        delimiter = match.group(1)
        columns = match.group(2).split(',')
        alias = match.group(3)

        transformed_parts = [f"COALESCE({column.strip()}, '')" for column in columns]
        transformed_string = f" || '{delimiter}' || ".join(transformed_parts)
        return f"{transformed_string} AS \"{alias}\""

    return pattern.sub(replace_match, redshift_query)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def is_safe_relative_path(path):
    """Reject paths that try to escape the destination directory."""
    normalized = os.path.normpath(path)
    if normalized.startswith('..') or os.path.isabs(normalized):
        return False
    return True


def has_allowed_extension(filename):
    _, ext = os.path.splitext(filename)
    return ext.lower() in ALLOWED_EXTENSIONS


def get_required_form_field(field_name):
    value = request.form.get(field_name, '').strip()
    if not value:
        raise ValueError(f"Missing required field: {field_name}")
    return value


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@app.route('/', methods=['GET', 'POST'])
def index():
    report_id = ""
    report_name = ""
    original_query = ""
    translated_query = ""

    if request.method == 'POST':
        try:
            report_id = get_required_form_field('report_id')
            report_name = get_required_form_field('report_name')
            original_query = get_required_form_field('query')
        except ValueError as e:
            return jsonify({'error': str(e)}), 400

        try:
            translated_query = translateQuery(original_query)
        except Exception:
            logger.exception("Failed to translate query")
            return jsonify({'error': 'Failed to translate query. Check the SQL syntax and try again.'}), 400

    return render_template('index.html',
                            translated_query=translated_query,
                            original_query=original_query,
                            report_id=report_id,
                            report_name=report_name,
                            file_name=f"{report_id}-{report_name}-RV.sql" if report_id else ""
                            )


@app.route('/download', methods=['POST'])
def downloadQuery():
    try:
        report_id = get_required_form_field('report_id')
        report_name = get_required_form_field('report_name')
        translated_query = get_required_form_field('translated_query')
    except ValueError as e:
        return jsonify({'error': str(e)}), 400

    file_name = secure_filename(f"{report_id}-{report_name}-RV.sql")

    return send_file(
        io.BytesIO(translated_query.encode('utf-8')),
        as_attachment=True,
        download_name=file_name,
        mimetype='text/sql'
    )


@app.route('/download/original', methods=['POST'])
def downloadOriginalQuery():
    try:
        report_id = get_required_form_field('report_id')
        report_name = get_required_form_field('report_name')
        original_query = get_required_form_field('original_query')
    except ValueError as e:
        return jsonify({'error': str(e)}), 400

    file_name = secure_filename(f"{report_id}-{report_name}-Original.sql")

    return send_file(
        io.BytesIO(original_query.encode('utf-8')),
        as_attachment=True,
        download_name=file_name,
        mimetype='text/sql'
    )


@app.route('/bulk_translate', methods=['GET', 'POST'])
def bulkTranslate():
    if request.method == 'GET':
        return render_template('bulk_translate.html')

    uploaded_files = request.files.getlist('files')
    if not uploaded_files:
        return jsonify({'error': 'No files uploaded.'}), 400

    temp_dir = tempfile.mkdtemp()
    processed_files = []
    skipped_files = []
    errors = []

    try:
        for file in uploaded_files:
            original_filename = file.filename or ""

            if not original_filename:
                continue

            # Normalize path separators and strip any leading path traversal attempts
            normalized_name = original_filename.replace('\\', '/')

            if not is_safe_relative_path(normalized_name):
                errors.append(f"Rejected unsafe path: {original_filename}")
                continue

            if not has_allowed_extension(normalized_name):
                skipped_files.append(f"{original_filename} (unsupported extension)")
                continue

            relative_dir = os.path.dirname(normalized_name)
            base_name = os.path.basename(normalized_name)
            file_name, file_ext = os.path.splitext(base_name)

            if '-RV' in file_name:
                logger.info(f"Skipping file: {base_name} (already processed)")
                skipped_files.append(base_name)
                continue

            # Build destination dir, still confined within temp_dir due to the safety check above
            temp_file_dir = os.path.join(temp_dir, relative_dir)
            os.makedirs(temp_file_dir, exist_ok=True)

            try:
                content = file.read().decode('utf-8')
            except UnicodeDecodeError:
                errors.append(f"{original_filename}: could not decode as UTF-8")
                continue

            try:
                translated_content = translateQuery(content)
            except Exception:
                logger.exception(f"Failed to translate {original_filename}")
                errors.append(f"{original_filename}: translation failed")
                continue

            translated_filename = secure_filename(f"{file_name}-RV{file_ext}")
            translated_file_path = os.path.join(temp_file_dir, translated_filename)

            with open(translated_file_path, 'w', encoding='utf-8') as translated_file:
                translated_file.write(translated_content)

            processed_files.append(translated_file_path)

        if not processed_files:
            return jsonify({
                'error': 'No files were translated.',
                'skipped': skipped_files,
                'errors': errors
            }), 400

        # Build the zip fully in memory so we can safely clean up temp_dir
        # before the response finishes streaming.
        zip_buffer = io.BytesIO()
        with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zipf:
            for file_path in processed_files:
                zipf.write(file_path, os.path.relpath(file_path, temp_dir))
        zip_buffer.seek(0)

        logger.info(f"ZIP built in memory with {len(processed_files)} file(s)")

        return send_file(
            zip_buffer,
            as_attachment=True,
            download_name='translated_files.zip',
            mimetype='application/zip'
        )
    finally:
        try:
            shutil.rmtree(temp_dir)
        except Exception:
            logger.exception(f"Error cleaning up temporary directory: {temp_dir}")


if __name__ == '__main__':
    debug_mode = os.environ.get('FLASK_DEBUG', 'false').lower() == 'true'
    app.run(debug=debug_mode)
