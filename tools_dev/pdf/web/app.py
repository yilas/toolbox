import os
import sys
import shutil
import subprocess
import uuid
from datetime import datetime
from flask import Flask, render_template, request, send_file, after_this_request
from pypdf import PdfReader, PdfWriter

app = Flask(__name__)

UPLOAD_FOLDER = 'temp_uploads'
if not os.path.exists(UPLOAD_FOLDER):
    os.makedirs(UPLOAD_FOLDER)

def get_ghostscript_command():
    if sys.platform.startswith('win'):
        if shutil.which("gswin64c"): return "gswin64c"
        elif shutil.which("gswin32c"): return "gswin32c"
    else:
        if shutil.which("gs"): return "gs"
    return None

def format_date_for_pdf(date_str):
    if not date_str: return None
    date_str = date_str.replace("T", " ")
    try:
        if len(date_str) > 10: dt = datetime.strptime(date_str, "%Y-%m-%d %H:%M")
        else: dt = datetime.strptime(date_str, "%Y-%m-%d")
        return f"D:{dt.strftime('%Y%m%d%H%M%S')}"
    except ValueError: return None

def process_pdf(input_path, output_path, level, metadata_dict):
    gs_executable = get_ghostscript_command()
    if not gs_executable: raise Exception("Ghostscript non trouvé.")

    quality = {'0': '/default', '1': '/prepress', '2': '/printer', '3': '/ebook', '4': '/screen'}
    settings = quality.get(str(level), '/printer')

    # 1. Compression
    subprocess.run([
        gs_executable, "-sDEVICE=pdfwrite", "-dCompatibilityLevel=1.4",
        f"-dPDFSETTINGS={settings}", "-dNOPAUSE", "-dQUIET", "-dBATCH",
        f"-sOutputFile={output_path}", input_path
    ], check=True)

    # 2. Métadonnées
    if any(v for k, v in metadata_dict.items()):
        try:
            reader = PdfReader(output_path)
            writer = PdfWriter()
            writer.append_pages_from_reader(reader)

            new_metadata = {}
            if reader.metadata:
                for key, value in reader.metadata.items(): new_metadata[key] = value

            if metadata_dict.get('title'): new_metadata['/Title'] = metadata_dict['title']
            if metadata_dict.get('author'): new_metadata['/Author'] = metadata_dict['author']
            if metadata_dict.get('subject'): new_metadata['/Subject'] = metadata_dict['subject']

            created = format_date_for_pdf(metadata_dict.get('created'))
            if created: new_metadata['/CreationDate'] = created

            modified = format_date_for_pdf(metadata_dict.get('modified'))
            if modified: new_metadata['/ModDate'] = modified

            writer.add_metadata(new_metadata)

            temp_meta = output_path + ".meta"
            with open(temp_meta, "wb") as f: writer.write(f)
            shutil.move(temp_meta, output_path)
        except Exception as e:
            print(f"Erreur meta: {e}")

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/compress', methods=['POST'])
def compress():
    if 'file' not in request.files: return "Aucun fichier", 400
    file = request.files['file']
    if file.filename == '': return "Nom vide", 400

    unique_id = str(uuid.uuid4())
    input_filename = f"{unique_id}_{file.filename}"
    output_filename = f"{unique_id}_compressed_{file.filename}"

    input_path = os.path.join(UPLOAD_FOLDER, input_filename)
    output_path = os.path.join(UPLOAD_FOLDER, output_filename)

    try:
        file.save(input_path)

        # Si le titre est vide, on prend le nom du fichier sans extension
        user_title = request.form.get('title')
        if not user_title:
            user_title = os.path.splitext(file.filename)[0]

        metadata = {
            'author': request.form.get('author'),
            'title': user_title,
            'subject': request.form.get('subject'),
            'created': request.form.get('created_date'),
            'modified': request.form.get('modified_date')
        }
        level = request.form.get('compression_level', 2)

        process_pdf(input_path, output_path, level, metadata)

        @after_this_request
        def cleanup(response):
            try:
                if os.path.exists(input_path): os.remove(input_path)
                if os.path.exists(output_path): os.remove(output_path)
            except: pass
            return response

        return send_file(output_path, as_attachment=True, download_name=f"compressed_{file.filename}")

    except Exception as e:
        return f"Erreur: {str(e)}", 500

if __name__ == '__main__':
    app.run(debug=True, port=5000)
