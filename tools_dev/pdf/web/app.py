import os
import sys
import shutil
import subprocess
import uuid
import zipfile
import logging # Module de logging
from datetime import datetime
from flask import Flask, render_template, request, send_file, after_this_request
from pypdf import PdfReader, PdfWriter

# --- CONFIGURATION LOGGING ---
# Crée un logger qui écrit dans la console ET dans un fichier
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler("activity.log", encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

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

def get_file_size_mb(path):
    return os.path.getsize(path) / (1024 * 1024)

def process_single_pdf(file_storage, compression_level, metadata_form):
    unique_id = str(uuid.uuid4())
    input_filename = f"{unique_id}_{file_storage.filename}"
    output_filename = f"{unique_id}_processed_{file_storage.filename}"

    input_path = os.path.join(UPLOAD_FOLDER, input_filename)
    output_path = os.path.join(UPLOAD_FOLDER, output_filename)

    try:
        file_storage.save(input_path)
        original_size = get_file_size_mb(input_path)
        logger.info(f"Fichier reçu: {file_storage.filename} ({original_size:.2f} MB)")

        gs_executable = get_ghostscript_command()
        if not gs_executable:
            logger.error("Ghostscript introuvable")
            raise Exception("Ghostscript non trouvé.")

        quality = {'0': '/default', '1': '/prepress', '2': '/printer', '3': '/ebook', '4': '/screen'}
        settings = quality.get(str(compression_level), '/printer')

        logger.info(f"Démarrage compression (Niveau {compression_level}) pour {file_storage.filename}...")

        # 1. Compression
        subprocess.run([
            gs_executable, "-sDEVICE=pdfwrite", "-dCompatibilityLevel=1.4",
            f"-dPDFSETTINGS={settings}", "-dNOPAUSE", "-dQUIET", "-dBATCH",
            f"-sOutputFile={output_path}", input_path
        ], check=True)

        # 2. Métadonnées
        final_title = metadata_form.get('title')
        if not final_title:
            final_title = os.path.splitext(file_storage.filename)[0]

        meta_dict = {
            'title': final_title,
            'author': metadata_form.get('author'),
            'subject': metadata_form.get('subject'),
            'created': metadata_form.get('created_date'),
            'modified': metadata_form.get('modified_date')
        }

        if any(v for k, v in meta_dict.items()):
            logger.info(f"Injection métadonnées pour {file_storage.filename}")
            reader = PdfReader(output_path)
            writer = PdfWriter()
            writer.append_pages_from_reader(reader)

            new_metadata = {}
            if reader.metadata:
                for key, value in reader.metadata.items(): new_metadata[key] = value

            if meta_dict['title']: new_metadata['/Title'] = meta_dict['title']
            if meta_dict['author']: new_metadata['/Author'] = meta_dict['author']
            if meta_dict['subject']: new_metadata['/Subject'] = meta_dict['subject']

            created = format_date_for_pdf(meta_dict['created'])
            if created: new_metadata['/CreationDate'] = created

            modified = format_date_for_pdf(meta_dict['modified'])
            if modified: new_metadata['/ModDate'] = modified

            writer.add_metadata(new_metadata)

            temp_meta = output_path + ".meta"
            with open(temp_meta, "wb") as f: writer.write(f)
            shutil.move(temp_meta, output_path)

        # Stats finales
        new_size = get_file_size_mb(output_path)
        reduction = (1 - (new_size / original_size)) * 100
        logger.info(f"Succès {file_storage.filename}: {original_size:.2f}MB -> {new_size:.2f}MB (-{reduction:.1f}%)")

        return input_path, output_path, file_storage.filename

    except Exception as e:
        logger.error(f"Erreur sur {file_storage.filename}: {e}")
        if os.path.exists(input_path): os.remove(input_path)
        return None, None, None

@app.route('/')
def index():
    logger.info("Page d'accueil visitée")
    return render_template('index.html')

@app.route('/compress', methods=['POST'])
def compress():
    if 'file' not in request.files: return "Aucun fichier", 400

    files = request.files.getlist('file')
    if not files or files[0].filename == '': return "Aucun fichier sélectionné", 400

    logger.info(f"Réception de {len(files)} fichier(s) à traiter.")

    compression_level = request.form.get('compression_level', 2)
    metadata_form = {
        'author': request.form.get('author'),
        'title': request.form.get('title'),
        'subject': request.form.get('subject'),
        'created_date': request.form.get('created_date'),
        'modified_date': request.form.get('modified_date')
    }

    processed_files = []

    for file in files:
        in_p, out_p, name = process_single_pdf(file, compression_level, metadata_form)
        if out_p:
            processed_files.append((in_p, out_p, name))

    if not processed_files:
        logger.error("Aucun fichier n'a pu être traité correctement.")
        return "Erreur lors du traitement.", 500

    @after_this_request
    def cleanup(response):
        for in_p, out_p, _ in processed_files:
            try:
                if os.path.exists(in_p): os.remove(in_p)
                if os.path.exists(out_p): os.remove(out_p)
            except: pass
        if len(processed_files) > 1:
            try:
                zip_path = os.path.join(UPLOAD_FOLDER, "documents_optimises.zip")
                if os.path.exists(zip_path): os.remove(zip_path)
            except: pass
        logger.info("Nettoyage des fichiers temporaires terminé.")
        return response

    if len(processed_files) == 1:
        _, output_path, download_name = processed_files[0]
        logger.info(f"Envoi du fichier unique: {download_name}")
        return send_file(output_path, as_attachment=True, download_name=download_name)
    else:
        zip_path = os.path.join(UPLOAD_FOLDER, "documents_optimises.zip")
        logger.info("Création de l'archive ZIP pour envoi groupé.")
        with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
            for _, output_path, download_name in processed_files:
                zipf.write(output_path, download_name)

        return send_file(zip_path, as_attachment=True, download_name="documents_optimises.zip")

if __name__ == '__main__':
    print("--- Serveur Démarré ---")
    print("Logs disponibles dans le fichier 'activity.log'")
    app.run(debug=True, port=5000)
