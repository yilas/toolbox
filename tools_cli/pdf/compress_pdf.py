#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import subprocess
import sys
import os
import shutil
import argparse
from datetime import datetime
from pypdf import PdfReader, PdfWriter

def get_ghostscript_command():
    """Détecte la commande Ghostscript selon l'OS."""
    if sys.platform.startswith('win'):
        if shutil.which("gswin64c"): return "gswin64c"
        elif shutil.which("gswin32c"): return "gswin32c"
    else:
        if shutil.which("gs"): return "gs"
    return None

def format_date_for_pdf(date_str):
    """
    Convertit une date humaine (YYYY-MM-DD [HH:MM:SS]) en format PDF.
    Format PDF attendu : D:YYYYMMDDHHmmSS
    """
    if not date_str:
        return None

    try:
        # Essai avec heure
        dt = datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S")
    except ValueError:
        try:
            # Essai sans heure (on met 00:00:00 par défaut)
            dt = datetime.strptime(date_str, "%Y-%m-%d")
        except ValueError:
            print(f"Format de date invalide : {date_str}. Utilisez 'YYYY-MM-DD' ou 'YYYY-MM-DD HH:MM:SS'")
            return None

    # Conversion au format PDF : D:YYYYMMDDHHmmSS
    return f"D:{dt.strftime('%Y%m%d%H%M%S')}"

def add_metadata(file_path, metadata_args):
    """
    Ajoute les métadonnées au fichier PDF compressé via pypdf.
    """
    try:
        reader = PdfReader(file_path)
        writer = PdfWriter()

        # Copie des pages
        writer.append_pages_from_reader(reader)

        # Création d'un dictionnaire Python standard pour éviter l'erreur PdfObject
        new_metadata = {}

        # 1. Copie des métadonnées existantes
        if reader.metadata:
            for key, value in reader.metadata.items():
                new_metadata[key] = value

        # 2. Mise à jour avec les arguments fournis
        if metadata_args.title:
            new_metadata['/Title'] = metadata_args.title
        if metadata_args.author:
            new_metadata['/Author'] = metadata_args.author
        if metadata_args.subject:
            new_metadata['/Subject'] = metadata_args.subject

        # 3. Gestion des dates (CreationDate et ModDate)
        if metadata_args.created:
            pdf_date = format_date_for_pdf(metadata_args.created)
            if pdf_date:
                new_metadata['/CreationDate'] = pdf_date

        if metadata_args.modified:
            pdf_date = format_date_for_pdf(metadata_args.modified)
            if pdf_date:
                new_metadata['/ModDate'] = pdf_date

        # Injection des métadonnées
        writer.add_metadata(new_metadata)

        # Écriture fichier temporaire
        temp_file = file_path + ".meta.tmp"
        with open(temp_file, "wb") as f_out:
            writer.write(f_out)

        # Remplacement
        shutil.move(temp_file, file_path)
        print("--- Métadonnées ajoutées avec succès ---")

    except Exception as e:
        print(f"Attention : Impossible d'ajouter les métadonnées : {e}")
        if os.path.exists(file_path + ".meta.tmp"):
            os.remove(file_path + ".meta.tmp")

def compress_pdf(input_file, args):
    if not os.path.isfile(input_file):
        print(f"Erreur : Le fichier '{input_file}' est introuvable.")
        sys.exit(1)

    gs_executable = get_ghostscript_command()
    if not gs_executable:
        print("Erreur : Ghostscript introuvable.")
        sys.exit(1)

    base_name, ext = os.path.splitext(input_file)
    output_file = f"{base_name}_compressed{ext}"

    quality = {0: '/default', 1: '/prepress', 2: '/printer', 3: '/ebook', 4: '/screen'}
    settings = quality.get(args.level, '/printer')

    print(f"--- Compression de '{input_file}' (Niveau {args.level}) ---")

    try:
        command = [
            gs_executable,
            "-sDEVICE=pdfwrite",
            "-dCompatibilityLevel=1.4",
            f"-dPDFSETTINGS={settings}",
            "-dNOPAUSE", "-dQUIET", "-dBATCH",
            f"-sOutputFile={output_file}",
            input_file
        ]

        subprocess.run(command, check=True)

        # --- ÉTAPE 2 : Ajout des métadonnées ---
        # Vérifie si au moins une option de métadonnée est utilisée
        if any([args.title, args.author, args.subject, args.created, args.modified]):
            add_metadata(output_file, args)

        # Stats
        original_size = os.path.getsize(input_file)
        new_size = os.path.getsize(output_file)
        ratio = (1 - (new_size / original_size)) * 100

        print(f"--- Terminé ---")
        print(f"Fichier : {output_file}")
        print(f"Taille  : {original_size/1024:.2f} KB -> {new_size/1024:.2f} KB ({ratio:.1f}%)")

    except subprocess.CalledProcessError as e:
        print(f"Erreur Ghostscript : {e}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Compresseur PDF local + Métadonnées complètes.")

    parser.add_argument("input", help="Fichier PDF source")
    parser.add_argument("-l", "--level", type=int, choices=[0,1,2,3,4], default=2, help="Niveau compression")

    # Arguments métadonnées texte
    parser.add_argument("--title", help="Titre du document")
    parser.add_argument("--author", help="Auteur du document")
    parser.add_argument("--subject", help="Sujet du document")

    # Arguments métadonnées dates
    parser.add_argument("--created", help="Date de création (YYYY-MM-DD ou YYYY-MM-DD HH:MM:SS)")
    parser.add_argument("--modified", help="Date de modification (YYYY-MM-DD ou YYYY-MM-DD HH:MM:SS)")

    args = parser.parse_args()

    compress_pdf(args.input, args)
