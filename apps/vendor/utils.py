import io
import os
from django.core.files.base import ContentFile


def compress_file(uploaded_file):
    name = uploaded_file.name
    ext = os.path.splitext(name)[1].lower()
    if ext in ['.jpg', '.jpeg', '.png', '.webp']:
        return _compress_image(uploaded_file, name, ext)
    elif ext == '.pdf':
        return _compress_pdf(uploaded_file, name)
    return uploaded_file


def _compress_image(uploaded_file, name, ext):
    try:
        from PIL import Image
        img = Image.open(uploaded_file)
        if img.mode in ('RGBA', 'P', 'LA'):
            img = img.convert('RGB')
        max_dim = 1920
        if max(img.size) > max_dim:
            img.thumbnail((max_dim, max_dim), Image.LANCZOS)
        output = io.BytesIO()
        fmt = 'JPEG' if ext in ['.jpg', '.jpeg'] else ext.lstrip('.').upper()
        img.save(output, format=fmt, quality=75, optimize=True)
        output.seek(0)
        return ContentFile(output.read(), name=name)
    except Exception:
        uploaded_file.seek(0)
        return ContentFile(uploaded_file.read(), name=name)


def _compress_pdf(uploaded_file, name):
    try:
        import pikepdf
        uploaded_file.seek(0)
        reader = pikepdf.open(uploaded_file)
        output = io.BytesIO()
        reader.save(output, compress_streams=True, recompress_flate=True)
        output.seek(0)
        return ContentFile(output.read(), name=name)
    except Exception:
        uploaded_file.seek(0)
        return ContentFile(uploaded_file.read(), name=name)