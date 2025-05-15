import os
from google.cloud import storage

# ============================
# BLOQUE: CARGA DE DATOS PROCESADOS A GOOGLE CLOUD STORAGE
# ============================

def guardar_csv_local(df, nombre_archivo, carpeta='salida'):
    """
    Guarda un DataFrame como archivo CSV localmente.
    """
    os.makedirs(carpeta, exist_ok=True)
    ruta_completa = os.path.join(carpeta, nombre_archivo)
    df.to_csv(ruta_completa, index=False)
    print(f"[LOAD] Archivo CSV guardado localmente: {ruta_completa}")
    return ruta_completa


def subir_archivo_a_gcs(nombre_bucket, ruta_local, nombre_remoto=None):
    """
    Sube un archivo local a un bucket de GCS.
    Si el archivo ya existe en el bucket, será sobrescrito automáticamente.
    """
    client = storage.Client()
    bucket = client.bucket(nombre_bucket)

    if not bucket.exists():
        print(f"[ERROR] El bucket '{nombre_bucket}' no existe.")
        return

    blob = bucket.blob(nombre_remoto or os.path.basename(ruta_local))
    blob.upload_from_filename(ruta_local)
    print(f"[LOAD] Archivo subido a GCS: gs://{nombre_bucket}/{blob.name} (sobrescrito si ya existía)")