import os
from google.cloud import storage
from google.cloud import bigquery

# ============================
#  CARGA DE DATOS PROCESADOS A GOOGLE CLOUD STORAGE y BIGQUERY
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


def subir_archivo_a_bucket(nombre_bucket, ruta_local, nombre_remoto=None):
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



def subir_a_bigquery(df, nombre_tabla, dataset, project):
    """
    Sube un DataFrame a BigQuery como tabla, sobrescribiendo si ya existe.
    """
    client = bigquery.Client(project=project)
    tabla_id = f"{project}.{dataset}.{nombre_tabla}"

    # Conversión segura: asegurarse que todos los objetos se conviertan a string
    for col in df.select_dtypes(include=['object']).columns:
        df[col] = df[col].astype(str)

    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE",
        autodetect=True,
    )

    job = client.load_table_from_dataframe(df, tabla_id, job_config=job_config)
    job.result()

    print(f"[LOAD] Datos subidos a BigQuery: {tabla_id}")