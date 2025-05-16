
#Variables de entorno desde .env
from dotenv import load_dotenv
load_dotenv()
import os  # Para manejar rutas, crear carpetas y verificar archivos locales
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
BUCKET_NAME = os.getenv("BUCKET_NAME")
BQ_PROJECT_ID = os.getenv("BQ_PROJECT_ID")
BQ_DATASET = os.getenv("BQ_DATASET")

#Librerias google
import base64  # Para decodificar los archivos adjuntos que vienen codificados desde Gmail
from google.oauth2.credentials import Credentials  # Manejo del token de acceso ya generado (token.json)
from google_auth_oauthlib.flow import InstalledAppFlow  # Flujo de autenticación interactivo para apps de escritorio
from googleapiclient.discovery import build  # Construcción del cliente de la API de Gmail
from google.cloud import storage  # Cliente para interactuar con Google Cloud Storage (GCS)
from google.auth.transport.requests import Request  # Para refrescar las credenciales si están expiradas

#librerias ETL
from ETL.extract import obtener_rutas_adjuntos, leer_archivo_presupuesto, leer_archivo_ventas
from ETL.transform import limpiar_datos_ventas, limpiar_datos_presupuesto
from ETL.load import guardar_csv_local, subir_archivo_a_bucket

#Para bigQuery
from ETL.load import subir_a_bigquery


# SCOPES define los permisos requeridos para acceder a Gmail
SCOPES = ['https://www.googleapis.com/auth/gmail.readonly']


def autenticar_gmail():
    """
    Autentica al usuario mediante OAuth 2.0 y retorna el servicio Gmail listo para usar.
    """
    creds = None
    if os.path.exists('token.json'):
        creds = Credentials.from_authorized_user_file('token.json', SCOPES)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(os.getenv("GMAIL_CREDENTIALS"), SCOPES)
            creds = flow.run_local_server(port=0)
        with open('token.json', 'w') as token:
            token.write(creds.to_json())
    return build('gmail', 'v1', credentials=creds)



def buscar_correos_con_adjuntos(service, remitente, asunto):
    """
    Busca mensajes en Gmail que cumplan con los filtros dados y tengan archivos adjuntos.
    """
    query = f'from:{remitente} subject:{asunto} has:attachment'
    resultados = service.users().messages().list(userId='me', q=query).execute()
    return resultados.get('messages', [])


def descargar_adjuntos(service, mensajes, extensiones_validas, carpeta_destino='adjuntos'):
    """
    Descarga los adjuntos filtrando por extensión (CSV, XLSX, etc.) y los guarda localmente.
    """
    os.makedirs(carpeta_destino, exist_ok=True)
    for msg in mensajes:
        mensaje = service.users().messages().get(userId='me', id=msg['id']).execute()
        partes = mensaje.get('payload', {}).get('parts', [])
        for parte in partes:
            if parte.get('filename') and any(parte['filename'].lower().endswith(ext) for ext in extensiones_validas):
                adjunto_id = parte['body']['attachmentId']
                adjunto = service.users().messages().attachments().get(
                    userId='me', messageId=msg['id'], id=adjunto_id
                ).execute()
                datos = base64.urlsafe_b64decode(adjunto['data'].encode('UTF-8'))
                ruta = os.path.join(carpeta_destino, parte['filename'])
                with open(ruta, 'wb') as f:
                    f.write(datos)
                print(f"Archivo guardado: {ruta}")


def subir_a_gcs(nombre_bucket, ruta_local, nombre_remoto=None):
    """
    Sube un archivo local a un bucket de Google Cloud Storage.
    Si el bucket no existe, muestra un mensaje de advertencia y no sube el archivo.
    """
    client = storage.Client()
    bucket = client.bucket(nombre_bucket)

    if not bucket.exists():
        print(f"[ERROR] El bucket '{nombre_bucket}' no existe en GCS. Verificá el nombre y los permisos.")
        return

    blob = bucket.blob(nombre_remoto or os.path.basename(ruta_local))
    blob.upload_from_filename(ruta_local)
    print(f"Archivo subido a GCS: gs://{nombre_bucket}/{blob.name}")


if __name__ == "__main__":
    # Paso 1: Autenticarse en Gmail
    servicio_gmail = autenticar_gmail()

    # Paso 2: Buscar correos que coincidan con los filtros deseados
    mensajes_filtrados = buscar_correos_con_adjuntos(
        service=servicio_gmail,
        remitente='lineadan@gmail.com',
        asunto='Reporte de Ventas'
    )

    # Paso 3: Descargar los adjuntos de esos correos
    descargar_adjuntos(
        service=servicio_gmail,
        mensajes=mensajes_filtrados,
        extensiones_validas=['.csv', '.xlsx']
    )

    # Paso 4: Subir los archivos descargados a Google Cloud Storage
    for archivo in os.listdir('adjuntos'):
        ruta_archivo = os.path.join('adjuntos', archivo)
        subir_a_gcs(nombre_bucket='pozuelotest', ruta_local=ruta_archivo)

    # ----------------------------------
    # EJECUCIÓN DEL PROCESO ETL COMPLETO
    # ----------------------------------
    print("\n🔁 Iniciando proceso ETL...")

    # Paso 4: Detectar archivos descargados
    ruta_ventas, ruta_presupuesto = obtener_rutas_adjuntos()

    if not ruta_ventas or not ruta_presupuesto:
        print("[ERROR] Faltan archivos necesarios para el proceso ETL.")
        exit()

    # Paso 5: Leer archivos
    df_ventas = leer_archivo_ventas(ruta_ventas)
    df_ppto = leer_archivo_presupuesto(ruta_presupuesto)

    # Paso 6: Limpiar datos
    df_ventas_limpio = limpiar_datos_ventas(df_ventas)
    df_ppto_limpio = limpiar_datos_presupuesto(df_ppto)

    # Obtener nombres originales con sufijo "_limpio"
    nombre_ventas_csv = os.path.splitext(os.path.basename(ruta_ventas))[0] + "_limpio.csv"
    nombre_ppto_csv = os.path.splitext(os.path.basename(ruta_presupuesto))[0] + "_limpio.csv"

    # Paso 7: Guardar como CSV local con nombre personalizado
    csv_ventas = guardar_csv_local(df_ventas_limpio, nombre_ventas_csv)
    csv_ppto = guardar_csv_local(df_ppto_limpio, nombre_ppto_csv)

    # Paso 8: Subir CSV limpio a GCS
    subir_archivo_a_bucket(BUCKET_NAME, csv_ventas)
    subir_archivo_a_bucket(BUCKET_NAME, csv_ppto)


    #Paso 9: Carga bigQuery
    subir_a_bigquery(df_ventas_limpio, "ventas_limpio", BQ_DATASET, BQ_PROJECT_ID)
    subir_a_bigquery(df_ppto_limpio, "presupuesto_limpio", BQ_DATASET, BQ_PROJECT_ID)


    print("\n✅ Proceso completado con éxito.")

    