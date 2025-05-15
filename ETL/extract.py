
import os
import pandas as pd

# ============================
# BLOQUE: EXTRACCIÓN DE DATOS DESDE ARCHIVOS EXCEL
# ============================


def leer_archivo_presupuesto(ruta_archivo):
    """
    Lee y devuelve el DataFrame del archivo de presupuesto (PPTO CAM).
    Parámetros:
        ruta_archivo (str): Ruta del archivo Excel de presupuesto.
    Retorna:
        pd.DataFrame: DataFrame con la información cargada.
    """
    try:
        df = pd.read_excel(ruta_archivo, sheet_name="Hoja1")
        print(f"[EXTRACT] Archivo de presupuesto leído correctamente: {ruta_archivo}")
        return df
    except Exception as e:
        print(f"[ERROR] No se pudo leer el archivo de presupuesto: {e}")
        return pd.DataFrame()


def leer_archivo_ventas(ruta_archivo):
    """
    Lee y devuelve el DataFrame del archivo de ventas (VENTAS CAM).
    Este archivo tiene encabezados informales al inicio, por eso se saltan las 3 primeras filas.

    Parámetros:
        ruta_archivo (str): Ruta del archivo Excel de ventas.
    Retorna:
        pd.DataFrame: DataFrame con la información cargada desde la fila correcta.
    """
    try:
        df = pd.read_excel(ruta_archivo, sheet_name="Informe 1", skiprows=3)
        print(f"[EXTRACT] Archivo de ventas leído correctamente: {ruta_archivo}")
        return df
    except Exception as e:
        print(f"[ERROR] No se pudo leer el archivo de ventas: {e}")
        return pd.DataFrame()


def obtener_rutas_adjuntos(directorio='adjuntos'):
    """
    Busca automáticamente las rutas de los archivos de ventas y presupuesto
    dentro del directorio especificado (por defecto: 'adjuntos').

    Utiliza coincidencias en el nombre del archivo para diferenciar ventas de presupuesto.

    Parámetros:
        directorio (str): Carpeta donde se encuentran los archivos descargados.
    Retorna:
        tuple(str, str): Ruta de ventas y ruta de presupuesto.
    """
    archivos = os.listdir(directorio)
    ruta_ventas = None
    ruta_ppto = None

    for archivo in archivos:
        nombre = archivo.lower()
        if "ventas" in nombre:
            ruta_ventas = os.path.join(directorio, archivo)
        elif "ppto" in nombre:
            ruta_ppto = os.path.join(directorio, archivo)

    return ruta_ventas, ruta_ppto
