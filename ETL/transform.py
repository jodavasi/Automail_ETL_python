
import pandas as pd

# ============================
# BLOQUE: TRANSFORMACIÓN Y LIMPIEZA DE DATOS
# ============================

def limpiar_columnas(df):
    """
    Limpia los nombres de las columnas eliminando espacios iniciales/finales
    y reemplazando espacios por guiones bajos.
    """
    df.columns = df.columns.str.strip().str.replace(' ', '_').str.replace('__', '_')
    return df


def limpiar_datos_ventas(df):
    """
    Aplica limpieza general al DataFrame de ventas.
    - Elimina columnas innecesarias
    - Limpia nombres de columnas
    - Convierte columnas numéricas si es necesario y redondea a 2 decimales
    - Convierte columna Mes a formato año/mes (ej: 2024/01)
    """
    df = limpiar_columnas(df)

    # Eliminar columnas vacías o no informativas
    df = df.drop(columns=[col for col in df.columns if 'unnamed' in col.lower()], errors='ignore')

    # Conversión de tipos y redondeo
    columnas_numericas = ['Venta_Neta_USD', 'Venta_Bruta_USD', 'Descuento_USD', 'Venta_Neta_Unidades', 'Venta_Neta_Kilos']
    for col in columnas_numericas:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').round(2)

    # Transformar columna Mes a formato YYYY/MM
    if 'Mes' in df.columns:
        try:
            df['Mes'] = df['Mes'].apply(lambda x: pd.to_datetime(str(x), format='%Y.%m')).dt.strftime('%Y/%m')
        except Exception as e:
            print(f"[WARNING] No se pudo convertir la columna 'Mes': {e}")

    return df


def limpiar_datos_presupuesto(df):
    """
    Aplica limpieza general al DataFrame de presupuesto.
    - Limpia nombres de columnas
    - Convierte campos numéricos correctamente y redondea a 2 decimales
    - Agrega una columna 'Periodo' en formato año/mes (ej: 2024/01)
    """
    df = limpiar_columnas(df)

    # Conversión de tipos y redondeo
    columnas_numericas = ['Ppto_USD', 'Ppto_ML', 'Ppto_Kg']
    for col in columnas_numericas:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').round(2)

    # Crear nueva columna 'Periodo' en formato YYYY/MM
    if 'Mes' in df.columns:
        try:
            df['Periodo'] = df['Mes'].apply(lambda x: pd.to_datetime("2024-" + str(x), format="%Y-%B").strftime("%Y/%m"))
        except Exception as e:
            print(f"[WARNING] No se pudo generar la columna 'Periodo': {e}")

    return df
