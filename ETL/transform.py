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
    """
    df = limpiar_columnas(df)

    # Eliminar columnas vacías o no informativas
    df = df.drop(columns=[col for col in df.columns if 'unnamed' in col.lower()], errors='ignore')

    # Conversión de tipos y redondeo
    columnas_numericas = ['Venta_Neta_USD', 'Venta_Bruta_USD', 'Descuento_USD', 'Venta_Neta_Unidades', 'Venta_Neta_Kilos']
    for col in columnas_numericas:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').round(2)

    return df


def limpiar_datos_presupuesto(df):
    """
    Aplica limpieza general al DataFrame de presupuesto.
    - Limpia nombres de columnas
    - Convierte campos numéricos correctamente y redondea a 2 decimales
    """
    df = limpiar_columnas(df)

    # Conversión de tipos y redondeo
    columnas_numericas = ['Ppto_USD', 'Ppto_ML', 'Ppto_Kg']
    for col in columnas_numericas:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').round(2)

    return df
